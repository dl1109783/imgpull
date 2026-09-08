package downloader

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// retryableError marks failures worth retrying without touching the .part.
type retryableError struct{ err error }

func (e retryableError) Error() string { return e.err.Error() }
func (e retryableError) Unwrap() error { return e.err }

// partCleanupError means the .part (+ control file) is unusable and must be
// deleted before the next attempt.
type partCleanupError struct{ retryableError }

// authRetryError means the token/URL likely expired: refresh auth, re-resolve
// the URL, and retry while keeping the .part.
type authRetryError struct{ retryableError }

// nonRetryableError aborts the task permanently.
type nonRetryableError struct{ err error }

func (e nonRetryableError) Error() string { return e.err.Error() }
func (e nonRetryableError) Unwrap() error { return e.err }

func retryable(err error) error    { return retryableError{err} }
func nonRetryable(err error) error { return nonRetryableError{err} }
func cleanupAndRetry(err error) error {
	return partCleanupError{retryableError{err}}
}
func authRetry(err error) error { return authRetryError{retryableError{err}} }

func isRetryable(err error) bool {
	var re retryableError
	return errors.As(err, &re)
}

func needsPartCleanup(err error) bool {
	var ce partCleanupError
	return errors.As(err, &ce)
}

func isAuthError(err error) bool {
	var ae authRetryError
	return errors.As(err, &ae)
}

func isNonRetryable(err error) bool {
	var ne nonRetryableError
	return errors.As(err, &ne)
}

// classifyAria2Failure maps aria2 errorCode to imgpull retry semantics.
//
// Notable codes:
//
//	 1  unknown            → retryable
//	 2  timeout            → retryable
//	 3  resource not found → fatal for that URL
//	 4  max tries          → retryable (our own loop decides when to stop)
//	 7  unfinished         → retryable
//	 9  not enough disk    → fatal
//	10 out of disk space   → fatal
//	11 range not supported → cleanup (server won't resume)
//	12 checksum mismatch   → cleanup
//	13 resume unsupported  → cleanup
//	14 auth failed         → auth refresh
//	15/16 bad options      → fatal
//	17 file already exists → cleanup (stale .part)
//	20/21 file I/O error   → fatal
//	22 cannot resume       → cleanup
//	23 out of memory/fd    → fatal
//	24 auth failed (TLS?)  → auth refresh
//	25 crc/checksum bad    → cleanup
//	26/27 misc             → cleanup / retryable
func classifyAria2Failure(st *DownloadStatus) error {
	code := st.ErrorCode
	msg := strings.ToLower(st.ErrorMessage)
	switch code {
	case "3":
		return nonRetryable(fmt.Errorf("aria2: resource not found: %s", st.ErrorMessage))
	case "9", "10", "23":
		return nonRetryable(fmt.Errorf("aria2: disk error (code %s): %s", code, st.ErrorMessage))
	case "15", "16":
		return nonRetryable(fmt.Errorf("aria2: bad download options (code %s): %s", code, st.ErrorMessage))
	case "20", "21":
		return nonRetryable(fmt.Errorf("aria2: file I/O error (code %s): %s", code, st.ErrorMessage))
	case "11", "12", "13", "17", "22", "25", "26":
		return cleanupAndRetry(fmt.Errorf("aria2: code %s: %s", code, st.ErrorMessage))
	case "14", "24":
		return authRetry(fmt.Errorf("aria2: auth (code %s): %s", code, st.ErrorMessage))
	default:
		if strings.Contains(msg, "401") || strings.Contains(msg, "403") ||
			strings.Contains(msg, "unauthorized") || strings.Contains(msg, "forbidden") {
			return authRetry(fmt.Errorf("aria2: %s", st.ErrorMessage))
		}
		return retryable(fmt.Errorf("aria2: code %s: %s", code, st.ErrorMessage))
	}
}

// backoffDelay returns the wait before attempt n (1-based): 1s, 2s, 4s, 8s,
// 16s capped at 30s, with ±20% jitter.
func backoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 4 {
		shift = 4
	}
	d := time.Second << uint(shift)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(d / 4)))
	if rand.Intn(2) == 0 {
		return d - jitter
	}
	return d + jitter
}

// waitBackoff sleeps the backoff delay, honoring ctx cancellation.
func waitBackoff(ctx context.Context, attempt int) error {
	d := backoffDelay(attempt)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

package downloader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/name"

	"imgpull/internal/fsx"
	"imgpull/internal/plan"
	"imgpull/internal/registry"
	"imgpull/internal/verify"
)

// SchedulerOptions tunes scheduler behavior.
type SchedulerOptions struct {
	Concurrency int // layers downloaded in parallel
	MaxRetries  int
	// ProgressInterval controls the aggregate progress log cadence.
	ProgressInterval time.Duration
	// Proxy, when set, is passed to aria2 as the per-download all-proxy
	// option so blob downloads go through it. Must be an http/https URL
	// (aria2 has no SOCKS support); the RPC connection is never proxied.
	Proxy string
}

// Scheduler submits plan tasks to aria2, watches them, verifies digests and
// commits blobs. It owns the per-image download loop including retries and
// resume of orphaned .part files.
type Scheduler struct {
	rpc    *RPC
	client *registry.Client
	auth   *registry.Authenticator
	repo   name.Repository
	plan   *plan.Plan
	opts   SchedulerOptions
	prog   *Progress

	mu      sync.Mutex
	live    map[string]*liveInfo // digest → progress of running tasks
	started time.Time

	rangeWarn sync.Once // "no resumable downloads here" warning, once per run
}

type liveInfo struct {
	kind  plan.Kind
	state string // "", "queue" (resolving/submitting), "down", "verify"
	done  int64
	total int64
	speed int64
}

// NewScheduler wires a scheduler together.
func NewScheduler(rpc *RPC, client *registry.Client, auth *registry.Authenticator,
	repo name.Repository, p *plan.Plan, opts SchedulerOptions) *Scheduler {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 3 * time.Second
	}
	return &Scheduler{
		rpc:    rpc,
		client: client,
		auth:   auth,
		repo:   repo,
		plan:   p,
		opts:   opts,
		prog:   NewProgress(),
		live:   map[string]*liveInfo{},
	}
}

// Run downloads every pending object, returning the joined errors of
// permanently failed tasks. It blocks until done or ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	pending := s.plan.PendingObjects()
	if len(pending) == 0 {
		return nil
	}
	s.started = time.Now()

	// A previous run may already have proven this source un-resumable; say
	// so up front instead of waiting for the first resume failure.
	for _, t := range pending {
		if t.RangeUnsupported {
			s.warnNoResume()
			break
		}
	}

	// Stable order: small layers first for quick wins, keeps progress steady.
	sort.Slice(pending, func(i, j int) bool { return pending[i].Size < pending[j].Size })

	sem := make(chan struct{}, s.opts.Concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(pending))

	// Live progress panel (TTY) or aggregate log lines (pipe).
	done := make(chan struct{})
	defer close(done)
	go s.progressLoop(ctx, done)

	for _, t := range pending {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(t *plan.ObjectTask) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				errCh <- s.downloadObject(ctx, t)
			case <-ctx.Done():
				errCh <- ctx.Err()
			}
		}(t)
	}
	wg.Wait()
	s.renderProgress() // leave a final panel reflecting the end state
	close(errCh)

	var errs []error
	seen := map[error]bool{}
	for err := range errCh {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		if !seen[err] {
			seen[err] = true
			errs = append(errs, err)
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(errs...)
}

// progressLoop drives the progress display: a dynamic panel redrawn ~2.5×/s
// on a terminal, or one aggregate line per ProgressInterval on a pipe.
func (s *Scheduler) progressLoop(ctx context.Context, done chan struct{}) {
	interval := s.opts.ProgressInterval
	if s.prog.Interactive() {
		interval = 400 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			s.renderProgress()
		}
	}
}

func (s *Scheduler) setLive(t *plan.ObjectTask, li *liveInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if li == nil {
		delete(s.live, t.Digest)
	} else {
		s.live[t.Digest] = li
	}
}

// downloadObject runs the full attempt/retry loop for one object.
func (s *Scheduler) downloadObject(ctx context.Context, t *plan.ObjectTask) error {
	// Show the object in the panel from the very start (URL resolution can
	// take a while on slow mirrors); waitTask keeps it updated afterwards.
	s.setLive(t, &liveInfo{kind: t.Kind, state: "queue"})
	defer s.setLive(t, nil)
	// lastErr stays a local: task fields may only be mutated under plan.mu
	// (persistLocked serializes them from other goroutines' commit paths);
	// MarkRetry/MarkFailed store the message under the lock.
	lastErr := ""
	for attempt := 1; attempt <= s.opts.MaxRetries; attempt++ {
		if attempt > 1 {
			if err := ctx.Err(); err != nil {
				return err
			}
			prevErr := errors.New("previous attempt failed")
			if lastErr != "" {
				prevErr = errors.New(lastErr)
			}
			s.plan.MarkRetry(t, prevErr)
			wait := backoffDelay(attempt)
			s.prog.Logf("retry %d/%d for %s in %s (%s)",
				attempt-1, s.opts.MaxRetries-1, t.Digest, wait.Truncate(time.Millisecond), lastErr)
			if err := waitBackoff(ctx, attempt); err != nil {
				return err
			}
			s.setLive(t, &liveInfo{kind: t.Kind, state: "queue"})
		}
		err := s.attempt(ctx, t)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if isNonRetryable(err) {
			s.plan.MarkFailed(t, err)
			s.prog.Logf("FAILED (fatal) %s %s: %v", t.Kind, t.Digest, err)
			return err
		}
		lastErr = err.Error()
		if isRetryable(err) {
			continue
		}
		// Unclassified: treat as retryable.
		continue
	}
	s.plan.MarkFailed(t, errors.New(lastErr))
	s.prog.Logf("FAILED %s %s after %d attempts: %s", t.Kind, t.Digest, s.opts.MaxRetries, lastErr)
	return fmt.Errorf("%s %s: %s", t.Kind, t.Digest, lastErr)
}

// attempt performs one full download try for t: shortcut-complete .part
// handling, URL resolution, aria2 submission, watching, verification and
// commit.
func (s *Scheduler) attempt(ctx context.Context, t *plan.ObjectTask) error {
	part := s.plan.PartPath(t)

	// (a) .part already complete → straight to verify/commit.
	if fi, err := os.Stat(part); err == nil && fi.Size() == t.Size {
		return s.verifyAndCommit(ctx, t)
	}
	// (b) precheck: remove junk control files and oversized parts.
	s.precheck(t)

	// (c) reuse an existing aria2 task for this file (external daemons).
	gid, err := s.findExistingTask(ctx, part)
	if err != nil {
		return err
	}

	// (d) resolve a downloadable URL.
	if t.URL == "" || urlExpired(t) {
		resolveStart := time.Now()
		res, rerr := s.client.ResolveBlobURL(ctx, s.repo, t.Digest, s.auth)
		if d := time.Since(resolveStart); d > 10*time.Second {
			// Pull-through mirrors stall on uncached blobs; tell the user why
			// the object sat in "preparing" for so long.
			s.prog.Logf("resolving %s took %s (slow origin pull on the mirror?)", short(t.Digest), d.Round(time.Second))
		}
		if rerr != nil {
			if errors.Is(rerr, registry.ErrBlobNotFound) {
				// Try manifest-provided alternate URLs before giving up.
				if u, ok := s.plan.PopAltURL(t); ok {
					s.plan.SetTask(t, plan.StatusDownloading, u, nil, "")
					return s.waitTask(ctx, t, "")
				}
				s.plan.MarkFailed(t, rerr)
				return nonRetryable(rerr)
			}
			return retryable(fmt.Errorf("resolve blob URL: %w", rerr))
		}
		if s.client.RangeUnsupported() {
			s.plan.MarkRangeUnsupported(t)
			s.warnNoResume()
		}
		s.plan.SetTask(t, plan.StatusDownloading, res.URL, expPtr(res.ExpiresAt), gid)
		if res.ExpiresAt.Unix() > 0 {
			s.prog.Logf("%s uses signed URL expiring %s", short(t.Digest), res.ExpiresAt.Format("15:04:05"))
		}
		url := res.URL
		headers := res.Headers
		return s.submitAndWatch(ctx, t, url, headers, gid)
	}

	// (e) remembered URL still fresh → resubmit directly.
	headers := s.currentHeaders()
	return s.submitAndWatch(ctx, t, t.URL, headers, gid)
}

func expPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func urlExpired(t *plan.ObjectTask) bool {
	if t.URLExpiresAt == nil {
		return false
	}
	return time.Now().After(t.URLExpiresAt.Add(-30 * time.Second))
}

func (s *Scheduler) currentHeaders() []string {
	if s.auth == nil {
		return nil
	}
	h, err := s.auth.Header(context.Background())
	if err != nil || h == "" {
		return nil
	}
	return []string{"Authorization: " + h}
}

// precheck removes an oversized .part or orphaned .aria2 control file.
func (s *Scheduler) precheck(t *plan.ObjectTask) {
	part := s.plan.PartPath(t)
	if fi, err := os.Stat(part); err == nil && fi.Size() > t.Size {
		os.Remove(part)
		os.Remove(part + ".aria2")
		return
	}
	if _, err := os.Stat(part); os.IsNotExist(err) {
		os.Remove(part + ".aria2")
	}
}

// findExistingTask looks for a still-live aria2 task writing to part and
// reuses its gid (resume with an external daemon across imgpull restarts).
func (s *Scheduler) findExistingTask(ctx context.Context, part string) (string, error) {
	active, err := s.rpc.TellActive(ctx)
	if err != nil {
		return "", retryable(fmt.Errorf("tellActive: %w", err))
	}
	waiting, err := s.rpc.TellWaiting(ctx)
	if err != nil {
		return "", retryable(fmt.Errorf("tellWaiting: %w", err))
	}
	for _, st := range append(active, waiting...) {
		for _, f := range st.Files {
			if f.Path == part {
				s.prog.Logf("reusing live aria2 task %s for %s", st.GID, part)
				return st.GID, nil
			}
		}
	}
	return "", nil
}

// submitAndWatch submits the task to aria2 then polls until completion.
func (s *Scheduler) submitAndWatch(ctx context.Context, t *plan.ObjectTask, url string, headers []string, gid string) error {
	if gid == "" {
		// A fresh submission cannot continue a .part when the source ignores
		// Range requests: aria2 would abort every attempt with code 8.
		if t.RangeUnsupported || s.client.RangeUnsupported() {
			s.discardUnresumablePart(t)
		}
		g, err := s.submitTask(ctx, t, url, headers)
		if err != nil {
			return err
		}
		gid = g
		s.plan.SetTask(t, plan.StatusDownloading, url, nil, gid)
	}
	return s.waitTask(ctx, t, gid)
}

// warnNoResume marks in the log (once per run) that this source cannot host
// resumable downloads, so .part files are discarded on every interruption.
func (s *Scheduler) warnNoResume() {
	s.rangeWarn.Do(func() {
		s.prog.Logf("%s does not support resumable downloads (ignores Range requests); interrupted layers restart from scratch",
			s.plan.Registry)
	})
}

// discardUnresumablePart removes a partial .part (and its aria2 control
// file) that cannot be resumed because the source ignores Range requests;
// the object restarts from scratch.
func (s *Scheduler) discardUnresumablePart(t *plan.ObjectTask) {
	s.warnNoResume()
	part := s.plan.PartPath(t)
	fi, err := os.Stat(part)
	if err != nil || fi.Size() == 0 {
		return
	}
	os.Remove(part)
	os.Remove(part + ".aria2")
	s.prog.Logf("%s: source ignores Range requests; discarding %s partial, restarting this object",
		short(t.Digest), fsx.HumanSize(fi.Size()))
}

// submitTask adds the URI with per-task options; on "unknown option"
// failures it drops options one class at a time (checksum first, then
// headers) for older aria2 builds.
func (s *Scheduler) submitTask(ctx context.Context, t *plan.ObjectTask, url string, headers []string) (string, error) {
	dir := s.plan.TmpDir
	out := t.PartName
	opts := map[string]interface{}{
		"dir":                       dir,
		"out":                       out,
		"continue":                  "true",
		"allow-overwrite":           "false",
		"auto-file-renaming":        "false",
		"max-connection-per-server": "1",
		"split":                     "1",
	}
	if len(headers) > 0 {
		opts["header"] = headers
	}
	if h := t.Hash(); h.Hex != "" && (h.Algorithm == "sha256" || h.Algorithm == "sha512") {
		// aria2 spells the algorithm with a hyphen (sha-256), v1.Hash without it.
		opts["checksum"] = fmt.Sprintf("sha-%s=%s", strings.TrimPrefix(h.Algorithm, "sha"), h.Hex)
	}
	if s.opts.Proxy != "" {
		opts["all-proxy"] = s.opts.Proxy
	}
	for drop := 0; ; drop++ {
		gid, err := s.rpc.AddURI(ctx, []string{url}, opts)
		if err == nil {
			return gid, nil
		}
		if errors.Is(err, context.Canceled) {
			return "", err
		}
		// aria2 rejects bad options with code 15/16 (or code 1 over HTTP
		// 400 with the option named in the message) → drop and retry.
		if re, ok := err.(*rpcError); ok && (re.Code == 15 || re.Code == 16 ||
			strings.Contains(re.Message, "option")) && drop < 3 {
			s.prog.Logf("aria2 rejected options (code %d), simplifying: %s", re.Code, err)
			delete(opts, "checksum")
			if _, has := opts["header"]; has && drop >= 1 {
				delete(opts, "header")
			}
			if _, has := opts["all-proxy"]; has && drop >= 2 {
				delete(opts, "all-proxy")
			}
			continue
		}
		return "", retryable(fmt.Errorf("addUri: %w", err))
	}
}

// waitTask polls aria2 until the task completes, errors, or ctx cancels.
func (s *Scheduler) waitTask(ctx context.Context, t *plan.ObjectTask, gid string) error {
	part := s.plan.PartPath(t)
	if gid != "" {
		s.setLive(t, &liveInfo{kind: t.Kind, state: "down"})
		defer s.setLive(t, nil)
	}
	pollErrs := 0
	for {
		if ctx.Err() != nil {
			if gid != "" {
				s.stopTask(gid) // keep .part
			}
			return ctx.Err()
		}
		if gid == "" {
			// Alt-URL fallback path without gid: poll the file instead.
			if fi, err := os.Stat(part); err == nil {
				s.setLive(t, &liveInfo{kind: t.Kind, state: "down", done: fi.Size(), total: t.Size})
				if fi.Size() == t.Size {
					return s.verifyAndCommit(ctx, t)
				}
			}
			time.Sleep(time.Second)
			continue
		}
		st, err := s.rpc.TellStatus(ctx, gid)
		if err != nil {
			pollErrs++
			if pollErrs > 10 {
				return retryable(fmt.Errorf("tellStatus %s: %w", gid, err))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		pollErrs = 0
		switch st.Status {
		case "complete":
			s.rpc.RemoveDownloadResult(context.Background(), gid)
			if fi, err := os.Stat(part); err == nil && fi.Size() == t.Size {
				return s.verifyAndCommit(ctx, t)
			}
			// File landed somewhere unexpected or vanished: resubmit.
			return retryable(fmt.Errorf("aria2 finished but %s is missing/wrong size", part))
		case "error":
			s.rpc.RemoveDownloadResult(context.Background(), gid)
			if st.ErrorCode == "8" {
				// "No URI available": aria2 aborted without transferring. On
				// a Range-blind source this is the resume being rejected; a
				// stale/expired URL can also trigger it. Probe the source
				// once to tell these apart, then force a re-resolve either
				// way so the next attempt cannot resubmit the same URL.
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				blind := t.RangeUnsupported || s.client.RangeUnsupported() ||
					s.client.ProbeRangeBlind(pctx, t.URL, s.currentHeaders())
				cancel()
				if blind {
					if !t.RangeUnsupported {
						s.prog.Logf("%s: source ignores Range requests; the partial cannot be resumed",
							short(t.Digest))
					}
					s.warnNoResume()
					s.plan.MarkRangeUnsupported(t)
				} else {
					s.plan.ForgetURL(t)
				}
			}
			ferr := classifyAria2Failure(st)
			s.clearLive()
			return ferr
		case "removed":
			return retryable(fmt.Errorf("aria2 task %s removed", gid))
		case "active", "waiting", "paused":
			if li := toLive(t, st); li != nil {
				s.setLive(t, li)
			}
		default:
			// unknown status: keep polling
		}
		select {
		case <-ctx.Done():
			if gid != "" {
				s.stopTask(gid)
			}
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// stopTask cancels a running aria2 task while preserving its .part file.
func (s *Scheduler) stopTask(gid string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.rpc.ForceRemove(ctx, gid); err != nil {
		s.prog.Logf("warning: forceRemove %s: %v", gid, err)
	}
}

func toLive(t *plan.ObjectTask, st *DownloadStatus) *liveInfo {
	done, total, speed := st.Progress()
	if total == 0 {
		total = t.Size
	}
	return &liveInfo{kind: t.Kind, state: "down", done: done, total: total, speed: speed}
}

func (s *Scheduler) clearLive() {
	// Called on error paths to avoid showing stale speed after failure.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Zero the speeds but keep sizes so totals remain meaningful.
	for _, li := range s.live {
		li.speed = 0
	}
}

// verifyAndCommit checks the .part digest/size then moves it into blobs/.
// On mismatch the part is deleted (per spec) and a cleanup-retry error
// returned so the scheduler redownloads from scratch.
func (s *Scheduler) verifyAndCommit(ctx context.Context, t *plan.ObjectTask) error {
	// Hashing large blobs takes a while — reflect it in the panel.
	s.setLive(t, &liveInfo{kind: t.Kind, state: "verify", done: t.Size, total: t.Size})
	part := s.plan.PartPath(t)
	if err := verify.Blob(part, t.Hash(), t.Size); err != nil {
		os.Remove(part)
		os.Remove(part + ".aria2")
		s.prog.Logf("verification failed for %s: %v — redownloading", t.Digest, err)
		return cleanupAndRetry(fmt.Errorf("verify %s: %w", t.Digest, err))
	}
	if err := s.plan.CommitVerified(t); err != nil {
		return retryable(fmt.Errorf("commit %s: %w", t.Digest, err))
	}
	committed := len(s.plan.Objects) - len(s.plan.PendingObjects())
	s.prog.Logf("✓ %s %s (%s) [%d/%d]",
		t.Kind, short(t.Digest), fsx.HumanSize(t.Size), committed, len(s.plan.Objects))
	return nil
}

func short(digest string) string {
	if i := strings.Index(digest, ":"); i >= 0 {
		digest = digest[i+1:]
	}
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

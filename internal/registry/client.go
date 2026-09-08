package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
)

const userAgent = "imgpull/0.1"

// ErrBlobNotFound is returned when the registry replies 404 for a blob.
var ErrBlobNotFound = errors.New("blob not found")

// errUnauthorized is the internal sentinel for 401/403 responses.
var errUnauthorized = errors.New("unauthorized")

// ResolvedURL is a ready-to-use blob download URL with any extra headers
// aria2 must send. For signed CDN URLs no Authorization header is included;
// for direct registry URLs the Bearer header is.
type ResolvedURL struct {
	URL       string
	Headers   []string
	ExpiresAt time.Time // zero when unknown (direct registry URL)
}

// Client resolves blob URLs against a registry. Manifests themselves are
// fetched through go-containerregistry; this client only handles the raw
// HTTP probing needed to hand aria2 a URL that works without re-auth for as
// long as possible.
type Client struct {
	insecure bool
	probe    *http.Client
	// headTimeout bounds each HEAD probe. Some pull-through mirrors never
	// answer HEAD for blobs they must fetch from upstream first — without a
	// bound the resolve would stall for minutes.
	headTimeout time.Duration
	// skipHead latches once HEAD proves useless for this registry (hang or
	// 405), so later blobs probe with a ranged GET immediately.
	skipHead atomic.Bool
	// rangeBlind latches once the registry answers a ranged GET with a full
	// 200 body: it ignores Range, so aria2 can never resume a .part here.
	rangeBlind atomic.Bool
}

// NewClient creates a Client.
func NewClient(insecure bool) *Client {
	return &Client{
		insecure: insecure,
		probe: &http.Client{
			Timeout: 60 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// We follow redirects manually hop by hop.
				return http.ErrUseLastResponse
			},
		},
		headTimeout: 15 * time.Second,
	}
}

// NewClientProxy creates a Client whose HTTP probes (blob URL resolution,
// range checks) go through the given proxy. A nil proxy means direct.
func NewClientProxy(insecure bool, proxy *url.URL) *Client {
	c := NewClient(insecure)
	if proxy != nil {
		c.probe.Transport = ProxyTransport(proxy)
	}
	return c
}

// ProxyTransport clones the default transport and routes every request
// through proxy. The result is safe for concurrent use.
func ProxyTransport(proxy *url.URL) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = http.ProxyURL(proxy)
	return t
}

// BlobURL is the canonical registry blob endpoint.
func BlobURL(repo name.Repository, digest string) string {
	scheme := registryScheme(repo.RegistryStr(), false)
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", scheme, repo.RegistryStr(), repo.RepositoryStr(), digest)
}

// blobURLWithScheme builds the blob endpoint using the client's scheme.
func (c *Client) blobURL(repo name.Repository, digest string) string {
	scheme := registryScheme(repo.RegistryStr(), c.insecure)
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", scheme, repo.RegistryStr(), repo.RepositoryStr(), digest)
}

// ResolveBlobURL turns the canonical blob URL into something aria2 can
// download: either the registry URL plus Authorization header (registries
// that serve blobs themselves) or a single signed CDN/redirect URL (registries
// like docker.io that 307 to storage). Retries transient auth failures.
func (c *Client) ResolveBlobURL(ctx context.Context, repo name.Repository, digest string, auth *Authenticator) (*ResolvedURL, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		res, err := c.resolveBlobURLOnce(ctx, repo, digest, auth)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if errors.Is(err, errUnauthorized) && auth != nil {
			auth.Invalidate()
			continue
		}
		if errors.Is(err, ErrBlobNotFound) {
			return nil, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// Other errors: brief pause then retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func (c *Client) resolveBlobURLOnce(ctx context.Context, repo name.Repository, digest string, auth *Authenticator) (*ResolvedURL, error) {
	var header string
	if auth != nil {
		h, err := auth.Header(ctx)
		if err != nil {
			return nil, err
		}
		header = h
	}
	// Fast path: this registry already proved HEAD useless (hang or 405) —
	// probe with a ranged GET right away.
	if c.skipHead.Load() {
		return c.resolveViaRangeGet(ctx, repo, digest, header)
	}
	url := c.blobURL(repo, digest)
	for hop := 0; hop < 6; hop++ {
		headCtx, cancel := context.WithTimeout(ctx, c.headTimeout)
		req, err := http.NewRequestWithContext(headCtx, http.MethodHead, url, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		if header != "" {
			// Inside this loop the URL host is always the registry itself
			// (cross-host redirects return immediately below).
			req.Header.Set("Authorization", header)
		}
		resp, err := c.probe.Do(req)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// HEAD can hang indefinitely on mirrors that must origin-pull an
			// uncached blob before answering. A ranged GET starts streaming
			// instead — probe that way and remember it for later blobs.
			c.skipHead.Store(true)
			return c.resolveViaRangeGet(ctx, repo, digest, header)
		}
		resp.Body.Close()

		switch {
		case resp.StatusCode >= 300 && resp.StatusCode < 400:
			loc := resp.Header.Get("Location")
			if loc == "" {
				return nil, fmt.Errorf("redirect %d from %s without Location", resp.StatusCode, url)
			}
			next, err := req.URL.Parse(loc)
			if err != nil {
				return nil, fmt.Errorf("redirect location %q: %w", loc, err)
			}
			if sameHost(next.Host, req.URL.Host) {
				// Same-host redirect (e.g. http→https or path change):
				// keep following, keep auth header.
				url = next.String()
				continue
			}
			// Cross-host redirect = signed CDN URL. Strip auth; the URL
			// itself carries the credentials.
			return &ResolvedURL{
				URL:       next.String(),
				Headers:   nil,
				ExpiresAt: parseURLExpiry(next),
			}, nil

		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
			// Registry serves blobs directly (quay.io etc.).
			var headers []string
			if header != "" {
				headers = []string{"Authorization: " + header}
			}
			return &ResolvedURL{URL: url, Headers: headers}, nil

		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return nil, errUnauthorized

		case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented:
			// HEAD not supported; confirm with a 1-byte GET.
			c.skipHead.Store(true)
			return c.resolveViaRangeGet(ctx, repo, digest, header)

		case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest:
			return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, digest)

		default:
			// 5xx and friends: the ranged GET may still be served.
			return c.resolveViaRangeGet(ctx, repo, digest, header)
		}
	}
	return nil, fmt.Errorf("too many redirects resolving %s", digest)
}

// resolveViaRangeGet falls back to GET with Range: bytes=0-0 for registries
// that reject HEAD requests.
func (c *Client) resolveViaRangeGet(ctx context.Context, repo name.Repository, digest, header string) (*ResolvedURL, error) {
	url := c.blobURL(repo, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Range", "bytes=0-0")
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	resp, err := c.probe.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		loc := resp.Header.Get("Location")
		if loc == "" {
			return nil, fmt.Errorf("redirect from %s without Location", url)
		}
		next, err := req.URL.Parse(loc)
		if err != nil {
			return nil, err
		}
		if sameHost(next.Host, req.URL.Host) {
			return &ResolvedURL{
				URL:     next.String(),
				Headers: []string{"Authorization: " + header},
			}, nil
		}
		return &ResolvedURL{URL: next.String(), ExpiresAt: parseURLExpiry(next)}, nil
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
		if resp.StatusCode == http.StatusOK {
			// The ranged GET got the full body: this server ignores Range,
			// so resuming a partial download against it is impossible.
			c.rangeBlind.Store(true)
		}
		var headers []string
		if header != "" {
			headers = []string{"Authorization: " + header}
		}
		return &ResolvedURL{URL: url, Headers: headers}, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, errUnauthorized
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest:
		return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, digest)
	default:
		return nil, fmt.Errorf("range-probe %s: status %d", url, resp.StatusCode)
	}
}

// RangeUnsupported reports whether this registry was caught serving full 200
// bodies to ranged GETs. Against such a server aria2 cannot continue a .part
// file: every resume attempt aborts with errorCode 8 (No URI available).
func (c *Client) RangeUnsupported() bool { return c.rangeBlind.Load() }

// ProbeRangeBlind checks url with a 1-byte ranged GET and reports whether it
// definitively ignores Range (200 full body instead of 206). Network errors,
// auth challenges and redirects are inconclusive and report false, so callers
// only act on a positive verdict when it is safe to discard the .part.
func (c *Client) ProbeRangeBlind(ctx context.Context, url string, headers []string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Range", "bytes=0-0")
	for _, h := range headers {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	resp, err := c.probe.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode == http.StatusOK
}

// parseURLExpiry extracts a signed-URL expiry hint from common query
// parameters (S3/Azure/Aliyun variants). Zero time if unknown.
func parseURLExpiry(u *url.URL) time.Time {
	q := u.Query()
	// S3 presigned: X-Amz-Date + X-Amz-Expires.
	if d := q.Get("X-Amz-Date"); d != "" {
		if exp, err := strconv.Atoi(q.Get("X-Amz-Expires")); err == nil && exp > 0 {
			if t, err := time.Parse("20060102T150405Z", d); err == nil {
				return t.Add(time.Duration(exp) * time.Second)
			}
		}
	}
	// Azure SAS: se=2026-01-02T03:04:05Z.
	if se := q.Get("se"); se != "" {
		for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z"} {
			if t, err := time.Parse(layout, se); err == nil {
				return t
			}
		}
	}
	// Aliyun OSS / generic unix-timestamp: Expires / x-oss-expires.
	for _, k := range []string{"x-oss-expires", "Expires", "expires"} {
		if v := q.Get(k); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 1<<30 {
				return time.Unix(n, 0)
			}
		}
	}
	return time.Time{}
}

// sameHost reports whether the two host[:port] values are equal.
func sameHost(a, b string) bool {
	return strings.EqualFold(a, b)
}

var localhostRe = regexp.MustCompile(`^localhost(:\d+)?$`)

// registryScheme picks http/https the same way go-containerregistry does.
func registryScheme(reg string, insecure bool) string {
	if insecure {
		return "http"
	}
	host := reg
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.Contains(host, "]") {
		host = host[:i]
	}
	if localhostRe.MatchString(reg) || reg == "::1" || host == "127.0.0.1" || strings.HasSuffix(reg, ".localhost") {
		return "http"
	}
	return "https"
}

// parseInt64 parses s as int64, 0 on failure.
func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

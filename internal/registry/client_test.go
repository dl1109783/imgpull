package registry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
)

// probeHooks route blob-endpoint requests to test behavior.
type probeHooks struct {
	head func(http.ResponseWriter, *http.Request) // nil → 404
	get  func(http.ResponseWriter, *http.Request) // nil → 404
}

type probeServer struct {
	s    *httptest.Server
	repo name.Repository

	mu        sync.Mutex
	headCalls int
	getCalls  int
}

func newProbeServer(t *testing.T, hooks probeHooks) *probeServer {
	t.Helper()
	ps := &probeServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/some/app/blobs/", func(w http.ResponseWriter, r *http.Request) {
		ps.mu.Lock()
		switch r.Method {
		case http.MethodHead:
			ps.headCalls++
		case http.MethodGet:
			ps.getCalls++
		}
		ps.mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if hooks.head != nil {
				hooks.head(w, r)
				return
			}
		case http.MethodGet:
			if hooks.get != nil {
				hooks.get(w, r)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	ps.s = httptest.NewServer(mux)
	t.Cleanup(ps.s.Close)
	repo, err := name.NewRepository("some/app", name.WithDefaultRegistry(ps.s.Listener.Addr().String()))
	if err != nil {
		t.Fatalf("new repository: %v", err)
	}
	ps.repo = repo
	return ps
}

func (ps *probeServer) calls() (head, get int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.headCalls, ps.getCalls
}

// streamFull emulates a mirror that ignores Range and starts streaming the
// whole blob (nju-style pull-through cache on a cold blob).
func streamFull(w http.ResponseWriter) {
	w.Header().Set("Content-Length", "100000")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "0123456789")
}

func TestResolveHeadHangFallsBackToRangeGet(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done() // hang until the client times out
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("Range"); got != "bytes=0-0" {
				t.Errorf("Range header = %q, want bytes=0-0", got)
			}
			streamFull(w)
		},
	})
	c := NewClient(false)
	c.headTimeout = 300 * time.Millisecond

	start := time.Now()
	res, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("resolve took %v, fallback did not engage", d)
	}
	if want := BlobURL(ps.repo, "sha256:abc"); res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
	if len(res.Headers) != 0 {
		t.Errorf("headers = %v, want none", res.Headers)
	}
	if !c.skipHead.Load() {
		t.Errorf("skipHead not latched after HEAD hang")
	}

	// The next resolve must skip HEAD entirely.
	if _, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:def", nil); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	head, get := ps.calls()
	if head != 1 {
		t.Errorf("HEAD calls = %d, want 1 (latched)", head)
	}
	if get < 2 {
		t.Errorf("GET calls = %d, want >= 2", get)
	}
}

func TestResolveHead405FallsBackToRangeGet(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, "0")
		},
	})
	c := NewClient(false)
	res, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if want := BlobURL(ps.repo, "sha256:abc"); res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
	if !c.skipHead.Load() {
		t.Errorf("skipHead not latched after 405")
	}
}

func TestRangeBlindLatchAndProbe(t *testing.T) {
	blind := false
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			if !blind {
				// Range-capable: answer the probe with 206.
				w.Header().Set("Content-Range", "bytes 0-0/1000")
				w.Header().Set("Content-Length", "1")
				w.WriteHeader(http.StatusPartialContent)
				fmt.Fprint(w, "0")
				return
			}
			// Range-blind mirror: full 200 body regardless of Range.
			streamFull(w)
		},
	})
	c := NewClient(false)

	if _, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if c.RangeUnsupported() {
		t.Errorf("RangeUnsupported = true, want false while the server honors Range")
	}

	// Flip the server to Range-blind; the next resolve must latch it.
	blind = true
	if _, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:def", nil); err != nil {
		t.Fatalf("resolve after flip: %v", err)
	}
	if !c.RangeUnsupported() {
		t.Errorf("RangeUnsupported = false, want true after a 200 to the ranged probe")
	}

	// ProbeRangeBlind: definitive 200 → true; 206 and error → false.
	if !c.ProbeRangeBlind(context.Background(), ps.s.URL+"/v2/some/app/blobs/sha256:x", nil) {
		t.Errorf("ProbeRangeBlind = false, want true against a 200-to-Range server")
	}
	blind = false
	if c.ProbeRangeBlind(context.Background(), ps.s.URL+"/v2/some/app/blobs/sha256:x", nil) {
		t.Errorf("ProbeRangeBlind = true, want false against a 206 server")
	}
	if c.ProbeRangeBlind(context.Background(), "http://127.0.0.1:1/nope", nil) {
		t.Errorf("ProbeRangeBlind = true, want false on a connection error")
	}
}

func TestResolveSignedRedirectCrossHost(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "https://cdn.example.com/v2/blob?se=2026-01-02T03:04:05Z")
			w.WriteHeader(http.StatusTemporaryRedirect)
		},
	})
	c := NewClient(false)
	res, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if want := "https://cdn.example.com/v2/blob?se=2026-01-02T03:04:05Z"; res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
	if len(res.Headers) != 0 {
		t.Errorf("auth must be stripped for signed URLs, got %v", res.Headers)
	}
	wantExp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if !res.ExpiresAt.Equal(wantExp) {
		t.Errorf("ExpiresAt = %v, want %v", res.ExpiresAt, wantExp)
	}
}

func TestResolveBlobNotFound(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	})
	c := NewClient(false)
	_, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("err = %v, want ErrBlobNotFound", err)
	}
	// A HEAD 404 is confirmed by one ranged GET before it counts, and the
	// verdict needs three consecutive strikes (flaky cache-front mirrors).
	head, get := ps.calls()
	if head != 3 || get != 3 {
		t.Errorf("HEAD/GET calls = %d/%d, want 3/3 (strike budget)", head, get)
	}
}

// TestResolveHead404FallsBackToSignedGET emulates ghcr.1ms.run: blob HEADs
// always 404 while a ranged GET answers with a cross-host signed redirect.
func TestResolveHead404FallsBackToSignedGET(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("Range"); got != "bytes=0-0" {
				t.Errorf("Range header = %q, want bytes=0-0", got)
			}
			w.Header().Set("Location", "https://cdn.example.com/blob?se=2026-01-02T03:04:05Z")
			w.WriteHeader(http.StatusTemporaryRedirect)
		},
	})
	c := NewClient(false)
	res, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if want := "https://cdn.example.com/blob?se=2026-01-02T03:04:05Z"; res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
	if len(res.Headers) != 0 {
		t.Errorf("auth must be stripped for signed URLs, got %v", res.Headers)
	}
	if !c.skipHead.Load() {
		t.Errorf("skipHead not latched after a HEAD 404 disproved by GET")
	}

	// The next resolve must skip HEAD entirely.
	if _, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:def", nil); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if head, _ := ps.calls(); head != 1 {
		t.Errorf("HEAD calls = %d, want 1 (latched)", head)
	}
}

// TestResolveFlaky404Retries emulates an edge pool where some nodes 404
// uncached blobs: the first ranged GET misses, the retry succeeds.
func TestResolveFlaky404Retries(t *testing.T) {
	var gets int32
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
		get: func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt32(&gets, 1) == 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Location", "https://cdn.example.com/blob?sig=x")
			w.WriteHeader(http.StatusTemporaryRedirect)
		},
	})
	c := NewClient(false)
	res, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if want := "https://cdn.example.com/blob?sig=x"; res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
}

func TestResolveUnauthorizedRetriesThenFails(t *testing.T) {
	ps := newProbeServer(t, probeHooks{
		head: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		},
	})
	c := NewClient(false)
	_, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if err == nil || !errors.Is(err, errUnauthorized) {
		t.Fatalf("err = %v, want unauthorized", err)
	}
	if head, _ := ps.calls(); head != 3 {
		t.Errorf("HEAD calls = %d, want 3 (retry budget)", head)
	}
}

// TestNewClientProxyRoutesProbes resolves against a registry host that is
// unreachable directly (RFC 2606 .test domain); the only way the probe can
// succeed is through the configured forward proxy.
func TestNewClientProxyRoutesProbes(t *testing.T) {
	var seenHost, seenScheme string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHost = r.Host
		seenScheme = r.URL.Scheme
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	pu, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}

	repo := name.MustParseReference("registry.example.test/org/app", name.Insecure).Context()
	c := NewClientProxy(true, pu)
	res, err := c.ResolveBlobURL(context.Background(), repo, "sha256:abc", nil)
	if err != nil {
		t.Fatalf("resolve via proxy: %v", err)
	}
	if want := "http://registry.example.test/v2/org/app/blobs/sha256:abc"; res.URL != want {
		t.Errorf("URL = %q, want %q", res.URL, want)
	}
	if seenHost != "registry.example.test" || seenScheme != "http" {
		t.Errorf("proxy saw host=%q scheme=%q, want registry.example.test/http", seenHost, seenScheme)
	}
}

func TestProxyTransportChoosesProxy(t *testing.T) {
	pu, err := url.Parse("http://192.0.2.7:1080")
	if err != nil {
		t.Fatal(err)
	}
	tr := ProxyTransport(pu)
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: "registry-1.docker.io"}}
	got, err := tr.Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if got != pu {
		t.Errorf("Proxy(req) = %v, want %v", got, pu)
	}
	if tr == http.DefaultTransport {
		t.Error("ProxyTransport must not mutate the default transport")
	}
}

package registry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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
	})
	c := NewClient(false)
	_, err := c.ResolveBlobURL(context.Background(), ps.repo, "sha256:abc", nil)
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("err = %v, want ErrBlobNotFound", err)
	}
	if head, _ := ps.calls(); head != 1 {
		t.Errorf("HEAD calls = %d, want 1 (no retries on 404)", head)
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

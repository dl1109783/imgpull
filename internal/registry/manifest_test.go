package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
)

// imageManifest returns a minimal valid docker manifest and its digest.
func imageManifest(t *testing.T) (raw []byte, digest string) {
	t.Helper()
	cfg := sha256.Sum256([]byte("config-blob"))
	l := sha256.Sum256([]byte("layer-blob"))
	raw = []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":%q,"config":{"mediaType":%q,"digest":"sha256:%s","size":10},`+
			`"layers":[{"mediaType":%q,"digest":"sha256:%s","size":20}]}`,
		MTDockerManifest, MTDockerConfig, hex.EncodeToString(cfg[:]),
		MTDockerLayer, hex.EncodeToString(l[:])))
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:])
}

// manifestServer serves /v2/some/app/manifests/latest via the given handler
// (or 404 when nil) and counts manifest GETs.
type manifestServer struct {
	s    *httptest.Server
	ref  name.Reference
	mu   sync.Mutex
	gets int
}

func newManifestServer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request)) *manifestServer {
	t.Helper()
	ms := &manifestServer{}
	mux := http.NewServeMux()
	// ggcr pings /v2/ first to discover the auth scheme.
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	})
	mux.HandleFunc("/v2/some/app/manifests/", func(w http.ResponseWriter, r *http.Request) {
		ms.mu.Lock()
		ms.gets++
		ms.mu.Unlock()
		if handle != nil {
			handle(w, r)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	ms.s = httptest.NewServer(mux)
	t.Cleanup(ms.s.Close)
	ref, err := name.ParseReference("some/app:latest",
		name.WithDefaultRegistry(ms.s.Listener.Addr().String()), name.Insecure)
	if err != nil {
		t.Fatalf("parse reference: %v", err)
	}
	ms.ref = ref
	return ms
}

func (ms *manifestServer) get() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.gets
}

func TestResolveManifestRetriesTransient(t *testing.T) {
	raw, _ := imageManifest(t)
	var ms *manifestServer
	ms = newManifestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if ms.get() <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", MTDockerManifest)
		w.Write(raw)
	})
	var logs []string
	m, err := ResolveManifestOpts(context.Background(), ms.ref,
		&Platform{OS: "linux", Architecture: "amd64"}, nil, ManifestFetchOptions{
			Attempts: 3, PerTryTimeout: 2 * time.Second,
			Log: func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) },
		})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if m.MediaType != MTDockerManifest {
		t.Errorf("media type = %q", m.MediaType)
	}
	if len(logs) != 2 {
		t.Fatalf("logs = %v, want 2 retry lines", logs)
	}
	if !strings.Contains(logs[0], "retrying in 1s (attempt 2/3)") {
		t.Errorf("first retry line = %q", logs[0])
	}
}

func TestResolveManifestTimeoutFailsClearly(t *testing.T) {
	ms := newManifestServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // hang like a stalled mirror
	})
	start := time.Now()
	var logs []string
	_, err := ResolveManifestOpts(context.Background(), ms.ref,
		&Platform{OS: "linux", Architecture: "amd64"}, nil, ManifestFetchOptions{
			Attempts: 2, PerTryTimeout: 300 * time.Millisecond,
			Log: func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) },
		})
	if err == nil {
		t.Fatal("want error")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v, retries did not bound the wait", d)
	}
	if !strings.Contains(err.Error(), "after 2 attempt(s)") {
		t.Errorf("error = %v, want attempt count", err)
	}
	if !strings.Contains(err.Error(), "not responding") {
		t.Errorf("error = %v, want mirror hint on timeout", err)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "attempt 2/2") {
		t.Errorf("logs = %v, want one retry line mentioning attempt 2/2", logs)
	}
}

func TestResolveManifest404NoRetry(t *testing.T) {
	ms := newManifestServer(t, nil) // 404
	_, err := ResolveManifestOpts(context.Background(), ms.ref,
		&Platform{OS: "linux", Architecture: "amd64"}, nil, ManifestFetchOptions{
			Attempts: 3, PerTryTimeout: 2 * time.Second,
		})
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %v, want status 404", err)
	}
	if n := ms.get(); n != 1 {
		t.Errorf("manifest GETs = %d, want 1 (no retry on definitive 404)", n)
	}
}

func TestResolveManifestIndexWalk(t *testing.T) {
	raw, _ := imageManifest(t)
	child := fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":%q,"manifests":[{"mediaType":%q,"digest":%q,"size":%d,`+
			`"platform":{"architecture":"amd64","os":"linux"}}]}`,
		MTDockerList, MTDockerManifest, func() string {
			sum := sha256.Sum256(raw)
			return "sha256:" + hex.EncodeToString(sum[:])
		}(), len(raw))
	ms := newManifestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/manifests/latest"):
			w.Header().Set("Content-Type", MTDockerList)
			w.Write([]byte(child))
		default: // digest-addressed child manifest
			w.Header().Set("Content-Type", MTDockerManifest)
			w.Write(raw)
		}
	})
	m, err := ResolveManifestOpts(context.Background(), ms.ref,
		&Platform{OS: "linux", Architecture: "amd64"}, nil, ManifestFetchOptions{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if m.MediaType != MTDockerManifest {
		t.Errorf("media type = %q, want platform manifest", m.MediaType)
	}
	if m.Platform.OS != "linux" || m.Platform.Architecture != "amd64" {
		t.Errorf("platform = %v", m.Platform)
	}
}

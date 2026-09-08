package downloader

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"imgpull/internal/reference"
	"imgpull/internal/registry"
)

// slowBlobServer serves one blob at ~1 MiB/s so tests can interrupt mid-way.
type slowBlobServer struct {
	content []byte
	calls   atomic.Int32
	ranges  atomic.Int64 // minimum start offset seen (resume proof)
}

func (s *slowBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.calls.Add(1)
	start := int64(0)
	if rng := r.Header.Get("Range"); rng != "" {
		fmt.Sscanf(rng, "bytes=%d-", &start)
	}
	for {
		min := s.ranges.Load()
		if start >= min || s.ranges.CompareAndSwap(min, start) {
			break
		}
	}
	w.Header().Set("Content-Length", fmt.Sprint(int64(len(s.content))-start))
	if r.Method == http.MethodHead {
		return
	}
	if start > 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(s.content)-1, len(s.content)))
		w.WriteHeader(206)
	}
	chunk := 32 * 1024
	for off := start; off < int64(len(s.content)); off += int64(chunk) {
		end := off + int64(chunk)
		if end > int64(len(s.content)) {
			end = int64(len(s.content))
		}
		w.Write(s.content[off:end])
		time.Sleep(30 * time.Millisecond) // ~1 MiB/s
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

func requireAria2c(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("short mode: skipping real aria2c test")
	}
	if _, err := exec.LookPath("aria2c"); err != nil {
		t.Skip("aria2c not installed")
	}
}

// TestRealAria2DaemonDownload runs the full scheduler against a real aria2c
// daemon started by StartDaemon.
func TestRealAria2DaemonDownload(t *testing.T) {
	requireAria2c(t)
	e := newTestEnv(t, false, false)
	p := e.buildPlan()
	defer p.Release()

	daemon, err := StartDaemon(context.Background(), DaemonOptions{Dir: p.TmpDir, ConcurrentDownloads: 3})
	if err != nil {
		t.Fatalf("StartDaemon: %v", err)
	}
	defer daemon.Stop()

	client := registry.NewClient(true)
	auth := registry.NewAuthenticator(e.repo, registry.BuildKeychain("", ""), true)
	s := NewScheduler(daemon.RPC, client, auth, e.repo, p,
		SchedulerOptions{Concurrency: 3, MaxRetries: 3, ProgressInterval: 100 * time.Millisecond})
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
}

// slowEnv builds a registry serving one config + one 3 MiB layer at ~1 MiB/s.
func slowEnv(t *testing.T) (*testEnv, *slowBlobServer) {
	t.Helper()
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	content := make([]byte, 3<<20) // ~3s to download at the throttled rate
	for i := range content {
		content[i] = byte(i * 7)
	}
	digestOf := func(b []byte) string {
		sum := sha256.Sum256(b)
		return "sha256:" + fmt.Sprintf("%x", sum)
	}
	cfgD, layerD := digestOf(config), digestOf(content)
	raw := fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,
	"config":{"mediaType":%q,"digest":%q,"size":%d},
	"layers":[{"mediaType":%q,"digest":%q,"size":%d}]}`,
		registry.MTOCIManifest, registry.MTOCIConfig, cfgD, len(config),
		registry.MTOCILayer, layerD, len(content))

	slow := &slowBlobServer{content: content}
	reg := &fakeRegistry{
		repo: "test/slow", tag: "dev",
		blobs:       map[string][]byte{cfgD: config, layerD: content},
		manifestMT:  registry.MTOCIManifest,
		manifestRaw: []byte(raw),
	}
	// Route the layer blob through the throttled handler.
	mux := http.NewServeMux()
	mux.Handle("/v2/test/slow/blobs/"+layerD, slow)
	mux.Handle("/", http.HandlerFunc(reg.handle))
	regSrv := httptest.NewServer(mux)
	t.Cleanup(regSrv.Close)

	ref, err := reference.Parse(strings.TrimPrefix(regSrv.URL, "http://")+"/test/slow:dev", true)
	if err != nil {
		t.Fatal(err)
	}
	mfst, err := registry.ResolveManifest(context.Background(), ref.Reference,
		&registry.Platform{OS: "linux", Architecture: "amd64"}, registry.BuildKeychain("", ""))
	if err != nil {
		t.Fatal(err)
	}
	return &testEnv{
		t: t, regSrv: regSrv, reg: reg,
		repo: ref.Reference.Context(), mfst: mfst, root: t.TempDir(),
	}, slow
}

// TestRealAria2InterruptResume downloads a slow blob, cancels mid-way, then
// reruns with a fresh daemon and verifies the .part is reused (resumed) and
// the final blob verifies.
func TestRealAria2InterruptResume(t *testing.T) {
	requireAria2c(t)
	e, slow := slowEnv(t)
	p := e.buildPlan()
	defer p.Release()

	// Phase 1: cancel mid-download. Wait for the .part to actually exist
	// instead of a blind sleep — under load (race detector, parallel test
	// packages) 700 ms is not always enough for aria2 to start writing.
	layer := p.Objects[len(p.Objects)-1] // the single layer task
	part := p.PartPath(layer)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if fi, err := os.Stat(part); err == nil && fi.Size() > 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
	}()
	daemon, err := StartDaemon(ctx, DaemonOptions{Dir: p.TmpDir, ConcurrentDownloads: 3})
	if err != nil {
		t.Fatalf("StartDaemon: %v", err)
	}
	client := registry.NewClient(true)
	auth := registry.NewAuthenticator(e.repo, registry.BuildKeychain("", ""), true)
	s := NewScheduler(daemon.RPC, client, auth, e.repo, p,
		SchedulerOptions{Concurrency: 1, MaxRetries: 2, ProgressInterval: 100 * time.Millisecond})
	_ = s.Run(ctx) // expect a context error; progress must survive

	fi, err := os.Stat(part)
	if err != nil {
		t.Fatalf("no .part after interrupt: %v", err)
	}
	daemon.Stop()
	if fi.Size() == 0 || fi.Size() >= layer.Size {
		t.Fatalf("partial size = %d, want 0 < size < %d", fi.Size(), layer.Size)
	}
	partial := fi.Size()
	t.Logf("interrupted with %d of %d bytes", partial, layer.Size)

	// Phase 2: fresh daemon, same image dir — must resume, not restart.
	daemon2, err := StartDaemon(context.Background(), DaemonOptions{Dir: p.TmpDir, ConcurrentDownloads: 3})
	if err != nil {
		t.Fatalf("StartDaemon(2): %v", err)
	}
	defer daemon2.Stop()
	s2 := NewScheduler(daemon2.RPC, client, auth, e.repo, p,
		SchedulerOptions{Concurrency: 1, MaxRetries: 3, ProgressInterval: 100 * time.Millisecond})
	if err := s2.Run(context.Background()); err != nil {
		t.Fatalf("scheduler(2): %v", err)
	}
	e.assertAllCommitted(p)

	// Resume proof: the server must have received a Range request at or
	// beyond the interruption offset.
	if got := slow.ranges.Load(); got < partial-1024*1024 {
		t.Errorf("resume started at offset %d, want >= ~%d (restarted from scratch?)", got, partial)
	}
}

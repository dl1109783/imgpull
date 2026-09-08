package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"

	"imgpull/internal/plan"
	"imgpull/internal/reference"
	"imgpull/internal/registry"
)

// ---------------------------------------------------------------------------
// fake registry: /v2/ ping, token endpoint, manifests and blobs with optional
// redirect-to-CDN and Range support.
// ---------------------------------------------------------------------------

type fakeRegistry struct {
	mu          sync.Mutex
	repo        string
	tag         string
	manifestRaw []byte
	manifestMT  string
	blobs       map[string][]byte // digest → content
	cdn         *httptest.Server  // when set, blob HEAD redirects here
	requireAuth bool
	rangeBlind  bool // GET serves full 200 body regardless of Range
	tokens      int
	seenTokens  []string
}

func (f *fakeRegistry) checkAuth(r *http.Request) bool {
	if !f.requireAuth {
		return true
	}
	auth := r.Header.Get("Authorization")
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.seenTokens {
		if t == auth {
			return true
		}
	}
	return false
}

func (f *fakeRegistry) handle(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// The token endpoint itself must not require a Bearer token.
	if strings.HasPrefix(path, "/token") {
		f.mu.Lock()
		f.tokens++
		tok := fmt.Sprintf("tok-%d", f.tokens)
		f.seenTokens = append(f.seenTokens, "Bearer "+tok)
		f.mu.Unlock()
		fmt.Fprintf(w, `{"token":%q,"expires_in":3600}`, tok)
		return
	}
	if !f.checkAuth(r) {
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer realm="%s://%s/token",service="test.local"`, schemeOf(r), r.Host))
		w.WriteHeader(401)
		return
	}
	switch {
	case path == "/v2/":
		w.WriteHeader(200)
	case strings.Contains(path, "/manifests/"):
		w.Header().Set("Content-Type", f.manifestMT)
		w.Write(f.manifestRaw)
	case strings.Contains(path, "/blobs/"):
		parts := strings.Split(path, "/")
		digest := parts[len(parts)-1]
		data, ok := f.blobs[digest]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if f.cdn != nil && r.Method == http.MethodHead {
			// Simulate registry redirecting to a signed CDN URL.
			http.Redirect(w, r, f.cdn.URL+"/blob/"+digest, http.StatusFound)
			return
		}
		if f.rangeBlind && r.Method == http.MethodGet {
			// Range-blind mirror: full 200 body regardless of Range.
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.WriteHeader(200)
			w.Write(data)
			return
		}
		serveWithRange(w, r, data)
	default:
		w.WriteHeader(404)
	}
}

// serveWithRange writes data honoring the Range header with 206 responses.
func serveWithRange(w http.ResponseWriter, r *http.Request, data []byte) {
	rng := r.Header.Get("Range")
	if rng == "" || r.Method == http.MethodHead {
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.WriteHeader(200)
		if r.Method != http.MethodHead {
			w.Write(data)
		}
		return
	}
	var start int64
	if _, err := fmt.Sscanf(rng, "bytes=%d-", &start); err != nil || start > int64(len(data)) {
		w.WriteHeader(416)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
	w.Header().Set("Content-Length", fmt.Sprint(int64(len(data))-start))
	w.WriteHeader(206)
	w.Write(data[start:])
}

// fakeCDN hosts the blobs the registry redirects to (no auth needed).
type fakeCDN struct{ blobs map[string][]byte }

func (c *fakeCDN) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	digest := strings.TrimPrefix(r.URL.Path, "/blob/")
	data, ok := c.blobs[digest]
	if !ok {
		w.WriteHeader(404)
		return
	}
	serveWithRange(w, r, data)
}

func schemeOf(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// ---------------------------------------------------------------------------
// fake aria2: JSON-RPC server that really downloads over HTTP with Range.
// ---------------------------------------------------------------------------

type fakeTask struct {
	gid      string
	url      string
	dir      string
	out      string
	headers  []string
	checksum string
	status   string // active|complete|error|removed
	errCode  string
	errMsg   string
	done     int64
	total    int64
}

type fakeAria2 struct {
	mu       sync.Mutex
	seq      int
	tasks    map[string]*fakeTask
	seenURLs map[string]string // out name → last submitted URL
	failOnce map[string]bool   // out name → first attempt fails midway
	corrupt  map[string]bool   // out name → complete with wrong bytes
	// failCodeOnce makes the next resume attempt abort with the given
	// aria2 errorCode before any bytes are fetched (e.g. "8").
	failCodeOnce map[string]string
	// lastOpts is the option map of the most recent addUri call.
	lastOpts map[string]interface{}
}

func newFakeAria2() *fakeAria2 {
	return &fakeAria2{
		tasks: map[string]*fakeTask{}, seenURLs: map[string]string{},
		failOnce: map[string]bool{}, corrupt: map[string]bool{},
		failCodeOnce: map[string]string{}, lastOpts: map[string]interface{}{},
	}
}

func (a *fakeAria2) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil {
			writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: -32600, Message: "bad request"}})
			return
		}
		a.dispatch(w, &req)
	})
}

func writeRPC(w http.ResponseWriter, resp rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *fakeAria2) dispatch(w http.ResponseWriter, req *rpcRequest) {
	switch req.Method {
	case "aria2.getVersion":
		writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(`{"version":"1.36-fake"}`)})
	case "aria2.addUri":
		a.addURI(w, req)
	case "aria2.tellStatus":
		a.tellStatus(w, req, stringOf(req.Params))
	case "aria2.tellActive":
		a.tellList(w, req, "active")
	case "aria2.tellWaiting":
		a.tellList(w, req, "waiting")
	case "aria2.removeDownloadResult":
		gid := stringOf(req.Params)
		a.mu.Lock()
		delete(a.tasks, gid)
		a.mu.Unlock()
		writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(`"ok"`)})
	case "aria2.forceRemove":
		gid := stringOf(req.Params)
		a.mu.Lock()
		if t, ok := a.tasks[gid]; ok {
			t.status = "removed"
		}
		a.mu.Unlock()
		writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(`"ok"`)})
	default:
		writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: 404, Message: "method not found: " + req.Method}})
	}
}

func stringOf(params []interface{}) string {
	if len(params) == 0 {
		return ""
	}
	if s, ok := params[0].(string); ok {
		return s
	}
	return ""
}

func (a *fakeAria2) addURI(w http.ResponseWriter, req *rpcRequest) {
	var uris []string
	opts := map[string]interface{}{}
	if len(req.Params) > 0 {
		if list, ok := req.Params[0].([]interface{}); ok {
			for _, u := range list {
				if s, ok := u.(string); ok {
					uris = append(uris, s)
				}
			}
		}
	}
	if len(req.Params) > 1 {
		if m, ok := req.Params[1].(map[string]interface{}); ok {
			opts = m
		}
	}
	url := ""
	if len(uris) > 0 {
		url = uris[0]
	}
	a.mu.Lock()
	a.seq++
	gid := fmt.Sprintf("g%d", a.seq)
	t := &fakeTask{
		gid: gid, url: url,
		dir: strOpt(opts, "dir"), out: strOpt(opts, "out"),
		checksum: strOpt(opts, "checksum"), status: "active",
	}
	if h, ok := opts["header"].([]interface{}); ok {
		for _, v := range h {
			if s, ok := v.(string); ok {
				t.headers = append(t.headers, s)
			}
		}
	}
	a.tasks[gid] = t
	a.seenURLs[t.out] = url
	a.lastOpts = opts
	a.mu.Unlock()
	writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(fmt.Sprintf("%q", gid))})
	go a.run(t)
}

func strOpt(opts map[string]interface{}, key string) string {
	if v, ok := opts[key].(string); ok {
		return v
	}
	return ""
}

// run emulates aria2's downloader: resumes from .part with Range, honors
// injected failures and corruption, then flips task status.
func (a *fakeAria2) run(t *fakeTask) {
	a.mu.Lock()
	out := t.out
	shouldFail := a.failOnce[out]
	shouldCorrupt := a.corrupt[out]
	injectCode := a.failCodeOnce[out]
	if shouldFail {
		a.failOnce[out] = false
	}
	if shouldCorrupt {
		a.corrupt[out] = false
	}
	if injectCode != "" {
		a.failCodeOnce[out] = ""
	}
	a.mu.Unlock()

	part := filepath.Join(t.dir, t.out)
	offset := int64(0)
	if fi, err := os.Stat(part); err == nil {
		offset = fi.Size()
	}
	if injectCode != "" && offset > 0 {
		// Reject the resume before fetching anything, like aria2 aborting
		// on a bad/expired URL or a Range-blind source.
		a.failTask(t, injectCode, "No URI available.")
		return
	}
	req, err := http.NewRequest(http.MethodGet, t.url, nil)
	if err != nil {
		a.failTask(t, "1", err.Error())
		return
	}
	for _, h := range t.headers {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		a.failTask(t, "2", err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		a.failTask(t, "3", fmt.Sprintf("status=%d", resp.StatusCode))
		return
	}
	var data []byte
	var total int64
	if resp.StatusCode == 200 && offset > 0 {
		// Server ignored the resume Range: real aria2 aborts here with
		// errorCode 8 ("Invalid range header" → no URI available).
		a.failTask(t, "8", "No URI available.")
		return
	}
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		a.failTask(t, "1", err.Error())
		return
	}
	total = resp.ContentLength + offset
	if shouldFail && len(data) > 1 {
		// Write half then fail like a network cut.
		f, _ := os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		f.Write(data[:len(data)/2])
		f.Close()
		a.mu.Lock()
		t.status, t.errCode, t.errMsg = "error", "22", "simulated network failure"
		t.done, t.total = offset+int64(len(data)/2), total
		a.mu.Unlock()
		return
	}
	if shouldCorrupt {
		for i := range data {
			data[i] = 'X'
		}
	}
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		a.failTask(t, "10", err.Error())
		return
	}
	f.Write(data)
	f.Close()
	a.mu.Lock()
	t.status = "complete"
	t.done, t.total = total, total
	a.mu.Unlock()
}

func (a *fakeAria2) failTask(t *fakeTask, code, msg string) {
	a.mu.Lock()
	t.status, t.errCode, t.errMsg = "error", code, msg
	a.mu.Unlock()
}

func (a *fakeAria2) tellStatus(w http.ResponseWriter, req *rpcRequest, gid string) {
	// Serialize taskJSON under the lock: the run() goroutine mutates task
	// fields concurrently.
	a.mu.Lock()
	t, ok := a.tasks[gid]
	var result json.RawMessage
	if ok {
		result = taskJSON(t)
	}
	a.mu.Unlock()
	if !ok {
		writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: 1, Message: "gid not found"}})
		return
	}
	writeRPC(w, rpcResponse{ID: req.ID, Result: result})
}

func (a *fakeAria2) tellList(w http.ResponseWriter, req *rpcRequest, want string) {
	a.mu.Lock()
	var out []json.RawMessage
	for _, t := range a.tasks {
		if t.status == want {
			out = append(out, taskJSON(t))
		}
	}
	a.mu.Unlock()
	if out == nil {
		out = []json.RawMessage{}
	}
	b, _ := json.Marshal(out)
	writeRPC(w, rpcResponse{ID: req.ID, Result: b})
}

func taskJSON(t *fakeTask) json.RawMessage {
	b, _ := json.Marshal(map[string]interface{}{
		"gid":             t.gid,
		"status":          t.status,
		"errorCode":       t.errCode,
		"errorMessage":    t.errMsg,
		"totalLength":     fmt.Sprint(t.total),
		"completedLength": fmt.Sprint(t.done),
		"downloadSpeed":   "1000",
		"files":           []map[string]string{{"path": filepath.Join(t.dir, t.out), "length": fmt.Sprint(t.total)}},
	})
	return b
}

// ---------------------------------------------------------------------------
// test scaffolding: full plan + scheduler run against the fakes.
// ---------------------------------------------------------------------------

type testEnv struct {
	t       *testing.T
	regSrv  *httptest.Server
	ariaSrv *httptest.Server
	reg     *fakeRegistry
	aria    *fakeAria2
	repo    name.Repository
	mfst    *registry.ImageManifest
	root    string
}

// newTestEnv builds a registry with 1 config + 2 layers, a fake aria2, and
// resolves the manifest through the real ggcr code path.
func newTestEnv(t *testing.T, redirect, requireAuth bool) *testEnv {
	t.Helper()
	reg := &fakeRegistry{
		repo: "test/repo", tag: "dev",
		blobs: map[string][]byte{}, manifestMT: registry.MTOCIManifest, requireAuth: requireAuth,
	}
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	layer1 := make([]byte, 64*1024)
	layer2 := make([]byte, 32*1024)
	for i := range layer1 {
		layer1[i] = byte(i % 251)
	}
	for i := range layer2 {
		layer2[i] = byte(i % 13)
	}
	digestOf := func(b []byte) string {
		sum := sha256.Sum256(b)
		return "sha256:" + fmt.Sprintf("%x", sum)
	}
	cfgD, l1D, l2D := digestOf(config), digestOf(layer1), digestOf(layer2)
	reg.blobs[cfgD] = config
	reg.blobs[l1D] = layer1
	reg.blobs[l2D] = layer2

	raw := fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,
	"config":{"mediaType":%q,"digest":%q,"size":%d},
	"layers":[
	{"mediaType":%q,"digest":%q,"size":%d},
	{"mediaType":%q,"digest":%q,"size":%d}]}`,
		registry.MTOCIManifest, registry.MTOCIConfig, cfgD, len(config),
		registry.MTOCILayer, l1D, len(layer1), registry.MTOCILayer, l2D, len(layer2))
	reg.manifestRaw = []byte(raw)

	if redirect {
		reg.cdn = httptest.NewServer(&fakeCDN{blobs: reg.blobs})
		t.Cleanup(reg.cdn.Close)
	}

	regSrv := httptest.NewServer(http.HandlerFunc(reg.handle))
	t.Cleanup(regSrv.Close)

	aria := newFakeAria2()
	ariaSrv := httptest.NewServer(aria.handler())
	t.Cleanup(ariaSrv.Close)

	host := strings.TrimPrefix(regSrv.URL, "http://")
	ref, err := reference.Parse(host+"/test/repo:dev", true)
	if err != nil {
		t.Fatal(err)
	}
	keychain := registry.BuildKeychain("", "")
	mfst, err := registry.ResolveManifest(context.Background(), ref.Reference,
		&registry.Platform{OS: "linux", Architecture: "amd64"}, keychain)
	if err != nil {
		t.Fatal(err)
	}
	return &testEnv{
		t: t, regSrv: regSrv, ariaSrv: ariaSrv, reg: reg, aria: aria,
		repo: ref.Reference.Context(), mfst: mfst, root: t.TempDir(),
	}
}

func (e *testEnv) buildPlan() *plan.Plan {
	e.t.Helper()
	ref, err := reference.Parse(strings.TrimPrefix(e.regSrv.URL, "http://")+"/test/repo:dev", true)
	if err != nil {
		e.t.Fatal(err)
	}
	p, err := plan.Build(ref, e.mfst, &registry.Platform{OS: "linux", Architecture: "amd64"},
		e.root, plan.BuildOptions{})
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

func (e *testEnv) runScheduler(ctx context.Context, p *plan.Plan, maxRetries int) error {
	e.t.Helper()
	rpc := NewRPC(e.ariaSrv.URL, "")
	client := registry.NewClient(true)
	auth := registry.NewAuthenticator(e.repo, registry.BuildKeychain("", ""), true)
	s := NewScheduler(rpc, client, auth, e.repo, p,
		SchedulerOptions{Concurrency: 3, MaxRetries: maxRetries, ProgressInterval: 100 * time.Millisecond})
	return s.Run(ctx)
}

// runSchedulerProxy is runScheduler with an aria2 all-proxy configured.
func (e *testEnv) runSchedulerProxy(ctx context.Context, p *plan.Plan, maxRetries int, proxy string) error {
	e.t.Helper()
	rpc := NewRPC(e.ariaSrv.URL, "")
	client := registry.NewClient(true)
	auth := registry.NewAuthenticator(e.repo, registry.BuildKeychain("", ""), true)
	s := NewScheduler(rpc, client, auth, e.repo, p,
		SchedulerOptions{Concurrency: 3, MaxRetries: maxRetries, ProgressInterval: 100 * time.Millisecond, Proxy: proxy})
	return s.Run(ctx)
}

func (e *testEnv) assertAllCommitted(p *plan.Plan) {
	e.t.Helper()
	for _, t := range p.Objects {
		if t.Status != plan.StatusCommitted {
			e.t.Errorf("object %s status = %s, want committed", t.Digest, t.Status)
		}
		final := p.FinalPath(t)
		fi, err := os.Stat(final)
		if err != nil || fi.Size() != t.Size {
			e.t.Errorf("blob %s missing or wrong size", final)
		}
		if _, err := os.Stat(p.PartPath(t)); !os.IsNotExist(err) {
			e.t.Errorf("part file should be gone: %s", p.PartPath(t))
		}
	}
}

// ---------------------------------------------------------------------------
// scenarios
// ---------------------------------------------------------------------------

func TestSchedulerEndToEndRedirect(t *testing.T) {
	e := newTestEnv(t, true /*redirect*/, false)
	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runScheduler(ctx, p, 5); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)

	// Redirected URLs must have been submitted to aria2 (CDN URL, not
	// registry URL; committed tasks clear t.URL in state, so check the
	// URLs the fake aria2 actually received).
	layers := 0
	for _, ob := range p.Objects {
		if ob.Kind == plan.KindLayer {
			url := e.aria.seenURLs[ob.PartName]
			if url == "" || !strings.Contains(url, "/blob/") {
				t.Errorf("layer %s submitted URL = %q, want CDN blob URL", ob.Digest, url)
			}
			layers++
		}
	}
	if layers != 2 {
		t.Fatalf("layer count = %d, want 2", layers)
	}

	// The committed blobs must round-trip through ggcr's OCI layout reader.
	// (Full index assembly is covered in the layout/export tests; here we
	// check blobs/sha256 contents hash to the manifest digests.)
	mfstBlob := filepath.Join(p.BlobsDir, "sha256", strings.TrimPrefix(p.ManifestDigest, "sha256:"))
	raw, err := os.ReadFile(mfstBlob)
	if err != nil {
		t.Fatalf("manifest blob: %v", err)
	}
	sum := sha256.Sum256(raw)
	if "sha256:"+fmt.Sprintf("%x", sum) != p.ManifestDigest {
		t.Error("manifest blob content does not hash to manifest digest")
	}
}

func TestSchedulerDirectURLAndResume(t *testing.T) {
	e := newTestEnv(t, false /*direct*/, false)
	// Inject one midway failure for the biggest blob; retry must recover.
	e.aria.mu.Lock()
	var biggest string
	var size int
	for _, b := range e.reg.blobs {
		if len(b) > size {
			size = len(b)
			sum := sha256.Sum256(b)
			biggest = "sha256_" + fmt.Sprintf("%x", sum) + ".part"
		}
	}
	e.aria.failOnce[biggest] = true
	e.aria.mu.Unlock()

	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runScheduler(ctx, p, 5); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)

	found := false
	for _, ob := range p.Objects {
		if ob.PartName == biggest {
			found = true
			if ob.Retries < 1 {
				t.Errorf("layer retries = %d, want >= 1 after injected failure", ob.Retries)
			}
		}
		if ob.Kind == plan.KindLayer && !strings.HasPrefix(e.aria.seenURLs[ob.PartName], e.regSrv.URL) {
			t.Errorf("direct-mode layer URL = %q, want registry URL", e.aria.seenURLs[ob.PartName])
		}
	}
	if !found {
		t.Fatalf("injected layer %s not found in plan", biggest)
	}
}

func TestSchedulerCorruptPartRedownloads(t *testing.T) {
	e := newTestEnv(t, false, false)
	e.aria.mu.Lock()
	var somePart string
	for d := range e.reg.blobs {
		sum := sha256.Sum256(e.reg.blobs[d])
		somePart = "sha256_" + fmt.Sprintf("%x", sum) + ".part"
		break
	}
	e.aria.corrupt[somePart] = true
	e.aria.mu.Unlock()

	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runScheduler(ctx, p, 5); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	for _, ob := range p.Objects {
		if ob.PartName == somePart && ob.Retries < 1 {
			t.Errorf("corrupt blob retries = %d, want >= 1", ob.Retries)
		}
	}
}

func TestSchedulerRangeBlindMirrorRestartsPart(t *testing.T) {
	e := newTestEnv(t, false /*direct*/, false)
	e.reg.rangeBlind = true

	p := e.buildPlan()
	defer p.Release()

	// Seed a stale partial for the biggest object, as a previous run would
	// have left behind on a mirror that ignores Range requests.
	var biggest *plan.ObjectTask
	for _, ob := range p.Objects {
		if biggest == nil || ob.Size > biggest.Size {
			biggest = ob
		}
	}
	content := e.reg.blobs[biggest.Digest]
	part := p.PartPath(biggest)
	if err := os.WriteFile(part, content[:len(content)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part+".aria2", []byte("control"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Without the fix this loops: resume → 200 → code 8 → retry, ×5.
	if err := e.runScheduler(context.Background(), p, 4); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	if biggest.Retries < 1 {
		t.Errorf("retries = %d, want >= 1 after the code-8 resume failure", biggest.Retries)
	}
	if !biggest.RangeUnsupported {
		t.Errorf("RangeUnsupported = false, want true after the probe confirmed the mirror")
	}
}

func TestSchedulerCode8KeepsPartWhenRangeWorks(t *testing.T) {
	e := newTestEnv(t, false, false)
	p := e.buildPlan()
	defer p.Release()

	var biggest *plan.ObjectTask
	for _, ob := range p.Objects {
		if biggest == nil || ob.Size > biggest.Size {
			biggest = ob
		}
	}
	content := e.reg.blobs[biggest.Digest]
	part := p.PartPath(biggest)
	if err := os.WriteFile(part, content[:len(content)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	// Inject one code-8 abort (e.g. a momentarily bad URL) against a
	// Range-capable registry: the partial must survive and be reused.
	e.aria.mu.Lock()
	e.aria.failCodeOnce[biggest.PartName] = "8"
	e.aria.mu.Unlock()

	if err := e.runScheduler(context.Background(), p, 4); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	if biggest.Retries < 1 {
		t.Errorf("retries = %d, want >= 1 after the injected code-8 failure", biggest.Retries)
	}
	if biggest.RangeUnsupported {
		t.Errorf("RangeUnsupported = true, want false: the probe proved Range works")
	}
}

func TestSchedulerTokenAuth(t *testing.T) {
	e := newTestEnv(t, false, true /*requireAuth*/)
	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runScheduler(ctx, p, 5); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	e.reg.mu.Lock()
	defer e.reg.mu.Unlock()
	if e.reg.tokens == 0 {
		t.Error("registry never issued a token")
	}
}

func TestSchedulerPassesProxyToAria2(t *testing.T) {
	const proxy = "http://192.0.2.7:1080"
	e := newTestEnv(t, false, false)
	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runSchedulerProxy(ctx, p, 5, proxy); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	e.aria.mu.Lock()
	defer e.aria.mu.Unlock()
	if got := e.aria.lastOpts["all-proxy"]; got != proxy {
		t.Errorf("addUri all-proxy = %v, want %q", got, proxy)
	}
}

func TestSchedulerNoProxyByDefault(t *testing.T) {
	e := newTestEnv(t, false, false)
	p := e.buildPlan()
	defer p.Release()

	ctx := context.Background()
	if err := e.runScheduler(ctx, p, 5); err != nil {
		t.Fatalf("scheduler: %v", err)
	}
	e.assertAllCommitted(p)
	e.aria.mu.Lock()
	defer e.aria.mu.Unlock()
	if got, ok := e.aria.lastOpts["all-proxy"]; ok {
		t.Errorf("addUri unexpectedly carried all-proxy = %v", got)
	}
}

func TestSchedulerContextCancelKeepsProgress(t *testing.T) {
	e := newTestEnv(t, false, false)
	p := e.buildPlan()
	defer p.Release()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel as soon as any fake task goes active.
		for i := 0; i < 300; i++ {
			e.aria.mu.Lock()
			active := false
			for _, tk := range e.aria.tasks {
				if tk.status == "active" {
					active = true
				}
			}
			e.aria.mu.Unlock()
			if active {
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	err := e.runScheduler(ctx, p, 5)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Either .part files or committed blobs must exist — no data loss either way.
	progress := 0
	for _, ob := range p.Objects {
		if _, err := os.Stat(p.PartPath(ob)); err == nil {
			progress++
		}
		if ob.Status == plan.StatusCommitted {
			progress++
		}
	}
	if progress == 0 {
		t.Fatal("no .part files and no committed blobs after cancel — progress lost")
	}
}

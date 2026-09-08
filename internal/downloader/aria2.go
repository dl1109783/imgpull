// Package downloader drives aria2 over JSON-RPC to fetch each blob as a
// resumable task, and schedules retries and verification.
package downloader

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RPC is an aria2 JSON-RPC client.
type RPC struct {
	endpoint string
	secret   string
	client   *http.Client
	id       atomic.Int64
}

// NormalizeEndpoint ensures the endpoint is a full JSON-RPC URL.
func NormalizeEndpoint(ep string) string {
	if ep == "" {
		return ep
	}
	if !strings.Contains(ep, "://") {
		ep = "http://" + ep
	}
	if !strings.HasSuffix(ep, "/jsonrpc") {
		ep = strings.TrimRight(ep, "/") + "/jsonrpc"
	}
	return ep
}

// NewRPC creates a client for an already-running aria2 RPC endpoint.
func NewRPC(endpoint, secret string) *RPC {
	return &RPC{
		endpoint: NormalizeEndpoint(endpoint),
		secret:   secret,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

type rpcRequest struct {
	ID      string        `json:"id"`
	Version string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("aria2 rpc error %d: %s", e.Code, e.Message)
}

// rpcResponse mirrors JSON-RPC 2.0 envelope.
type rpcResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// Call invokes an aria2 RPC method, prepending the secret token.
func (r *RPC) Call(ctx context.Context, method string, params []interface{}, result interface{}) error {
	full := params
	if r.secret != "" {
		full = append([]interface{}{"token:" + r.secret}, params...)
	}
	req := rpcRequest{
		ID:      fmt.Sprintf("imgpull-%d-%d", os.Getpid(), r.id.Add(1)),
		Version: "2.0",
		Method:  method,
		Params:  full,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("aria2 rpc %s: %w", method, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	// aria2 answers bad requests (e.g. unknown options) with HTTP 400 but
	// still carries a JSON-RPC error body — parse it so callers can react
	// to the error code.
	var out rpcResponse
	unmarshalErr := json.Unmarshal(data, &out)
	if resp.StatusCode != http.StatusOK && (unmarshalErr != nil || out.Error == nil) {
		return fmt.Errorf("aria2 rpc %s: http %d: %s", method, resp.StatusCode, truncate(string(data), 200))
	}
	if unmarshalErr != nil {
		return fmt.Errorf("aria2 rpc %s: bad response: %w", method, unmarshalErr)
	}
	if out.Error != nil {
		return out.Error
	}
	if result != nil {
		if err := json.Unmarshal(out.Result, result); err != nil {
			return fmt.Errorf("aria2 rpc %s: decode result: %w", method, err)
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// AddURI submits a download task and returns its gid.
func (r *RPC) AddURI(ctx context.Context, uris []string, options map[string]interface{}) (string, error) {
	params := []interface{}{uris}
	if options != nil {
		params = append(params, options)
	}
	var gid string
	if err := r.Call(ctx, "aria2.addUri", params, &gid); err != nil {
		return "", err
	}
	return gid, nil
}

// DownloadFile is one file inside a download task.
type DownloadFile struct {
	Path   string `json:"path"`
	Length string `json:"length"`
}

// DownloadStatus is the subset of aria2.tellStatus we consume.
type DownloadStatus struct {
	GID             string         `json:"gid"`
	Status          string         `json:"status"` // active|waiting|paused|error|complete|removed
	ErrorCode       string         `json:"errorCode"`
	ErrorMessage    string         `json:"errorMessage"`
	TotalLength     string         `json:"totalLength"`
	CompletedLength string         `json:"completedLength"`
	DownloadSpeed   string         `json:"downloadSpeed"`
	Files           []DownloadFile `json:"files"`
}

// Progress returns (done, total, speed) parsed from string fields.
func (s *DownloadStatus) Progress() (int64, int64, int64) {
	d, _ := strconv.ParseInt(s.CompletedLength, 10, 64)
	t, _ := strconv.ParseInt(s.TotalLength, 10, 64)
	v, _ := strconv.ParseInt(s.DownloadSpeed, 10, 64)
	return d, t, v
}

// TellStatus queries one task.
func (r *RPC) TellStatus(ctx context.Context, gid string) (*DownloadStatus, error) {
	var st DownloadStatus
	if err := r.Call(ctx, "aria2.tellStatus", []interface{}{gid}, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// TellActive lists active downloads.
func (r *RPC) TellActive(ctx context.Context) ([]DownloadStatus, error) {
	var out []DownloadStatus
	err := r.Call(ctx, "aria2.tellActive", []interface{}{}, &out)
	return out, err
}

// TellWaiting lists queued downloads (offset 0, num 1000).
func (r *RPC) TellWaiting(ctx context.Context) ([]DownloadStatus, error) {
	var out []DownloadStatus
	err := r.Call(ctx, "aria2.tellWaiting", []interface{}{0, 1000}, &out)
	return out, err
}

// RemoveDownloadResult forgets a finished/errored task so its files list
// stops appearing; kept downloads are untouched.
func (r *RPC) RemoveDownloadResult(ctx context.Context, gid string) error {
	return r.Call(ctx, "aria2.removeDownloadResult", []interface{}{gid}, nil)
}

// ForceRemove cancels a running task immediately. Files already on disk
// (.part + .aria2) remain, enabling later resume.
func (r *RPC) ForceRemove(ctx context.Context, gid string) error {
	return r.Call(ctx, "aria2.forceRemove", []interface{}{gid}, nil)
}

// GetVersion pings the daemon.
func (r *RPC) GetVersion(ctx context.Context) (string, error) {
	var v struct {
		Version string `json:"version"`
	}
	if err := r.Call(ctx, "aria2.getVersion", []interface{}{}, &v); err != nil {
		return "", err
	}
	return v.Version, nil
}

// Shutdown asks the daemon to exit gracefully.
func (r *RPC) Shutdown(ctx context.Context) error {
	return r.Call(ctx, "aria2.shutdown", []interface{}{}, nil)
}

// DaemonOptions configures a locally spawned aria2c daemon.
type DaemonOptions struct {
	Dir                 string // default --dir for tasks
	LogPath             string // when empty, aria2 logs to stderr (captured)
	ConcurrentDownloads int
}

// Daemon is a locally spawned aria2c RPC daemon.
type Daemon struct {
	RPC      *RPC
	Port     int
	Secret   string
	cmd      *exec.Cmd
	stderrMu sync.Mutex //nolint:unused // guarded buffer; kept for future debugging
	stderr   bytes.Buffer
	exited   chan struct{}
}

// StartDaemon spawns aria2c with imgpull's required resume settings and
// waits until the RPC endpoint answers. The daemon is stopped automatically
// when this process dies (--stop-with-process) and via Stop().
func StartDaemon(ctx context.Context, opts DaemonOptions) (*Daemon, error) {
	aria2c, err := exec.LookPath("aria2c")
	if err != nil {
		return nil, fmt.Errorf("aria2c not found in PATH: %w (install aria2 or use --aria2-rpc)", err)
	}
	port, err := freeTCPPort()
	if err != nil {
		return nil, err
	}
	secret, err := randomSecret()
	if err != nil {
		return nil, err
	}
	if opts.ConcurrentDownloads <= 0 {
		opts.ConcurrentDownloads = 3
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	args := []string{
		"--enable-rpc=true",
		"--rpc-listen-all=false",
		"--rpc-listen-port=" + strconv.Itoa(port),
		"--rpc-secret=" + secret,
		"--continue=true",
		"--allow-overwrite=false",
		"--auto-file-renaming=false",
		"--max-connection-per-server=1",
		"--split=1",
		"--max-concurrent-downloads=" + strconv.Itoa(opts.ConcurrentDownloads),
		"--max-tries=5",
		"--retry-wait=3",
		"--timeout=60",
		"--connect-timeout=30",
		"--file-allocation=none",
		"--quiet=true",
		"--summary-interval=0",
		"--stop-with-process=" + strconv.Itoa(os.Getpid()),
		"--dir=" + opts.Dir,
	}
	if opts.LogPath != "" {
		args = append(args, "--log="+opts.LogPath, "--log-level=notice")
	}
	for _, a := range proxyArgs() {
		args = append(args, a)
	}

	cmd := exec.Command(aria2c, args...)
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start aria2c: %w", err)
	}
	d := &Daemon{
		Port:   port,
		Secret: secret,
		cmd:    cmd,
		exited: make(chan struct{}),
	}
	// Capture stderr for post-mortem.
	go func() {
		defer close(d.exited)
		buf := make([]byte, 8192)
		for {
			n, err := stderrPipe.Read(buf)
			if n > 0 {
				d.stderrMu.Lock()
				if d.stderr.Len() < 64<<10 {
					d.stderr.Write(buf[:n])
				}
				d.stderrMu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	rpc := NewRPC(fmt.Sprintf("http://127.0.0.1:%d/jsonrpc", port), secret)
	d.RPC = rpc
	deadline := time.Now().Add(15 * time.Second)
	for {
		vctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := rpc.GetVersion(vctx)
		cancel()
		if err == nil {
			return d, nil
		}
		select {
		case <-ctx.Done():
			d.Stop()
			return nil, ctx.Err()
		case <-d.exited:
			d.stderrMu.Lock()
			out := d.stderr.String()
			d.stderrMu.Unlock()
			return nil, fmt.Errorf("aria2c exited during startup: %s", truncate(strings.TrimSpace(out), 400))
		default:
		}
		if time.Now().After(deadline) {
			d.Stop()
			return nil, fmt.Errorf("aria2c RPC not ready after 15s: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Stop shuts the daemon down (gracefully), falling back to SIGTERM/Kill.
func (d *Daemon) Stop() {
	if d == nil || d.cmd == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	d.RPC.Shutdown(ctx)
	cancel()
	select {
	case <-d.exited:
		return
	case <-time.After(5 * time.Second):
	}
	if d.cmd.Process != nil {
		d.cmd.Process.Kill()
	}
}

func (d *Daemon) exitedNow() bool {
	select {
	case <-d.exited:
		return true
	default:
		return false
	}
}

func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func randomSecret() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

var proxyEnvMap = map[string]string{
	"HTTP_PROXY":  "--http-proxy",
	"HTTPS_PROXY": "--https-proxy",
	"ALL_PROXY":   "--all-proxy",
	"NO_PROXY":    "--no-proxy",
}

// proxyArgs translates proxy environment variables into aria2c flags so
// spawned daemons honor the same proxies as the rest of the system.
func proxyArgs() []string {
	var args []string
	seen := map[string]bool{}
	for env, flag := range proxyEnvMap {
		v := os.Getenv(env)
		if v == "" {
			v = os.Getenv(strings.ToLower(env))
		}
		if v == "" || seen[flag] {
			continue
		}
		seen[flag] = true
		args = append(args, flag+"="+v)
	}
	return args
}

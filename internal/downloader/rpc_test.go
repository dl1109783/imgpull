package downloader

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeRPCServer answers a fixed method and echoes params for others.
func fakeRPCServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/jsonrpc", func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		json.NewDecoder(r.Body).Decode(&req)
		if got := req.Params[0].(string); got != "token:sekret" {
			writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: 401, Message: "bad token"}})
			return
		}
		switch req.Method {
		case "aria2.getVersion":
			writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(`{"version":"1.36.0"}`)})
		case "aria2.addUri":
			writeRPC(w, rpcResponse{ID: req.ID, Result: json.RawMessage(`"gid42"`)})
		case "aria2.failMethod":
			writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: 3, Message: "file not found"}})
		default:
			writeRPC(w, rpcResponse{ID: req.ID, Error: &rpcError{Code: 404, Message: "unknown"}})
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestRPCCallWithSecret(t *testing.T) {
	srv := fakeRPCServer(t)
	rpc := NewRPC(srv.URL+"/jsonrpc", "sekret")

	v, err := rpc.GetVersion(context.Background())
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if v != "1.36.0" {
		t.Errorf("version = %q, want 1.36.0", v)
	}

	gid, err := rpc.AddURI(context.Background(), []string{"http://example/x"}, map[string]interface{}{"dir": "/tmp"})
	if err != nil || gid != "gid42" {
		t.Errorf("AddURI = %q, %v; want gid42, nil", gid, err)
	}
}

func TestRPCErrorPropagates(t *testing.T) {
	srv := fakeRPCServer(t)
	rpc := NewRPC(srv.URL+"/jsonrpc", "sekret")

	err := rpc.Call(context.Background(), "aria2.failMethod", nil, nil)
	if err == nil {
		t.Fatal("want error for failing method")
	}
	re, ok := err.(*rpcError)
	if !ok {
		t.Fatalf("error type = %T, want *rpcError", err)
	}
	if re.Code != 3 {
		t.Errorf("code = %d, want 3", re.Code)
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://1.2.3.4:6800/jsonrpc", "http://1.2.3.4:6800/jsonrpc"},
		{"1.2.3.4:6800/jsonrpc", "http://1.2.3.4:6800/jsonrpc"},
		{"1.2.3.4:6800", "http://1.2.3.4:6800/jsonrpc"},
		{"http://1.2.3.4:6800", "http://1.2.3.4:6800/jsonrpc"},
	}
	for _, c := range cases {
		if got := NormalizeEndpoint(c.in); got != c.want {
			t.Errorf("NormalizeEndpoint(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestClassifyAria2Failure(t *testing.T) {
	cases := []struct {
		code, msg string
		check     func(error) bool
		name      string
	}{
		{"3", "file not found", isNonRetryable, "fatal"},
		{"9", "disk alloc", isNonRetryable, "fatal"},
		{"15", "unknown option", isNonRetryable, "fatal"},
		{"11", "range not supported", needsPartCleanup, "cleanup"},
		{"13", "resume unsupported", needsPartCleanup, "cleanup"},
		{"22", "cannot resume", needsPartCleanup, "cleanup"},
		{"14", "auth failed", isAuthError, "auth"},
		{"24", "authorization failed", isAuthError, "auth"},
		{"0", "got 401 unauthorized", isAuthError, "auth"},
		{"2", "timeout", isRetryable, "retryable"},
	}
	for _, c := range cases {
		st := &DownloadStatus{ErrorCode: c.code, ErrorMessage: c.msg}
		err := classifyAria2Failure(st)
		if !c.check(err) {
			t.Errorf("classify(%s,%q): want %s, got %v", c.code, c.msg, c.name, err)
		}
	}
}

func TestBackoffDelayCaps(t *testing.T) {
	for i := 0; i < 50; i++ {
		base := backoffDelay(1)
		if base < 700*time.Millisecond || base > 1300*time.Millisecond {
			t.Errorf("first backoff = %v, want ~1s±20%%", base)
		}
	}
	capd := backoffDelay(30)
	if capd > 30*time.Second {
		t.Errorf("backoff cap exceeded: %v", capd)
	}
}

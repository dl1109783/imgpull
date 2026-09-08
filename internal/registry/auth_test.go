package registry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

func TestParseChallenge(t *testing.T) {
	c, err := parseChallenge(`Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"`)
	if err != nil {
		t.Fatal(err)
	}
	if c.scheme != "Bearer" {
		t.Errorf("scheme = %q", c.scheme)
	}
	if c.params["realm"] != "https://auth.docker.io/token" {
		t.Errorf("realm = %q", c.params["realm"])
	}
	if c.params["service"] != "registry.docker.io" {
		t.Errorf("service = %q", c.params["service"])
	}
	// Value containing an escaped quote must not break the scanner.
	c2, err := parseChallenge(`Bearer realm="http://h/token",note="a\"b",x="1"`)
	if err != nil {
		t.Fatal(err)
	}
	if c2.params["note"] != `a"b` || c2.params["x"] != "1" {
		t.Errorf("params = %v", c2.params)
	}
	if _, err := parseChallenge(""); err == nil {
		t.Error("empty challenge should fail")
	}
}

// fakeTokenRegistry exercises the full anonymous → challenge → token → blob
// flow, including token refresh after Invalidate().
func TestAuthenticatorTokenFlow(t *testing.T) {
	var tokens atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		// Tokens must be "tok-N"; tok-0-style requests are rejected.
		if auth == "Bearer tok-1" && tokens.Load() >= 1 {
			w.WriteHeader(200)
			fmt.Fprint(w, "{}")
			return
		}
		if auth == "Bearer tok-2" && tokens.Load() >= 2 {
			w.WriteHeader(200)
			fmt.Fprint(w, "{}")
			return
		}
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer realm="%s/token",service="test.local"`, testURL(r)))
		w.WriteHeader(401)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		n := tokens.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"token":"tok-%d","expires_in":3600}`, n)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	repo, err := name.NewRepository(srv.URL[7:]+"/some/repo", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAuthenticator(repo, authn.DefaultKeychain, true)

	h, err := a.Header(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h != "Bearer tok-1" {
		t.Fatalf("first token = %q", h)
	}
	// Cached: repeated call must not mint a new token.
	if h2, _ := a.Header(context.Background()); h2 != "Bearer tok-1" {
		t.Fatalf("cached token = %q", h2)
	}
	// Invalidate → refresh with a new token.
	a.Invalidate()
	if h3, _ := a.Header(context.Background()); h3 != "Bearer tok-2" {
		t.Fatalf("refreshed token = %q", h3)
	}
}

// testURL returns the base URL of the request's host for challenge realms.
func testURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func TestBuildKeychain(t *testing.T) {
	kc := BuildKeychain("user", "pass")
	res, err := name.NewRegistry("docker.io")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := kc.Resolve(res)
	if err != nil {
		t.Fatal(err)
	}
	ac, err := cfg.Authorization()
	if err != nil {
		t.Fatal(err)
	}
	if ac.Username != "user" || ac.Password != "pass" {
		t.Errorf("creds = %v", ac)
	}
}

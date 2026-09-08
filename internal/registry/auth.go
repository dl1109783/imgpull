package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

// staticKeychain serves fixed credentials for every registry.
type staticKeychain struct{ cfg authn.AuthConfig }

func (k staticKeychain) Resolve(authn.Resource) (authn.Authenticator, error) {
	return authn.FromConfig(k.cfg), nil
}

// BuildKeychain composes explicit --username/--password credentials (when
// given) with the default keychain (docker config.json logins).
func BuildKeychain(username, password string) authn.Keychain {
	if username != "" {
		return authn.NewMultiKeychain(
			staticKeychain{authn.AuthConfig{Username: username, Password: password}},
			authn.DefaultKeychain,
		)
	}
	return authn.DefaultKeychain
}

// challenge is a parsed WWW-Authenticate header.
type challenge struct {
	scheme string
	params map[string]string
}

// parseChallenge parses headers like:
// Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
func parseChallenge(v string) (*challenge, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, fmt.Errorf("empty challenge")
	}
	i := strings.IndexAny(v, " \t")
	if i < 0 {
		return &challenge{scheme: v, params: map[string]string{}}, nil
	}
	c := &challenge{
		scheme: v[:i],
		params: map[string]string{},
	}
	rest := v[i+1:]
	// Scan key="value" pairs, tolerating commas inside values.
	for len(rest) > 0 {
		eq := strings.Index(rest, "=")
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(rest[:eq])
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			j := 1
			for ; j < len(rest); j++ {
				if rest[j] == '"' && rest[j-1] != '\\' {
					break
				}
			}
			if j >= len(rest) {
				return nil, fmt.Errorf("malformed challenge %q", v)
			}
			val = rest[1:j]
			val = strings.ReplaceAll(val, `\"`, `"`)
			rest = strings.TrimPrefix(rest[j+1:], ",")
		} else {
			j := strings.Index(rest, ",")
			if j < 0 {
				val = rest
				rest = ""
			} else {
				val = rest[:j]
				rest = rest[j+1:]
			}
			val = strings.TrimSpace(val)
		}
		if key != "" {
			c.params[strings.ToLower(key)] = val
		}
	}
	return c, nil
}

// Authenticator supplies an Authorization header value for raw HTTP blob
// requests made outside of go-containerregistry (i.e. by aria2). ggcr
// deliberately does not expose its bearer tokens, so we replay the auth
// dance ourselves: ping /v2/, read the WWW-Authenticate challenge, fetch a
// token from the realm. Token refreshes are cached until shortly before
// expiry.
type Authenticator struct {
	repo      name.Repository
	keychain  authn.Keychain
	scheme    string // "http" or "https" for registry requests
	client    *http.Client
	userAgent string

	mu        sync.Mutex
	authValue string
	expiresAt time.Time
	refreshed bool
}

// NewAuthenticator creates an Authenticator for the repository.
func NewAuthenticator(repo name.Repository, keychain authn.Keychain, insecure bool) *Authenticator {
	return &Authenticator{
		repo:      repo,
		keychain:  keychain,
		scheme:    registryScheme(repo.RegistryStr(), insecure),
		client:    &http.Client{Timeout: 60 * time.Second},
		userAgent: userAgent,
	}
}

// Header returns a usable Authorization header value ("" for anonymous).
// On 401, callers should call Invalidate() and ask again.
func (a *Authenticator) Header(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.refreshed && (a.authValue == "" || time.Now().Before(a.expiresAt.Add(-30*time.Second))) {
		return a.authValue, nil
	}
	if err := a.refreshLocked(ctx); err != nil {
		return "", err
	}
	return a.authValue, nil
}

// Invalidate drops the cached token so the next Header call refreshes it.
func (a *Authenticator) Invalidate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refreshed = false
	a.authValue = ""
	a.expiresAt = time.Time{}
}

func (a *Authenticator) refreshLocked(ctx context.Context) error {
	pingURL := fmt.Sprintf("%s://%s/v2/", a.scheme, a.repo.RegistryStr())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", a.userAgent)
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("registry ping %s: %w", pingURL, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// Anonymous access allowed.
		a.authValue = ""
		a.expiresAt = time.Now().Add(time.Hour)
		a.refreshed = true
		return nil
	case resp.StatusCode == http.StatusUnauthorized:
		return a.handleChallengeLocked(ctx, resp.Header.Get("WWW-Authenticate"))
	default:
		return fmt.Errorf("registry ping %s: unexpected status %d", pingURL, resp.StatusCode)
	}
}

func (a *Authenticator) handleChallengeLocked(ctx context.Context, header string) error {
	c, err := parseChallenge(header)
	if err != nil {
		return fmt.Errorf("registry challenge: %w (WWW-Authenticate: %q)", err, header)
	}
	switch strings.ToLower(c.scheme) {
	case "bearer":
		return a.fetchBearerLocked(ctx, c)
	case "basic":
		// Registry wants HTTP Basic directly.
		val, err := a.basicValue()
		if err != nil {
			return err
		}
		a.authValue = val
		a.expiresAt = time.Now().Add(time.Hour)
		a.refreshed = true
		return nil
	default:
		return fmt.Errorf("unsupported auth scheme %q", c.scheme)
	}
}

func (a *Authenticator) basicValue() (string, error) {
	creds, err := a.resolveCreds()
	if err != nil {
		return "", err
	}
	if creds == nil || creds.Username == "" {
		return "", fmt.Errorf("registry requires basic auth but no credentials found (use --username/--password or docker login)")
	}
	return "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(creds.Username+":"+creds.Password)), nil
}

func (a *Authenticator) fetchBearerLocked(ctx context.Context, c *challenge) error {
	realm := c.params["realm"]
	if realm == "" {
		return fmt.Errorf("bearer challenge missing realm")
	}
	q := map[string]string{
		"scope": a.repo.Scope("pull"),
	}
	if svc := c.params["service"]; svc != "" {
		q["service"] = svc
	}
	if creds, err := a.resolveCreds(); err == nil && creds != nil && creds.Username != "" {
		q["account"] = creds.Username
	}
	u := realm + "?" + encodeQuery(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", a.userAgent)
	// Attach basic auth to the token endpoint when credentials exist.
	if creds, err := a.resolveCreds(); err == nil && creds.Username != "" {
		req.SetBasicAuth(creds.Username, creds.Password)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("token fetch %s: %w", u, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token fetch %s: status %d", u, resp.StatusCode)
	}
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		IssuedAt    string `json:"issued_at"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return fmt.Errorf("token response: %w", err)
	}
	token := tok.Token
	if token == "" {
		token = tok.AccessToken
	}
	if token == "" {
		return fmt.Errorf("token response contains no token")
	}
	expires := 60 * time.Second
	if tok.ExpiresIn > 0 {
		expires = time.Duration(tok.ExpiresIn) * time.Second
	}
	if t, err := time.Parse(time.RFC3339, tok.IssuedAt); err == nil && !t.IsZero() {
		a.expiresAt = t.Add(expires)
	} else {
		a.expiresAt = time.Now().Add(expires)
	}
	a.authValue = "Bearer " + token
	a.refreshed = true
	return nil
}

// resolveCreds looks up credentials for the repository's registry.
// Returns nil creds when nothing is configured (anonymous pull).
func (a *Authenticator) resolveCreds() (*authn.AuthConfig, error) {
	return resolveCreds(a.repo.RegistryStr(), a.keychain)
}

func resolveCreds(registry string, keychain authn.Keychain) (*authn.AuthConfig, error) {
	if keychain == nil {
		keychain = authn.DefaultKeychain
	}
	res, err := name.NewRegistry(registry)
	if err != nil {
		return nil, err
	}
	cfg, err := keychain.Resolve(res)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, fmt.Errorf("no credentials for %s", registry)
	}
	auth, err := cfg.Authorization()
	if err != nil {
		return nil, err
	}
	if auth == nil {
		return nil, fmt.Errorf("no credentials for %s", registry)
	}
	// IdentityToken (registry token login) maps to the password slot.
	if auth.IdentityToken != "" && auth.Password == "" {
		auth.Password = auth.IdentityToken
	}
	if auth.Username == "" && auth.Password == "" {
		return nil, fmt.Errorf("no credentials for %s", registry)
	}
	return auth, nil
}

// encodeQuery builds "k=v&k2=v2" with URL escaping.
func encodeQuery(q map[string]string) string {
	var sb strings.Builder
	first := true
	for k, v := range q {
		if !first {
			sb.WriteByte('&')
		}
		first = false
		sb.WriteString(urlEscape(k))
		sb.WriteByte('=')
		sb.WriteString(urlEscape(v))
	}
	return sb.String()
}

func urlEscape(s string) string {
	const hex = "0123456789ABCDEF"
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('%')
			sb.WriteByte(hex[c>>4])
			sb.WriteByte(hex[c&0xf])
		}
	}
	return sb.String()
}

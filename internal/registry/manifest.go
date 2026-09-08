package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// hexDigest returns the sha256 hex of b.
func hexDigest(b []byte) (string, error) {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// Media types imgpull understands.
const (
	MTDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"
	MTDockerList     = "application/vnd.docker.distribution.manifest.list.v2+json"
	MTDockerConfig   = "application/vnd.docker.container.image.v1+json"
	MTDockerLayer    = "application/vnd.docker.image.rootfs.diff.tar.gzip"
	MTOCIManifest    = "application/vnd.oci.image.manifest.v1+json"
	MTOCIIndex       = "application/vnd.oci.image.index.v1+json"
	MTOCIConfig      = "application/vnd.oci.image.config.v1+json"
	MTOCILayer       = "application/vnd.oci.image.layer.v1.tar+gzip"
)

// IsIndexMediaType reports whether mt is an image index / manifest list.
func IsIndexMediaType(mt string) bool {
	switch types.MediaType(mt) {
	case types.OCIImageIndex, types.DockerManifestList:
		return true
	}
	return false
}

// IsManifestMediaType reports whether mt is a single image manifest.
func IsManifestMediaType(mt string) bool {
	switch types.MediaType(mt) {
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		return true
	}
	return false
}

// BlobDescriptor describes one blob referenced by a manifest.
type BlobDescriptor struct {
	Digest    string
	Size      int64
	MediaType string
	URLs      []string // alternate download URLs (from manifest "urls")
}

// ImageManifest is the resolved single-platform manifest plus its blobs.
type ImageManifest struct {
	Digest       string
	Size         int64
	MediaType    string
	Raw          []byte
	Config       BlobDescriptor
	Layers       []BlobDescriptor
	Platform     *Platform
	Architecture string
}

// ManifestFetchOptions tunes manifest fetching robustness. Zero fields fall
// back to the defaults (3 attempts, 60s per attempt). Log may be nil.
type ManifestFetchOptions struct {
	Attempts      int
	PerTryTimeout time.Duration
	// Proxy routes manifest requests through this proxy; nil = direct.
	Proxy *url.URL
	// Log receives retry warnings; may be nil.
	Log func(format string, args ...any)
}

// ResolveManifest fetches the top-level manifest for ref (following index →
// platform selection), returning the single-platform image manifest. Uses the
// default fetch policy: per-attempt timeout and retries on transient errors.
func ResolveManifest(ctx context.Context, ref name.Reference, plat *Platform,
	keychain authn.Keychain) (*ImageManifest, error) {
	return ResolveManifestOpts(ctx, ref, plat, keychain, ManifestFetchOptions{})
}

// ResolveManifestOpts is ResolveManifest with an explicit fetch policy. Each
// attempt walks the whole index→manifest chain under a per-attempt deadline;
// timeouts, network errors and 5xx/429 are retried with a short backoff.
func ResolveManifestOpts(ctx context.Context, ref name.Reference, plat *Platform,
	keychain authn.Keychain, opts ManifestFetchOptions) (*ImageManifest, error) {
	attempts := opts.Attempts
	if attempts <= 0 {
		attempts = 3
	}
	timeout := opts.PerTryTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	logf := opts.Log

	var lastErr error
	attempt := 0
	for attempt = 1; ; attempt++ {
		tryCtx, cancel := context.WithTimeout(ctx, timeout)
		m, err := resolveManifestOnce(tryCtx, ref, plat, keychain, opts)
		cancel()
		if err == nil {
			return m, nil
		}
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			// User abort (or run-level shutdown): never retry.
			return nil, err
		}
		lastErr = err
		if attempt >= attempts || !retryableFetchErr(err) {
			break
		}
		backoff := time.Duration(attempt) * time.Second
		if logf != nil {
			logf("manifest fetch failed (%v); retrying in %s (attempt %d/%d)", err, backoff, attempt+1, attempts)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	if isTimeout(lastErr) {
		return nil, fmt.Errorf("manifest fetch failed after %d attempt(s): %w — the registry/mirror is not responding, check the mirror service",
			attempt, lastErr)
	}
	return nil, fmt.Errorf("manifest fetch failed after %d attempt(s): %w", attempt, lastErr)
}

// retryableFetchErr classifies manifest-fetch failures: HTTP 5xx/429, any
// transport-level error (conn refused, TLS, timeout) and deadlines are worth
// another attempt; 4xx statuses are definitive.
func retryableFetchErr(err error) bool {
	var terr *transport.Error
	if errors.As(err, &terr) {
		return terr.StatusCode >= 500 || terr.StatusCode == http.StatusTooManyRequests
	}
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var uerr *url.Error
	return errors.As(err, &uerr) && uerr.Timeout()
}

func resolveManifestOnce(ctx context.Context, ref name.Reference, plat *Platform,
	keychain authn.Keychain, fetch ManifestFetchOptions) (*ImageManifest, error) {
	const maxDepth = 4
	opts := []remote.Option{
		remote.WithContext(ctx),
		// ggcr's internal retry would eat the per-attempt deadline silently;
		// the caller-level policy in ResolveManifestOpts owns retries (and
		// logs them).
		remote.WithRetryPredicate(func(error) bool { return false }),
	}
	if fetch.Proxy != nil {
		opts = append(opts, remote.WithTransport(ProxyTransport(fetch.Proxy)))
	}
	if keychain != nil {
		opts = append(opts, remote.WithAuthFromKeychain(keychain))
	}
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}
	for depth := 0; ; depth++ {
		mt := string(desc.MediaType)
		switch {
		case IsIndexMediaType(mt):
			if depth >= maxDepth {
				return nil, fmt.Errorf("index nesting too deep at %s", desc.Digest)
			}
			raw, err := desc.RawManifest()
			if err != nil {
				return nil, fmt.Errorf("read index %s: %w", desc.Digest, err)
			}
			var idx v1.IndexManifest
			if err := json.Unmarshal(raw, &idx); err != nil {
				return nil, fmt.Errorf("parse index %s: %w", desc.Digest, err)
			}
			child, err := SelectPlatform(idx.Manifests, plat)
			if err != nil {
				return nil, err
			}
			childRef := ref.Context().Digest(child.Digest.String())
			desc, err = remote.Get(childRef, opts...)
			if err != nil {
				return nil, fmt.Errorf("fetch child manifest %s: %w", child.Digest, err)
			}
			continue

		case IsManifestMediaType(mt):
			return manifestFromDescriptor(desc, plat)

		default:
			// Some registries serve manifests with empty/generic media
			// types; sniff the JSON to decide index vs manifest.
			raw, err := desc.RawManifest()
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", desc.Digest, err)
			}
			var probe struct {
				Manifests *json.RawMessage `json:"manifests"`
				Config    *json.RawMessage `json:"config"`
			}
			if json.Unmarshal(raw, &probe) == nil {
				if probe.Manifests != nil {
					return nil, fmt.Errorf("content %s looks like an index but has media type %q",
						desc.Digest, mt)
				}
				if probe.Config != nil {
					return manifestFromDescriptor(desc, plat)
				}
			}
			return nil, fmt.Errorf("unsupported media type %q for %s", mt, desc.Digest)
		}
	}
}

func manifestFromDescriptor(desc *remote.Descriptor, plat *Platform) (*ImageManifest, error) {
	raw, err := desc.RawManifest()
	if err != nil {
		return nil, err
	}
	// Verify the manifest bytes hash to the descriptor digest.
	if got, err := hexDigest(raw); err != nil {
		return nil, err
	} else if got != desc.Digest.Hex {
		return nil, fmt.Errorf("manifest digest mismatch: got %s, want %s", got, desc.Digest.Hex)
	}
	var m struct {
		Config struct {
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
			MediaType string `json:"mediaType"`
		} `json:"config"`
		Layers []struct {
			Digest    string   `json:"digest"`
			Size      int64    `json:"size"`
			MediaType string   `json:"mediaType"`
			URLs      []string `json:"urls"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Config.Digest == "" {
		return nil, fmt.Errorf("manifest %s has no config digest", desc.Digest)
	}
	out := &ImageManifest{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
		Raw:       raw,
		Platform:  plat,
		Config: BlobDescriptor{
			Digest:    m.Config.Digest,
			Size:      m.Config.Size,
			MediaType: m.Config.MediaType,
		},
	}
	for _, l := range m.Layers {
		if l.Digest == "" {
			return nil, fmt.Errorf("manifest %s has a layer without digest", desc.Digest)
		}
		out.Layers = append(out.Layers, BlobDescriptor{
			Digest:    l.Digest,
			Size:      l.Size,
			MediaType: l.MediaType,
			URLs:      l.URLs,
		})
	}
	return out, nil
}

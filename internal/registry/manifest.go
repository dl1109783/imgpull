package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
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

// ResolveManifest fetches the top-level manifest for ref (following index →
// platform selection), returning the single-platform image manifest. Handles
// nested indexes and unknown content types.
func ResolveManifest(ctx context.Context, ref name.Reference, plat *Platform,
	keychain authn.Keychain) (*ImageManifest, error) {
	const maxDepth = 4
	opts := []remote.Option{remote.WithContext(ctx)}
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

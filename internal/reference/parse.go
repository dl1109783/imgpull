// Package reference parses user-supplied image references and derives
// filesystem-friendly identifiers for the download directory layout.
package reference

import (
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
)

// Ref is a parsed image reference.
type Ref struct {
	Input     string
	Reference name.Reference

	// Registry is the display form of the registry host, e.g. "docker.io"
	// instead of the internal "index.docker.io".
	Registry string
	// Repository is the normalized repository path, e.g. "library/nginx".
	Repository string
	// Tag is the tag name, "latest" included by default; empty for digest refs.
	Tag string
	// Digest is the digest ("sha256:..."); empty for tag refs.
	Digest string
}

// Parse parses an image reference such as "docker.io/library/nginx:latest",
// "nginx", or "quay.io/org/app@sha256:...".
func Parse(input string, insecure bool) (*Ref, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty image reference")
	}
	var opts []name.Option
	if insecure {
		opts = append(opts, name.Insecure)
	}
	ref, err := name.ParseReference(input, opts...)
	if err != nil {
		return nil, fmt.Errorf("parse image reference %q: %w", input, err)
	}
	out := &Ref{Input: input, Reference: ref}
	reg := ref.Context().Registry.RegistryStr()
	if reg == name.DefaultRegistry {
		reg = "docker.io"
	}
	out.Registry = reg
	out.Repository = ref.Context().RepositoryStr()
	switch t := ref.(type) {
	case name.Tag:
		out.Tag = t.TagStr()
	case name.Digest:
		out.Digest = t.Identifier()
	}
	return out, nil
}

// RefDir is the per-image directory name. The tag is only used to organize
// files on disk, never as a cache identity: state.json tracks the manifest
// digest, which is the real image identity.
func (r *Ref) RefDir() string {
	if r.Tag != "" {
		return r.Tag
	}
	return "digest-" + ShortDigest(r.Digest)
}

// ShortDigest returns the first few hex characters of "algo:hex" for display.
func ShortDigest(digest string) string {
	hex := digest
	if i := strings.Index(hex, ":"); i >= 0 {
		hex = hex[i+1:]
	}
	if len(hex) > 12 {
		hex = hex[:12]
	}
	return hex
}

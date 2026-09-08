// Package registry implements the Registry V2 protocol pieces imgpull needs:
// platform selection, bearer/basic authentication for aria2, blob URL
// resolution (direct or signed-CDN), and manifest resolution.
package registry

import (
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// Platform identifies a target OS/architecture/variant.
type Platform struct {
	OS           string
	Architecture string
	Variant      string
}

// ParsePlatform parses "linux/amd64", "linux/arm64/v8", or "amd64"
// (arch alone implies linux).
func ParsePlatform(s string) (*Platform, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty platform")
	}
	parts := strings.Split(s, "/")
	if len(parts) > 3 {
		return nil, fmt.Errorf("invalid platform %q: expected os/arch[/variant]", s)
	}
	p := &Platform{OS: parts[0], Architecture: parts[0]}
	if len(parts) == 1 {
		p.OS = "linux"
		return p, nil
	}
	p.Architecture = parts[1]
	if len(parts) == 3 {
		p.Variant = parts[2]
	}
	return p, nil
}

func (p *Platform) String() string {
	if p.Variant != "" {
		return p.OS + "/" + p.Architecture + "/" + p.Variant
	}
	return p.OS + "/" + p.Architecture
}

// Matches reports whether the descriptor's platform matches p, ignoring
// an empty variant on either side.
func (p *Platform) Matches(desc v1.Descriptor) bool {
	dp := desc.Platform
	if dp == nil {
		return false
	}
	if dp.OS != p.OS || dp.Architecture != p.Architecture {
		return false
	}
	// amd64 variant is conventionally omitted; treat empty==v1.
	dv, pv := dp.Variant, p.Variant
	if dv == "" && dp.Architecture == "amd64" {
		dv = "v1"
	}
	if pv == "" && p.Architecture == "amd64" {
		pv = "v1"
	}
	return dv == pv || dv == "" || pv == ""
}

// attestation media types / unknown platforms must be skipped during
// platform selection (buildkit provenance/sbom refs).
func isAttestation(desc v1.Descriptor) bool {
	if desc.Platform != nil &&
		(desc.Platform.OS == "unknown" || desc.Platform.Architecture == "unknown") {
		return true
	}
	if desc.MediaType == types.OCIImageIndex || desc.MediaType == types.DockerManifestList {
		// nested index: could be attestation manifest; only treat as
		// attestation if platform says unknown (handled above).
		return false
	}
	return false
}

// SelectPlatform picks the manifest descriptor for p from an index.
func SelectPlatform(manifests []v1.Descriptor, p *Platform) (*v1.Descriptor, error) {
	for i := range manifests {
		desc := manifests[i]
		if isAttestation(desc) {
			continue
		}
		if p.Matches(desc) {
			return &manifests[i], nil
		}
	}
	var avail []string
	for _, desc := range manifests {
		if isAttestation(desc) {
			continue
		}
		if desc.Platform != nil {
			avail = append(avail, desc.Platform.OS+"/"+desc.Platform.Architecture+
				map[bool]string{true: "/" + desc.Platform.Variant, false: ""}[desc.Platform.Variant != ""])
		} else {
			avail = append(avail, string(desc.MediaType))
		}
	}
	return nil, fmt.Errorf("platform %s not found in index; available: %s",
		p, strings.Join(avail, ", "))
}

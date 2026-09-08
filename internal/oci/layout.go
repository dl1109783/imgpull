// Package oci assembles the final OCI image layout (oci-layout, index.json)
// once all blobs are downloaded and verified.
package oci

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-containerregistry/pkg/v1"

	"imgpull/internal/fsx"
	"imgpull/internal/registry"
)

// EnsureLayout writes the oci-layout marker file if missing.
func EnsureLayout(root string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	marker := filepath.Join(root, "oci-layout")
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	data, _ := json.MarshalIndent(map[string]string{"imageLayoutVersion": "1.0.0"}, "", "  ")
	return fsx.WriteFileAtomic(marker, append(data, '\n'), 0o644)
}

// indexEntry is one manifest reference inside index.json.
type indexEntry struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Platform    *v1.Platform      `json:"platform,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type imageIndex struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Manifests     []indexEntry `json:"manifests"`
}

// WriteIndex creates index.json pointing at the downloaded single-platform
// manifest, tagged with the original reference via annotations.
func WriteIndex(root string, mfst *registry.ImageManifest, plat *registry.Platform, refName string) error {
	if err := EnsureLayout(root); err != nil {
		return err
	}
	entry := indexEntry{
		MediaType: mfst.MediaType,
		Digest:    mfst.Digest,
		Size:      int64(len(mfst.Raw)),
		Platform: &v1.Platform{
			OS:           plat.OS,
			Architecture: plat.Architecture,
			Variant:      plat.Variant,
		},
		Annotations: map[string]string{
			"org.opencontainers.image.ref.name": refName,
		},
	}
	idx := imageIndex{
		SchemaVersion: 2,
		MediaType:     registry.MTOCIIndex,
		Manifests:     []indexEntry{entry},
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return fsx.WriteFileAtomic(filepath.Join(root, "index.json"), data, 0o644)
}

// IndexTime returns file mtime (unused hook for tests).
func IndexTime(path string) (time.Time, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("stat %s: %w", path, err)
	}
	return fi.ModTime(), nil
}

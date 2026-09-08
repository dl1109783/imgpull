// Package export converts a downloaded OCI layout into other formats; the
// first supported target is the docker save (docker-archive) format.
package export

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// DockerArchive converts the manifest identified by digest inside layoutDir
// into a docker-archive file at outPath, tagged with tag.
func DockerArchive(layoutDir string, digest string, tag name.Tag, outPath string) error {
	path, err := layout.FromPath(layoutDir)
	if err != nil {
		return fmt.Errorf("open layout: %w", err)
	}
	idx, err := path.ImageIndex()
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	h, err := v1.NewHash(digest)
	if err != nil {
		return fmt.Errorf("digest %s: %w", digest, err)
	}
	img, err := idx.Image(h)
	if err != nil {
		return fmt.Errorf("find image %s in layout: %w", digest, err)
	}
	if err := tarball.WriteToFile(outPath, tag, img); err != nil {
		return fmt.Errorf("write docker archive: %w", err)
	}
	return nil
}

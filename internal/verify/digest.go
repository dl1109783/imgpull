// Package verify checks downloaded blobs against their digests and sizes.
package verify

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/google/go-containerregistry/pkg/v1"
)

// FileDigest streams the file at path and returns the hex digest using the
// given algorithm ("sha256" or "sha512"). The raw (still compressed) bytes
// are hashed — never the decompressed layer content.
func FileDigest(path, algorithm string) (string, error) {
	var newHash func() hash.Hash
	switch algorithm {
	case "sha256":
		newHash = sha256.New
	case "sha512":
		newHash = sha512.New
	default:
		return "", fmt.Errorf("unsupported digest algorithm %q", algorithm)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := newHash()
	buf := make([]byte, 1<<20)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Blob verifies that the file at path matches digest and size exactly.
func Blob(path string, digest v1.Hash, size int64) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Size() != size {
		return fmt.Errorf("size mismatch: got %d bytes, want %d", fi.Size(), size)
	}
	sum, err := FileDigest(path, digest.Algorithm)
	if err != nil {
		return err
	}
	if got := (v1.Hash{Algorithm: digest.Algorithm, Hex: sum}); got != digest {
		return fmt.Errorf("digest mismatch: got %s, want %s", got, digest)
	}
	return nil
}

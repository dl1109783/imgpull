package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1"
)

func TestFileDigestAndBlob(t *testing.T) {
	data := []byte("hello imgpull")
	sum := sha256.Sum256(data)
	hexSum := hex.EncodeToString(sum[:])

	path := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := FileDigest(path, "sha256")
	if err != nil {
		t.Fatal(err)
	}
	if got != hexSum {
		t.Errorf("digest = %s, want %s", got, hexSum)
	}
	h := v1.Hash{Algorithm: "sha256", Hex: hexSum}
	if err := Blob(path, h, int64(len(data))); err != nil {
		t.Errorf("Blob should pass: %v", err)
	}
	if err := Blob(path, h, int64(len(data))+1); err == nil {
		t.Error("size mismatch should fail")
	}
	bad := v1.Hash{Algorithm: "sha256", Hex: "0000000000000000000000000000000000000000000000000000000000000000"}
	err = Blob(path, bad, int64(len(data)))
	if err == nil || len(err.Error()) == 0 {
		t.Error("digest mismatch should fail")
	}
}

func TestFileDigestUnsupported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	os.WriteFile(path, []byte("x"), 0o644)
	if _, err := FileDigest(path, "md5"); err == nil {
		t.Error("md5 should be rejected")
	}
}

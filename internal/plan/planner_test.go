package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1"

	"imgpull/internal/reference"
	"imgpull/internal/registry"
)

// mkManifest fabricates a single-platform manifest with one config and two
// layers whose contents are deterministic.
func mkManifest(t *testing.T) (*reference.Ref, *registry.ImageManifest, map[string][]byte) {
	t.Helper()
	contents := map[string][]byte{}
	mk := func(data []byte) string {
		sum := sha256.Sum256(data)
		h := v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
		contents[h.String()] = data
		return h.String()
	}
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	cfgDigest := mk(config)
	layer1 := []byte("layer-one-data")
	layer2 := []byte("layer-two-data")
	l1 := mk(layer1)
	l2 := mk(layer2)

	raw := fmt.Sprintf(`{
	  "schemaVersion":2,
	  "mediaType":"application/vnd.oci.image.manifest.v1+json",
	  "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},
	  "layers":[
	    {"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":%d},
	    {"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":%d}
	  ]
	}`, cfgDigest, len(config), l1, len(layer1), l2, len(layer2))
	manifestDigest := mk([]byte(raw))

	ref, err := reference.Parse("registry.local:5000/org/app:dev", true)
	if err != nil {
		t.Fatal(err)
	}
	mfst := &registry.ImageManifest{
		Digest:    manifestDigest,
		Size:      int64(len(raw)),
		MediaType: registry.MTOCIManifest,
		Raw:       []byte(raw),
		Config: registry.BlobDescriptor{
			Digest: cfgDigest, Size: int64(len(config)), MediaType: registry.MTOCIConfig,
		},
		Layers: []registry.BlobDescriptor{
			{Digest: l1, Size: int64(len(layer1)), MediaType: registry.MTOCILayer},
			{Digest: l2, Size: int64(len(layer2)), MediaType: registry.MTOCILayer},
		},
		Platform: &registry.Platform{OS: "linux", Architecture: "amd64"},
	}
	return ref, mfst, contents
}

func TestBuildAndResume(t *testing.T) {
	root := t.TempDir()
	ref, mfst, contents := mkManifest(t)

	p1, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p1.PendingObjects()); got != 3 {
		t.Fatalf("pending = %d, want 3 (config+2 layers)", got)
	}
	// Manifest blob + manifest.json exist immediately.
	if _, err := os.Stat(filepath.Join(p1.ImageDir, "manifest.json")); err != nil {
		t.Error("manifest.json missing")
	}
	// Layout matches spec: <root>/<registry>/<repo>/<tag>/
	want := filepath.Join(root, "registry.local:5000", "org", "app", "dev")
	if p1.ImageDir != want {
		t.Errorf("ImageDir = %q, want %q", p1.ImageDir, want)
	}
	p1.Release()

	// A complete .part is verified and committed on resume.
	p2, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	layer := p2.Objects[1] // config, layer1, layer2 order
	part := p2.PartPath(layer)
	if err := os.WriteFile(part, contents[layer.Digest], 0o644); err != nil {
		t.Fatal(err)
	}
	p2.Release()
	p3, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ob := range p3.Objects {
		if ob.Digest == layer.Digest && ob.Status != StatusCommitted {
			t.Errorf("complete .part should be committed, status=%s", ob.Status)
		}
	}

	// A corrupt committed blob is removed under VerifyExisting.
	final := p3.FinalPath(layer)
	p3.Release()
	if err := os.WriteFile(final, []byte("corrupted data"), 0o644); err != nil {
		t.Fatal(err)
	}
	p4, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{VerifyExisting: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p4.Release()
	for _, ob := range p4.Objects {
		if ob.Digest == layer.Digest && ob.Status == StatusCommitted {
			t.Error("corrupt blob must not stay committed under --verify")
		}
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Error("corrupt blob should have been removed")
	}
}

func TestRangeUnsupportedPersists(t *testing.T) {
	root := t.TempDir()
	ref, mfst, _ := mkManifest(t)

	p1, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	layer := p1.Objects[1]
	p1.MarkRangeUnsupported(layer)
	p1.Release()

	// The flag must survive the state round-trip into a rebuilt plan.
	p2, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Release()
	for _, ob := range p2.Objects {
		if ob.Digest == layer.Digest && !ob.RangeUnsupported {
			t.Error("RangeUnsupported flag was not adopted from state.json")
		}
	}
}

func TestDifferentManifestReconciles(t *testing.T) {
	root := t.TempDir()
	ref, mfst, _ := mkManifest(t)
	p1, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p1.Release()
	// Same tag now points at a different manifest digest: plan must rebuild
	// the task list without inheriting committed statuses.
	mfst2 := *mfst
	sum := sha256.Sum256([]byte("other-manifest"))
	mfst2.Digest = "sha256:" + hex.EncodeToString(sum[:])
	p2, err := Build(ref, &mfst2, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Release()
	if got := len(p2.PendingObjects()); got != 3 {
		t.Errorf("pending after manifest change = %d, want 3", got)
	}
}

func TestLockedImageDir(t *testing.T) {
	root := t.TempDir()
	ref, mfst, _ := mkManifest(t)
	p1, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{}); err == nil {
		t.Error("second concurrent Build must fail on the dir lock")
	}
	p1.Release()
	// After release, the lock is reacquirable.
	p2, err := Build(ref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatalf("relock after release: %v", err)
	}
	p2.Release()
}

func TestDigestRefDir(t *testing.T) {
	root := t.TempDir()
	_, mfst, _ := mkManifest(t)
	dref, err := reference.Parse("registry.local:5000/org/app@"+mfst.Digest, true)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Build(dref, mfst, mfst.Platform, root, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	if p.Reference != "digest-"+mfst.Digest[7:19] {
		t.Errorf("digest ref dir = %q", p.Reference)
	}
	if p.ExportTag == "" {
		t.Error("ExportTag should be set for digest refs")
	}
}

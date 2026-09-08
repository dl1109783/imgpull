package registry

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/v1"
)

func TestParsePlatform(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"linux/amd64", "linux/amd64"},
		{"amd64", "linux/amd64"},
		{"linux/arm64/v8", "linux/arm64/v8"},
		{"windows/amd64", "windows/amd64"},
	}
	for _, c := range cases {
		p, err := ParsePlatform(c.in)
		if err != nil {
			t.Fatalf("ParsePlatform(%q): %v", c.in, err)
		}
		if p.String() != c.want {
			t.Errorf("ParsePlatform(%q) = %q, want %q", c.in, p.String(), c.want)
		}
	}
	if _, err := ParsePlatform(""); err == nil {
		t.Error("empty platform should fail")
	}
	if _, err := ParsePlatform("a/b/c/d"); err == nil {
		t.Error("too many parts should fail")
	}
}

func mkDesc(os, arch, variant string) v1.Descriptor {
	d := v1.Descriptor{Digest: v1.Hash{Algorithm: "sha256", Hex: "aa"}}
	if os != "" {
		d.Platform = &v1.Platform{OS: os, Architecture: arch, Variant: variant}
	}
	return d
}

func TestSelectPlatform(t *testing.T) {
	manifests := []v1.Descriptor{
		mkDesc("linux", "amd64", ""),
		mkDesc("linux", "arm64", "v8"),
		mkDesc("unknown", "unknown", ""),
	}
	p, _ := ParsePlatform("linux/amd64")
	d, err := SelectPlatform(manifests, p)
	if err != nil {
		t.Fatal(err)
	}
	if d.Platform.Architecture != "amd64" {
		t.Errorf("picked %s", d.Platform.Architecture)
	}
	// arm64 without explicit variant must not match amd64 request.
	p2, _ := ParsePlatform("linux/arm64")
	d2, err := SelectPlatform(manifests, p2)
	if err != nil || d2.Platform.Architecture != "arm64" {
		t.Fatalf("arm64 select: %v", err)
	}
	// amd64 request matches descriptor without variant (variant tolerance).
	p3, _ := ParsePlatform("linux/amd64/v1")
	if d3, err := SelectPlatform(manifests, p3); err != nil || d3.Platform.Architecture != "amd64" {
		t.Fatalf("amd64/v1 select: %v", err)
	}
	// Missing platform lists available ones.
	p4, _ := ParsePlatform("darwin/arm64")
	_, err = SelectPlatform(manifests, p4)
	if err == nil || !contains(err.Error(), "linux/amd64") {
		t.Errorf("expected available-platforms error, got %v", err)
	}
	// Attestation (unknown/unknown) is skipped, never selected.
	p5, _ := ParsePlatform("unknown/unknown")
	if _, err := SelectPlatform(manifests, p5); err == nil {
		t.Error("attestation platform should not be selectable")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

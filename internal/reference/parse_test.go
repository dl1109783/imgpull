package reference

import "testing"

func TestParseTag(t *testing.T) {
	r, err := Parse("nginx", false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Registry != "docker.io" {
		t.Errorf("registry = %q, want docker.io", r.Registry)
	}
	if r.Repository != "library/nginx" {
		t.Errorf("repository = %q, want library/nginx", r.Repository)
	}
	if r.Tag != "latest" {
		t.Errorf("tag = %q, want latest", r.Tag)
	}
	if r.RefDir() != "latest" {
		t.Errorf("RefDir = %q, want latest", r.RefDir())
	}
}

func TestParseFull(t *testing.T) {
	r, err := Parse("quay.io/org/app:1.2.3", false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Registry != "quay.io" || r.Repository != "org/app" || r.Tag != "1.2.3" {
		t.Fatalf("got %s / %s / %s", r.Registry, r.Repository, r.Tag)
	}
}

func TestParseDigest(t *testing.T) {
	const hex64 = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	r, err := Parse("registry.example.com/org/app@sha256:"+hex64, false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Digest != "sha256:"+hex64 {
		t.Errorf("digest = %q", r.Digest)
	}
	want := "digest-abcdef012345"
	if got := r.RefDir(); got != want {
		t.Errorf("RefDir = %q, want %q", got, want)
	}
}

func TestParseInvalid(t *testing.T) {
	for _, in := range []string{"", "   ", "UPPERCASE/bad:tag"} {
		if _, err := Parse(in, false); err == nil {
			t.Errorf("Parse(%q) should fail", in)
		}
	}
}

func TestShortDigest(t *testing.T) {
	if got := ShortDigest("sha256:0123456789abcdef"); got != "0123456789ab" {
		t.Errorf("ShortDigest = %q", got)
	}
}

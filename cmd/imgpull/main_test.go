package main

import (
	"flag"
	"fmt"
	"reflect"
	"testing"
)

func TestExpandShortFlags(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"help and version", []string{"-h"}, []string{"--help"}},
		{"version", []string{"-v"}, []string{"--version"}},
		{
			"separate values",
			[]string{"nginx:1.2", "-p", "linux/arm64", "-o", "./n", "-c", "3"},
			[]string{"nginx:1.2", "--platform", "linux/arm64", "--output", "./n", "--concurrency", "3"},
		},
		{
			"attached and = values",
			[]string{"-c3", "-odir", "-p=linux/arm64"},
			[]string{"--concurrency", "3", "--output", "dir", "--platform", "linux/arm64"},
		},
		{
			"boolean cluster",
			[]string{"-kV"},
			[]string{"--insecure", "--verify"},
		},
		{
			"value flag inside cluster",
			[]string{"-kc2"},
			[]string{"--insecure", "--concurrency", "2"},
		},
		{"long flags untouched", []string{"--platform", "linux/arm64"}, []string{"--platform", "linux/arm64"}},
		{"terminator untouched", []string{"--", "-o", "x"}, []string{"--", "-o", "x"}},
		{"bare dash untouched", []string{"-"}, []string{"-"}},
		{"unknown short passthrough", []string{"-z"}, []string{"-z"}},
		{"unknown cluster tail", []string{"-kz"}, []string{"--insecure", "-z"}},
		{"positional untouched", []string{"docker.io/library/nginx:latest"}, []string{"docker.io/library/nginx:latest"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expandShortFlags(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("expandShortFlags(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestShortEndToEndArgs checks the full argv path: expansion + positional
// splitting + flag parsing yield the same config as the long forms.
func TestShortEndToEndArgs(t *testing.T) {
	short := runFlags(t, []string{"nginx:1.2", "-p", "linux/arm64", "-o", "/tmp/x", "-c2", "-kV"})
	long := runFlags(t, []string{"nginx:1.2", "--platform", "linux/arm64", "--output", "/tmp/x",
		"--concurrency", "2", "--insecure", "--verify"})
	if short != long {
		t.Fatalf("short form %v != long form %v", short, long)
	}
	if short != "linux/arm64 /tmp/x 2 true true nginx:1.2" {
		t.Fatalf("unexpected parsed values: %q", short)
	}
}

// runFlags parses argv the same way run() does and returns the values that
// would drive the pull, joined with spaces.
func runFlags(t *testing.T, args []string) string {
	t.Helper()
	positional, rest := splitPositional(expandShortFlags(args))
	fs := flag.NewFlagSet("imgpull", flag.ContinueOnError)
	platformFlag := fs.String("platform", "linux/amd64", "")
	output := fs.String("output", ".", "")
	concurrency := fs.Int("concurrency", 3, "")
	insecure := fs.Bool("insecure", false, "")
	verify := fs.Bool("verify", false, "")
	if err := fs.Parse(rest); err != nil {
		t.Fatalf("parse: %v", err)
	}
	image := ""
	if len(positional) > 0 {
		image = positional[0]
	}
	return fmt.Sprintf("%s %s %d %t %t %s",
		*platformFlag, *output, *concurrency, *insecure, *verify, image)
}

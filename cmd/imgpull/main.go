// imgpull is a resumable Docker image downloader. It resolves the image
// reference with go-containerregistry and pulls every blob (config, layers)
// through aria2 with per-layer resume, then verifies SHA-256 and assembles
// an OCI image layout.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"imgpull/internal/downloader"
	"imgpull/internal/export"
	"imgpull/internal/fsx"
	"imgpull/internal/oci"
	"imgpull/internal/plan"
	"imgpull/internal/reference"
	"imgpull/internal/registry"
)

const imgpullVersion = "0.2.4"

// valueFlags are options whose value may be the next argv element.
var valueFlags = map[string]bool{
	"platform":     true,
	"output":       true,
	"aria2-rpc":    true,
	"aria2-secret": true,
	"concurrency":  true,
	"max-retries":  true,
	"export":       true,
	"username":     true,
	"password":     true,
	"proxy":        true,
}

// shortFlags maps single-letter aliases to their long flag names. Letter
// case is significant (-v version vs -V verify).
var shortFlags = map[string]string{
	"h": "help",
	"v": "version",
	"p": "platform",
	"o": "output",
	"r": "aria2-rpc",
	"s": "aria2-secret",
	"c": "concurrency",
	"R": "max-retries",
	"e": "export",
	"u": "username",
	"P": "password",
	"k": "insecure",
	"V": "verify",
	"x": "proxy",
}

// splitPositional extracts the image reference (first non-flag argument) so
// flags may be interspersed: imgpull <ref> --platform x --output y.
func splitPositional(args []string) (positional []string, rest []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			rest = append(rest, args[i:]...)
			return
		case strings.HasPrefix(a, "-") && a != "-":
			name := strings.TrimLeft(a, "-")
			rest = append(rest, a)
			if valueFlags[name] && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				rest = append(rest, args[i])
			}
		default:
			positional = append(positional, a)
		}
	}
	return
}

// expandShortFlags rewrites single-dash short flags to their long form so
// splitPositional/flag only ever see long names. Supported forms:
// -p linux/amd64, -p=linux/amd64, -plinux/amd64, boolean clusters (-kV),
// and -p=v for value flags. Unknown shorts pass through untouched so the
// flag package reports them.
func expandShortFlags(args []string) []string {
	out := make([]string, 0, len(args))
	terminator := false
	for _, a := range args {
		if a == "--" {
			terminator = true
		}
		if terminator || len(a) < 2 || a[0] != '-' || a[1] == '-' {
			out = append(out, a)
			continue
		}
		body := a[1:]
		for len(body) > 0 {
			long, ok := shortFlags[string(body[0])]
			if !ok {
				out = append(out, "-"+body)
				break
			}
			rest := body[1:]
			if strings.HasPrefix(rest, "=") || (len(rest) > 0 && valueFlags[long]) {
				out = append(out, "--"+long, strings.TrimPrefix(rest, "="))
				break
			}
			out = append(out, "--"+long)
			body = rest
		}
	}
	return out
}

func usage() {
	fmt.Fprintf(os.Stderr, `imgpull %s — resumable image downloader (go-containerregistry + aria2)

Usage:
  imgpull <image> [flags]

Examples:
  imgpull docker.io/library/nginx:latest -p linux/amd64 -o ./nginx -c 3
  imgpull quay.io/org/app:1.2 -r http://127.0.0.1:6800/jsonrpc
  imgpull registry.local:5000/dev/foo:main -k
  imgpull docker.io/library/alpine:latest -x http://192.168.0.7:1080

Flags:
  -p, --platform OS/ARCH[/VARIANT]   target platform (default: linux/amd64)
  -o, --output DIR                   download root (default: current directory)
  -r, --aria2-rpc URL                external aria2 JSON-RPC endpoint (default: spawn own aria2c)
  -s, --aria2-secret TOKEN           RPC secret for the external daemon
  -c, --concurrency N                layers downloaded in parallel (default 3)
  -R, --max-retries N                retry attempts per layer (default 5)
  -x, --proxy URL                    proxy for registry access and layer downloads (http/https/socks5)
  -e, --export FORMAT                "docker-archive" also writes image.tar (docker load)
  -u, --username / -P, --password    registry credentials (default: read from docker login config)
  -k, --insecure                     use http:// for the registry
  -V, --verify                       re-verify digests of already-committed blobs on start
  -v, --version                      print version
  -h, --help                         this help

Short flags also accept attached values: -c3 equals -c 3, and boolean
shorts may cluster: -kV equals -k -V.

Layout: <output>/<registry>/<repo>/<tag>/ holds state.json, manifest.json,
oci-layout, index.json, blobs/sha256/<hex> and tmp/<sha256_<hex>>.part.
Interrupted downloads resume in place; the same command may simply be rerun.
`, imgpullVersion)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("imgpull: ")
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	positional, rest := splitPositional(expandShortFlags(args))
	fs := flag.NewFlagSet("imgpull", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	platformFlag := fs.String("platform", "linux/amd64", "target platform os/arch[/variant]")
	output := fs.String("output", ".", "download root directory")
	aria2RPC := fs.String("aria2-rpc", "", "external aria2 JSON-RPC endpoint")
	aria2Secret := fs.String("aria2-secret", "", "aria2 RPC secret")
	concurrency := fs.Int("concurrency", 3, "parallel layer downloads")
	maxRetries := fs.Int("max-retries", 5, "retry attempts per object")
	exportFlag := fs.String("export", "", "export format: '' or 'docker-archive'")
	username := fs.String("username", "", "registry username (default: read from docker config)")
	password := fs.String("password", "", "registry password")
	proxyFlag := fs.String("proxy", "", "proxy URL for registry + downloads (e.g. http://192.168.0.7:1080)")
	insecure := fs.Bool("insecure", false, "use http:// registry")
	verifyFlag := fs.Bool("verify", false, "re-verify committed blobs on startup")
	showVersion := fs.Bool("version", false, "print version")
	showHelp := fs.Bool("help", false, "show help")
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	switch {
	case *showVersion:
		fmt.Println("imgpull", imgpullVersion)
		return 0
	case *showHelp:
		usage()
		return 0
	}
	if len(positional) == 0 {
		usage()
		return 2
	}
	image := positional[0]
	if *exportFlag != "" && *exportFlag != "docker-archive" {
		log.Printf("unsupported --export %q (only 'docker-archive')", *exportFlag)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return pull(ctx, pullConfig{
		image:        image,
		platformFlag: *platformFlag,
		output:       *output,
		aria2RPC:     *aria2RPC,
		aria2Secret:  *aria2Secret,
		concurrency:  *concurrency,
		maxRetries:   *maxRetries,
		export:       *exportFlag,
		username:     *username,
		password:     *password,
		proxy:        *proxyFlag,
		insecure:     *insecure,
		verify:       *verifyFlag,
	})
}

type pullConfig struct {
	image        string
	platformFlag string
	output       string
	aria2RPC     string
	aria2Secret  string
	concurrency  int
	maxRetries   int
	export       string
	username     string
	password     string
	proxy        string
	insecure     bool
	verify       bool
}

// parseProxy validates the --proxy value: nil when empty, otherwise a URL
// with one of the supported schemes (http, https for both registry and
// aria2; socks5 registry-only).
func parseProxy(s string) (*url.URL, error) {
	if s == "" {
		return nil, nil
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid --proxy %q (want e.g. http://192.168.0.7:1080)", s)
	}
	switch u.Scheme {
	case "http", "https", "socks5":
		return u, nil
	default:
		return nil, fmt.Errorf("unsupported --proxy scheme %q (use http, https or socks5)", u.Scheme)
	}
}

func pull(ctx context.Context, cfg pullConfig) int {
	// 0. Proxy validation.
	proxyURL, err := parseProxy(cfg.proxy)
	if err != nil {
		log.Println(err)
		return 2
	}
	if proxyURL != nil {
		log.Printf("using proxy %s", cfg.proxy)
		if proxyURL.Scheme == "socks5" {
			// Go dials SOCKS5 natively, but aria2 only understands http/https
			// proxies, so layer downloads stay direct in that case.
			log.Printf("warning: aria2 does not support SOCKS proxies — %s applies to registry access only, layers download directly", cfg.proxy)
		}
	}

	// 1. Reference + platform.
	ref, err := reference.Parse(cfg.image, cfg.insecure)
	if err != nil {
		log.Println(err)
		return 2
	}
	pf := cfg.platformFlag
	if pf == "" {
		pf = "linux/amd64"
	}
	plat, err := registry.ParsePlatform(pf)
	if err != nil {
		log.Println(err)
		return 2
	}
	keychain := registry.BuildKeychain(cfg.username, cfg.password)

	// 2. Resolve the single-platform manifest via go-containerregistry.
	client := registry.NewClientProxy(cfg.insecure, proxyURL)
	log.Printf("resolving %s (%s) from %s ...", ref.Reference.Name(), plat, ref.Registry)
	mfst, err := registry.ResolveManifestOpts(ctx, ref.Reference, plat, keychain, registry.ManifestFetchOptions{
		Proxy: proxyURL,
		Log:   func(f string, a ...any) { log.Printf(f, a...) },
	})
	if err != nil {
		log.Println(err)
		return 1
	}
	var total int64 = int64(len(mfst.Raw))
	for _, l := range append([]registry.BlobDescriptor{mfst.Config}, mfst.Layers...) {
		total += l.Size
	}
	log.Printf("image %s | manifest %s | %d layers | %s total",
		mfst.Digest, strings.TrimPrefix(mfst.MediaType, "application/vnd."), len(mfst.Layers), fsx.HumanSize(total))

	// 3. Build/resume the plan (state.json + filesystem reconciliation).
	p, err := plan.Build(ref, mfst, plat, cfg.output, plan.BuildOptions{VerifyExisting: cfg.verify})
	if err != nil {
		log.Println(err)
		return 1
	}
	defer p.Release()
	log.Printf("layout: %s", p.ImageDir)

	// 4. Download what is missing (aria2 handles per-layer resume).
	if pending := p.PendingObjects(); len(pending) > 0 {
		log.Printf("to download: %d object(s), %s", len(pending), fsx.HumanSize(p.TotalBytes()))
		rpc, daemon, err := connectAria2(ctx, cfg, p)
		if err != nil {
			log.Println(err)
			return 1
		}
		if daemon != nil {
			defer daemon.Stop()
		}
		auth := registry.NewAuthenticatorProxy(ref.Reference.Context(), keychain, cfg.insecure, proxyURL)
		schedProxy := cfg.proxy
		if proxyURL != nil && proxyURL.Scheme != "http" && proxyURL.Scheme != "https" {
			schedProxy = "" // aria2 cannot use SOCKS; keep layer downloads direct
		}
		sched := downloader.NewScheduler(rpc, client, auth, ref.Reference.Context(), p,
			downloader.SchedulerOptions{Concurrency: cfg.concurrency, MaxRetries: cfg.maxRetries, Proxy: schedProxy})
		if err := sched.Run(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("interrupted — rerun the same command to resume from tmp/*.part")
			} else {
				log.Printf("download failed: %v", err)
			}
			return 1
		}
	} else {
		log.Printf("all blobs already present and verified")
	}
	if err := p.SaveFinal(); err != nil {
		log.Printf("warning: save state: %v", err)
	}

	// 5. Assemble the OCI layout (only after everything is verified).
	if err := oci.EnsureLayout(p.ImageDir); err != nil {
		log.Println(err)
		return 1
	}
	if err := oci.WriteIndex(p.ImageDir, mfst, plat, ref.RefDir()); err != nil {
		log.Println(err)
		return 1
	}

	// 6. Optional docker-archive export.
	if cfg.export == "docker-archive" {
		tag, err := p.TagRef()
		if err != nil {
			log.Println(err)
			return 1
		}
		outPath := filepath.Join(p.ImageDir, "image.tar")
		log.Printf("exporting docker-archive %s ...", outPath)
		if err := export.DockerArchive(p.ImageDir, p.ManifestDigest, tag, outPath); err != nil {
			log.Println(err)
			return 1
		}
	}

	// 7. Summary.
	log.Printf("done ✓")
	log.Printf("  image:    %s", cfg.image)
	log.Printf("  digest:   %s", p.ManifestDigest)
	log.Printf("  platform: %s", plat)
	log.Printf("  layout:   %s", p.ImageDir)
	log.Printf("  inspect:  skopeo inspect oci:%s", p.ImageDir)
	return 0
}

// connectAria2 either uses an external RPC daemon or spawns one.
func connectAria2(ctx context.Context, cfg pullConfig, p *plan.Plan) (*downloader.RPC, *downloader.Daemon, error) {
	if cfg.aria2RPC != "" {
		rpc := downloader.NewRPC(cfg.aria2RPC, cfg.aria2Secret)
		vctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		v, err := rpc.GetVersion(vctx)
		if err != nil {
			return nil, nil, fmt.Errorf("aria2 RPC %s unreachable: %w", cfg.aria2RPC, err)
		}
		log.Printf("using external aria2 %s at %s", v, cfg.aria2RPC)
		return rpc, nil, nil
	}
	daemon, err := downloader.StartDaemon(ctx, downloader.DaemonOptions{
		Dir:                 p.TmpDir,
		ConcurrentDownloads: cfg.concurrency,
	})
	if err != nil {
		return nil, nil, err
	}
	log.Printf("spawned aria2c daemon on 127.0.0.1:%d", daemon.Port)
	return daemon.RPC, daemon, nil
}

package plan

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"

	"imgpull/internal/fsx"
	"imgpull/internal/reference"
	"imgpull/internal/registry"
	"imgpull/internal/verify"
)

// Kind classifies a downloadable object.
type Kind string

const (
	KindManifest Kind = "manifest"
	KindConfig   Kind = "config"
	KindLayer    Kind = "layer"
)

// Status values for an object task (persisted in state.json).
const (
	StatusPending     = "pending"
	StatusDownloading = "downloading"
	StatusCommitted   = "committed" // verified and moved into blobs/
	StatusFailed      = "failed"
)

// ObjectTask is one downloadable blob (manifest, config, or layer).
type ObjectTask struct {
	Kind      Kind
	Digest    string // "sha256:hex"
	Size      int64
	MediaType string

	Status   string
	PartName string // tmp file name, "sha256_<hex>.part"
	GID      string // aria2 gid when known

	URL          string
	URLExpiresAt *time.Time
	AltURLs      []string

	Retries   int
	LastError string
	UpdatedAt time.Time

	// RangeUnsupported records that the source answered a resume Range
	// request with a full 200 body: the .part cannot be continued and is
	// discarded before the next attempt re-downloads from scratch.
	RangeUnsupported bool

	hash v1.Hash
}

// Hash returns the parsed digest of the object.
func (t *ObjectTask) Hash() v1.Hash { return t.hash }

// Plan owns the download plan for one image: directory layout, task list,
// state persistence, and resume reconciliation against the filesystem.
type Plan struct {
	mu sync.Mutex

	ImageDir string // <output>/<registry>/<repo>/<ref>
	TmpDir   string
	BlobsDir string

	Registry   string
	Repository string
	Reference  string // tag or digest identifier
	Image      string // original user input
	ExportTag  string // tag for docker-archive export, e.g. "docker.io/library/nginx:latest"

	Platform          *registry.Platform
	ManifestDigest    string
	ManifestMediaType string

	// ManifestRaw is the raw single-platform manifest JSON; it is stored
	// directly as a blob and as manifest.json for easy inspection.
	ManifestRaw []byte
	// ManifestHash is the parsed manifest digest.
	ManifestHash v1.Hash

	Objects []*ObjectTask

	lock *fileLock
	st   *stateFile

	lastSave time.Time
}

// BuildOptions tunes plan construction.
type BuildOptions struct {
	// VerifyExisting re-verifies digest of files already in blobs/ even when
	// state.json says they are committed.
	VerifyExisting bool
}

// Build constructs (or resumes) the plan for ref+manifest under outputRoot.
func Build(ref *reference.Ref, mfst *registry.ImageManifest, plat *registry.Platform,
	outputRoot string, opts BuildOptions) (*Plan, error) {

	imageDir := filepath.Join(outputRoot, ref.Registry, ref.Repository, ref.RefDir())
	lock, err := lockImageDir(imageDir)
	if err != nil {
		return nil, err
	}
	for _, d := range []string{imageDir, filepath.Join(imageDir, "tmp"), filepath.Join(imageDir, "blobs")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			lock.Release()
			return nil, err
		}
	}

	manifestHash, err := v1.NewHash(mfst.Digest)
	if err != nil {
		lock.Release()
		return nil, fmt.Errorf("manifest digest: %w", err)
	}

	p := &Plan{
		ImageDir: imageDir,
		TmpDir:   filepath.Join(imageDir, "tmp"),
		BlobsDir: filepath.Join(imageDir, "blobs"),

		Registry:   ref.Registry,
		Repository: ref.Repository,
		Reference:  ref.RefDir(),
		Image:      ref.Input,
		ExportTag:  exportTagFor(ref),

		Platform:          plat,
		ManifestDigest:    mfst.Digest,
		ManifestMediaType: mfst.MediaType,
		ManifestRaw:       mfst.Raw,
		ManifestHash:      manifestHash,
		lock:              lock,
	}

	prev, err := loadState(imageDir)
	if err != nil {
		log.Printf("warning: %v; starting from scratch", err)
		prev = nil
	}

	// The manifest itself is a blob: write it immediately (idempotent).
	if err := p.storeManifestBlob(mfst); err != nil {
		lock.Release()
		return nil, err
	}

	committed := map[string]*stateObject{}
	if prev != nil && prev.ManifestDigest == mfst.Digest {
		// Same image identity: adopt previous per-object state.
		for i := range prev.Objects {
			o := &prev.Objects[i]
			committed[o.Digest] = o
		}
	} else if prev != nil {
		log.Printf("state.json belongs to manifest %s, now %s; reconciling from disk",
			prev.ManifestDigest, mfst.Digest)
	}

	// Build tasks: config + layers. The manifest blob is already committed.
	add := func(kind Kind, d registry.BlobDescriptor) *ObjectTask {
		h, err := v1.NewHash(d.Digest)
		if err != nil {
			// Manifests are validated upstream; treat as fatal.
			panic(fmt.Sprintf("invalid digest %q: %v", d.Digest, err))
		}
		t := &ObjectTask{
			Kind:      kind,
			Digest:    d.Digest,
			Size:      d.Size,
			MediaType: d.MediaType,
			AltURLs:   d.URLs,
			PartName:  partName(h),
			hash:      h,
			Status:    StatusPending,
		}
		p.Objects = append(p.Objects, t)
		return t
	}
	add(KindConfig, mfst.Config)
	for _, l := range mfst.Layers {
		add(KindLayer, l)
	}

	// Reconcile each task against previous state and the filesystem.
	for _, t := range p.Objects {
		if o, ok := committed[t.Digest]; ok {
			if o.Status == StatusCommitted {
				t.Retries = o.Retries
				if !opts.VerifyExisting {
					t.Status = StatusCommitted
				}
			} else {
				// Carry over what a previous run learned about this source,
				// so the scheduler can warn up front instead of re-learning.
				t.RangeUnsupported = o.RangeUnsupported
			}
		}
		if err := p.reconcileTask(t, opts.VerifyExisting); err != nil {
			log.Printf("warning: %v", err)
		}
	}
	if err := p.SaveNow(); err != nil {
		lock.Release()
		return nil, err
	}
	return p, nil
}

// storeManifestBlob writes the manifest JSON as blobs/<algo>/<hex> (skipping
// the work when already present and matching) and mirrors it to manifest.json.
func (p *Plan) storeManifestBlob(mfst *registry.ImageManifest) error {
	final := p.finalPathFor(mfst.Digest)
	exists, err := blobExists(final, mfst.Digest, int64(len(mfst.Raw)))
	if err == nil && exists {
		return nil
	}
	if err := fsx.WriteFileAtomic(final, mfst.Raw, 0o644); err != nil {
		return err
	}
	return fsx.WriteFileAtomic(filepath.Join(p.ImageDir, "manifest.json"), mfst.Raw, 0o644)
}

// blobExists verifies path holds exactly digest/size; returns
// (false, reason) when it does not.
func blobExists(path, digest string, size int64) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		return false, err
	}
	h, err := v1.NewHash(digest)
	if err != nil {
		return false, err
	}
	if err := verify.Blob(path, h, size); err != nil {
		return false, err
	}
	return true, nil
}

// reconcileTask aligns one task with previous state and files on disk:
//   - committed final blob with matching digest → StatusCommitted
//   - leftover .part files stay for aria2 to resume
//   - stale .aria2 control file without a .part is removed
func (p *Plan) reconcileTask(t *ObjectTask, verifyExisting bool) error {
	if t.Status == StatusCommitted {
		// Trust state unless asked to re-verify; still confirm presence.
		if _, err := os.Stat(p.FinalPath(t)); err == nil {
			return nil
		}
		// Blob disappeared; fall through to re-download.
		t.Status = StatusPending
	}
	final := p.FinalPath(t)
	if ok, _ := blobExists(final, t.Digest, t.Size); ok {
		t.Status = StatusCommitted
		return nil
	}
	if verifyExisting {
		// A corrupt committed blob is deleted so it gets re-downloaded.
		if _, statErr := os.Stat(final); statErr == nil {
			os.Remove(final)
			log.Printf("removing invalid blob %s", filepath.Base(final))
		}
	}
	part := p.PartPath(t)
	if fi, err := os.Stat(part); err == nil {
		if fi.Size() == t.Size {
			// Fully downloaded part: verify and commit without aria2.
			if err := verify.Blob(part, t.hash, t.Size); err == nil {
				return p.commitFile(t, part)
			}
			// Corrupt: delete and start over.
			os.Remove(part)
			os.Remove(part + ".aria2")
			return fmt.Errorf("discarding corrupt .part for %s", t.Digest)
		}
		if fi.Size() > t.Size {
			os.Remove(part)
			os.Remove(part + ".aria2")
			return fmt.Errorf("oversized .part for %s removed", t.Digest)
		}
		// Partial .part: leave for resume.
		t.Status = StatusPending
		return nil
	}
	// Control file without part file is junk.
	os.Remove(part + ".aria2")
	return nil
}

func partName(h v1.Hash) string {
	return h.Algorithm + "_" + h.Hex + ".part"
}

func exportTagFor(ref *reference.Ref) string {
	if ref.Tag != "" {
		return ref.Registry + "/" + ref.Repository + ":" + ref.Tag
	}
	return ref.Registry + "/" + ref.Repository + ":" + "imgpull-" + reference.ShortDigest(ref.Digest)
}

// FinalPath is the committed blob path for t (blobs/<algo>/<hex>).
func (p *Plan) FinalPath(t *ObjectTask) string {
	return p.finalPathFor(t.Digest)
}

func (p *Plan) finalPathFor(digest string) string {
	h, err := v1.NewHash(digest)
	if err != nil {
		return filepath.Join(p.BlobsDir, "sha256", strings.ReplaceAll(digest, ":", "_"))
	}
	return filepath.Join(p.BlobsDir, h.Algorithm, h.Hex)
}

// PartPath is the in-progress .part path for t (tmp/<sha256_<hex>>.part).
func (p *Plan) PartPath(t *ObjectTask) string {
	return filepath.Join(p.TmpDir, t.PartName)
}

// PendingObjects returns tasks not yet committed.
func (p *Plan) PendingObjects() []*ObjectTask {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*ObjectTask
	for _, t := range p.Objects {
		if t.Status != StatusCommitted {
			out = append(out, t)
		}
	}
	return out
}

// TotalBytes sums the expected size of all uncommitted objects.
func (p *Plan) TotalBytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var n int64
	for _, t := range p.Objects {
		if t.Status != StatusCommitted {
			n += t.Size
		}
	}
	return n
}

// DoneBytes sums the expected size of committed objects.
func (p *Plan) DoneBytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var n int64
	for _, t := range p.Objects {
		if t.Status == StatusCommitted {
			n += t.Size
		}
	}
	return n
}

// SetTask updates status and download bookkeeping for t and persists.
func (p *Plan) SetTask(t *ObjectTask, status, url string, expires *time.Time, gid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Status = status
	if url != "" {
		t.URL = url
	}
	if expires != nil {
		t.URLExpiresAt = expires
	}
	if gid != "" {
		t.GID = gid
	}
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// MarkRetry records a transient failure and re-queues the task.
func (p *Plan) MarkRetry(t *ObjectTask, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Retries++
	t.LastError = errString(err)
	t.Status = StatusPending
	t.GID = ""
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// ForgetURL drops a remembered blob URL so the next attempt re-resolves it.
func (p *Plan) ForgetURL(t *ObjectTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t.URL = ""
	t.URLExpiresAt = nil
	t.GID = ""
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// PopAltURL removes and returns the first alternate URL from t. Task fields
// may only change under p.mu: persistLocked serializes them from other
// goroutines' commit paths.
func (p *Plan) PopAltURL(t *ObjectTask) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(t.AltURLs) == 0 {
		return "", false
	}
	u := t.AltURLs[0]
	t.AltURLs = t.AltURLs[1:]
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
	return u, true
}

// MarkRangeUnsupported records that the source rejected a resume Range
// request, so the .part cannot be continued: the next attempt discards it
// and re-downloads the object from scratch with a freshly resolved URL.
func (p *Plan) MarkRangeUnsupported(t *ObjectTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t.RangeUnsupported = true
	t.URL = ""
	t.URLExpiresAt = nil
	t.GID = ""
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// MarkFailed records a permanent failure.
func (p *Plan) MarkFailed(t *ObjectTask, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Retries++
	t.LastError = errString(err)
	t.Status = StatusFailed
	t.GID = ""
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// MarkCommitted flags t as fully verified and in blobs/.
func (p *Plan) MarkCommitted(t *ObjectTask) {
	p.mu.Lock()
	p.markCommittedLocked(t)
}

func (p *Plan) markCommittedLocked(t *ObjectTask) {
	t.Status = StatusCommitted
	t.GID = ""
	t.URL = ""
	t.URLExpiresAt = nil
	t.LastError = ""
	t.UpdatedAt = time.Now().UTC()
	p.persistLocked()
}

// AllCommitted reports whether every object is committed.
func (p *Plan) AllCommitted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.Objects {
		if t.Status != StatusCommitted {
			return false
		}
	}
	return true
}

// SaveNow forces a state.json write.
func (p *Plan) SaveNow() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.persistLocked()
}

// persistLocked writes state.json at most once per second unless forced by
// SaveNow; the throttling is best-effort and callers hold p.mu.
func (p *Plan) persistLocked() error {
	if time.Since(p.lastSave) < time.Second {
		return nil
	}
	p.lastSave = time.Now()
	st := &stateFile{
		Version:           stateVersion,
		Image:             p.Image,
		Platform:          p.Platform.String(),
		ManifestDigest:    p.ManifestDigest,
		ManifestMediaType: p.ManifestMediaType,
		UpdatedAt:         time.Now().UTC(),
	}
	for _, t := range p.Objects {
		st.Objects = append(st.Objects, stateObject{
			Kind:         string(t.Kind),
			Digest:       t.Digest,
			Size:         t.Size,
			MediaType:    t.MediaType,
			Status:       t.Status,
			PartName:     t.PartName,
			GID:          t.GID,
			URL:          t.URL,
			URLExpiresAt: t.URLExpiresAt,
			AltURLs:      t.AltURLs,
			Retries:      t.Retries,
			LastError:    t.LastError,
			UpdatedAt:    t.UpdatedAt,

			RangeUnsupported: t.RangeUnsupported,
		})
	}
	return saveState(p.ImageDir, st)
}

// SaveFinal is an unthrottled save used at end of run.
func (p *Plan) SaveFinal() error {
	p.mu.Lock()
	p.lastSave = time.Time{}
	p.mu.Unlock()
	return p.SaveNow()
}

// commitFile moves a verified .part into blobs/ atomically and marks the
// task committed. Callers must have verified digest+size already, and either
// hold p.mu (CommitVerified) or be single-threaded (Build's reconcileTask).
func (p *Plan) commitFile(t *ObjectTask, part string) error {
	final := p.FinalPath(t)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	if err := os.Rename(part, final); err != nil {
		return err
	}
	os.Remove(part + ".aria2")
	p.markCommittedLocked(t)
	return nil
}

// CommitVerified is called by the scheduler after verify.Blob succeeds.
func (p *Plan) CommitVerified(t *ObjectTask) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commitFile(t, p.PartPath(t))
}

// Release saves state and drops the directory lock.
func (p *Plan) Release() {
	if p == nil {
		return
	}
	p.SaveFinal()
	if p.lock != nil {
		p.lock.Release()
		p.lock = nil
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}

// TagRef parses ExportTag into a name.Tag for docker-archive export.
func (p *Plan) TagRef() (name.Tag, error) {
	return name.NewTag(p.ExportTag, name.WithDefaultRegistry("docker.io"))
}

// Package plan builds and persists the per-image download plan: the task
// list (manifest/config/layers), its on-disk layout under
// <output>/<registry>/<repo>/<ref>/, and the resumable state.json.
package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"imgpull/internal/fsx"
)

// stateVersion is the state.json schema version.
const stateVersion = 1

// stateObject is the JSON form of one task in state.json.
type stateObject struct {
	Kind         string     `json:"kind"`
	Digest       string     `json:"digest"`
	Size         int64      `json:"size"`
	MediaType    string     `json:"mediaType"`
	Status       string     `json:"status"`
	PartName     string     `json:"partName,omitempty"`
	GID          string     `json:"gid,omitempty"`
	URL          string     `json:"url,omitempty"`
	URLExpiresAt *time.Time `json:"urlExpiresAt,omitempty"`
	AltURLs      []string   `json:"altUrls,omitempty"`
	Retries      int        `json:"retries"`
	LastError    string     `json:"lastError,omitempty"`
	UpdatedAt    time.Time  `json:"updatedAt"`
}

// stateFile is the state.json document.
type stateFile struct {
	Version           int           `json:"version"`
	Image             string        `json:"image"`
	Platform          string        `json:"platform,omitempty"`
	ManifestDigest    string        `json:"manifestDigest"`
	ManifestMediaType string        `json:"manifestMediaType"`
	Objects           []stateObject `json:"objects"`
	UpdatedAt         time.Time     `json:"updatedAt"`
}

// loadState reads state.json from imageDir. Returns (nil, nil) when absent.
// A corrupt file is reported as an error with the path so callers can warn
// and continue from scratch.
func loadState(imageDir string) (*stateFile, error) {
	path := filepath.Join(imageDir, "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var st stateFile
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", path, err)
	}
	if st.Version != stateVersion {
		return nil, fmt.Errorf("unsupported state version %d in %s", st.Version, path)
	}
	return &st, nil
}

// saveState atomically writes the state document.
func saveState(imageDir string, st *stateFile) error {
	st.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return fsx.WriteFileAtomic(filepath.Join(imageDir, "state.json"), data, 0o644)
}

// fileLock holds an exclusive advisory lock on <imageDir>/.imgpull.lock so
// two imgpull processes never fight over the same image directory.
type fileLock struct {
	f *os.File
}

func lockImageDir(imageDir string) (*fileLock, error) {
	if err := os.MkdirAll(imageDir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(imageDir, ".imgpull.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("image directory %s is locked by another imgpull process", imageDir)
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) Release() {
	if l != nil && l.f != nil {
		syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
		l.f.Close()
		l.f = nil
	}
}

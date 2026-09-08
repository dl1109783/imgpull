package verify

import "os"

// FileSize returns the size of the file at path in bytes.
func FileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

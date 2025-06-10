package utils

import (
	"fmt"
	"os"
)

func ForceDirExists(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("provided a file instead of directory")
	}
	return nil
}
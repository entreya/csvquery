//go:build windows
// +build windows

package common

import (
	"io"
	"os"
)

// MmapFile memory maps a file (Fallback to ReadAll on Windows for now to avoid unsafe pointer arithmetic complexity without external lib)
// TODO: Implement proper Windows mmap
func MmapFile(f *os.File) ([]byte, error) {
	return io.ReadAll(f)
}

// MunmapFile unmaps the memory (No-op for ReadAll)
func MunmapFile(data []byte) error {
	return nil
}

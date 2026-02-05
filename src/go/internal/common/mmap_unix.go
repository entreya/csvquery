//go:build !windows
// +build !windows

package common

import (
	"os"
	"syscall"
)

// MmapFile memory maps a file for reading
func MmapFile(f *os.File) ([]byte, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	if size == 0 {
		return []byte{}, nil
	}

	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

// MunmapFile unmaps the memory
func MunmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}

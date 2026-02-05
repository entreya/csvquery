//go:build !windows

package writer

import (
	"os"
	"syscall"
)

// lockFile acquires an exclusive lock on the file
func lockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
}

// unlockFile releases the lock
func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

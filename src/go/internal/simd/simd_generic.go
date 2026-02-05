//go:build !amd64

package simd

import "bytes"

func init() {
	scanImpl = scanSeparatorsGeneric
}

// scanSeparatorsGeneric is a pure Go fallback for non-AMD64 architectures.
func scanSeparatorsGeneric(data []byte, sep byte) uint64 {
	return uint64(bytes.Count(data, []byte{sep}))
}

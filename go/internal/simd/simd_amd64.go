//go:build amd64

package simd

import "golang.org/x/sys/cpu"

func init() {
	if cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW {
		scanImpl = ScanSeparatorsAVX512
	} else if cpu.X86.HasAVX2 {
		scanImpl = ScanSeparatorsAVX2
	} else {
		scanImpl = scanSeparatorsGeneric
	}
}

// scanSeparatorsGeneric is the fallback for AMD64 CPUs without AVX2.
// It matches the signature of the ASM functions.
func scanSeparatorsGeneric(data []byte, sep byte) uint64 {
	// Simple scalar loop or byte-byte check
	// Using generic fallback from standard library equivalent
	var count uint64
	for _, b := range data {
		if b == sep {
			count++
		}
	}
	return count
}

// Declared in ops_amd64.s
func ScanSeparatorsAVX2(data []byte, sep byte) uint64
func ScanSeparatorsAVX512(data []byte, sep byte) uint64

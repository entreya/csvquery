//go:build amd64

// Package simd provides SIMD-accelerated CSV delimiter scanning.
//
// On AMD64, it uses AVX2 (256-bit) or SSE4.2 (128-bit) instructions
// to scan for quotes, commas, and newlines at hardware speed.
// This enables processing 64 bytes per iteration, achieving
// near-memory-bandwidth throughput.
package simd

import (
	"unsafe"
)

// scanAVX2 scans data using AVX2 instructions.
// Returns number of bytes processed (multiple of 64).
//
//go:noescape
func scanAVX2(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int

// scanSSE42 scans data using SSE4.2 instructions.
// Returns number of bytes processed (multiple of 64).
//
//go:noescape
func scanSSE42(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int

// checkAVX2 checks if the CPU supports AVX2.
//
//go:noescape
func checkAVX2() bool

// checkSSE42 checks if the CPU supports SSE4.2.
//
//go:noescape
func checkSSE42() bool

// useAVX2 is set at init time based on CPU capabilities.
var useAVX2 bool

// useSSE42 is set at init time based on CPU capabilities.
var useSSE42 bool

func init() {
	useAVX2 = checkAVX2()
	useSSE42 = checkSSE42()
}

// HasAVX2 returns true if AVX2 is available on this CPU.
func HasAVX2() bool {
	return useAVX2
}

// Scan scans the input buffer and populates bitmaps for quotes, commas, and newlines.
//
// Each bit in the output slices corresponds to one byte in the input.
// A bit is set to 1 if that byte is the corresponding character.
//
// The bitmaps must be pre-allocated with length >= (len(input) + 63) / 64.
//
// This function dispatches to AVX2 or SSE4.2 based on CPU capabilities,
// and handles the tail bytes (< 64) with a scalar fallback.
func Scan(input []byte, quotes, commas, newlines []uint64) {
	if len(input) == 0 {
		return
	}

	pInput := unsafe.Pointer(&input[0])
	pQuotes := unsafe.Pointer(&quotes[0])
	pCommas := unsafe.Pointer(&commas[0])
	pNewlines := unsafe.Pointer(&newlines[0])
	size := len(input)

	processed := 0
	if useAVX2 {
		processed = scanAVX2(pInput, size, pQuotes, pCommas, pNewlines)
	} else if useSSE42 {
		processed = scanSSE42(pInput, size, pQuotes, pCommas, pNewlines)
	}

	// Process tail (scalar fallback for remaining bytes < 64 or if no SIMD)
	for i := processed; i < size; i++ {
		b := input[i]
		wordIdx := i / 64
		bitPos := uint(i % 64)
		switch b {
		case '"':
			quotes[wordIdx] |= 1 << bitPos
		case ',':
			commas[wordIdx] |= 1 << bitPos
		case '\n':
			newlines[wordIdx] |= 1 << bitPos
		}
	}
}

// ScanWithSeparator scans the input buffer for quotes, a custom separator, and newlines.
// This is useful for CSV files that use semicolons, tabs, or other separators.
func ScanWithSeparator(input []byte, sep byte, quotes, seps, newlines []uint64) {
	if len(input) == 0 {
		return
	}

	// First, scan quotes and newlines using SIMD
	pInput := unsafe.Pointer(&input[0])
	pQuotes := unsafe.Pointer(&quotes[0])
	pNewlines := unsafe.Pointer(&newlines[0])
	size := len(input)

	processed := 0
	if useAVX2 {
		// We scan quotes and newlines but ignore the comma output
		tempCommas := make([]uint64, len(seps))
		pTempCommas := unsafe.Pointer(&tempCommas[0])
		processed = scanAVX2(pInput, size, pQuotes, pTempCommas, pNewlines)
	} else if useSSE42 {
		tempCommas := make([]uint64, len(seps))
		pTempCommas := unsafe.Pointer(&tempCommas[0])
		processed = scanSSE42(pInput, size, pQuotes, pTempCommas, pNewlines)
	}

	// Handle tail for quotes and newlines
	for i := processed; i < size; i++ {
		b := input[i]
		wordIdx := i / 64
		bitPos := uint(i % 64)
		switch b {
		case '"':
			quotes[wordIdx] |= 1 << bitPos
		case '\n':
			newlines[wordIdx] |= 1 << bitPos
		}
	}

	// Scan for custom separator (scalar - could be SIMD-optimized later)
	for i := 0; i < size; i++ {
		if input[i] == sep {
			wordIdx := i / 64
			bitPos := uint(i % 64)
			seps[wordIdx] |= 1 << bitPos
		}
	}
}

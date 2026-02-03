//go:build !amd64

// Package simd provides SIMD-accelerated CSV delimiter scanning.
//
// On non-AMD64 platforms (e.g., ARM64), this package provides a pure Go
// fallback implementation that maintains the same interface.
package simd

// HasAVX2 returns false on non-AMD64 platforms.
func HasAVX2() bool {
	return false
}

// Scan scans the input buffer and populates bitmaps for quotes, commas, and newlines.
//
// Each bit in the output slices corresponds to one byte in the input.
// A bit is set to 1 if that byte is the corresponding character.
//
// The bitmaps must be pre-allocated with length >= (len(input) + 63) / 64.
//
// This is the pure Go fallback for non-AMD64 platforms.
func Scan(input []byte, quotes, commas, newlines []uint64) {
	for i, b := range input {
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
	for i, b := range input {
		wordIdx := i / 64
		bitPos := uint(i % 64)
		switch b {
		case '"':
			quotes[wordIdx] |= 1 << bitPos
		case sep:
			seps[wordIdx] |= 1 << bitPos
		case '\n':
			newlines[wordIdx] |= 1 << bitPos
		}
	}
}

package simd

// ScanSeparators counts the occurrences of sep in data using the best available algorithm.
func ScanSeparators(data []byte, sep byte) uint64 {
	return scanImpl(data, sep)
}

// scanImpl is the function pointer to the optimized implementation.
// It is set in init() based on CPU flags for AMD64, or defaults to the generic version.
var scanImpl func(data []byte, sep byte) uint64

// Scan generates bitmaps for quotes, commas, and newlines.
// Deprecated: optimizations moved to ScanSeparators, this is now a fallback wrapper or generic.
func Scan(data []byte, quotes, commas, newlines []uint64) {
	ScanWithSeparator(data, ',', quotes, commas, newlines)
}

// ScanWithSeparator generates bitmaps for a custom separator.
func ScanWithSeparator(data []byte, sep byte, quotes, seps, newlines []uint64) {
	// Fallback to generic implementation for now to satisfy build
	scanWithSeparatorGeneric(data, sep, quotes, seps, newlines)
}

func scanWithSeparatorGeneric(data []byte, sep byte, quotes, seps, newlines []uint64) {
	for i, b := range data {
		if b == '"' {
			quotes[i/64] |= 1 << uint(i%64)
		} else if b == sep {
			seps[i/64] |= 1 << uint(i%64)
		} else if b == '\n' {
			newlines[i/64] |= 1 << uint(i%64)
		}
	}
}

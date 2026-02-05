package simd

import (
	"bytes"
	"strings"
	"testing"
)

func TestScanSeparators(t *testing.T) {
	tests := []struct {
		name  string
		input string
		sep   byte
		want  uint64
	}{
		{"empty", "", ',', 0},
		{"no separators", "hello world", ',', 0},
		{"single separator", "hello,world", ',', 1},
		{"multiple separators", "a,b,c,d", ',', 3},
		{"start and end", ",middle,", ',', 2},
		{"only separators", ",,,", ',', 3},
		{"newline separator", "line1\nline2\n", '\n', 2},
		{"custom separator", "a|b|c", '|', 2},
		{"long string", strings.Repeat("a", 100) + "," + strings.Repeat("b", 100), ',', 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ScanSeparators([]byte(tt.input), tt.sep); got != tt.want {
				t.Errorf("ScanSeparators() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScanSeparatorsLarge(t *testing.T) {
	// 5KB buffer (triggers AVX2/AVX512 paths)
	size := 5000
	data := make([]byte, size)
	sep := byte(',')
	expected := 0

	for i := 0; i < size; i++ {
		if i%100 == 0 {
			data[i] = sep
			expected++
		} else {
			data[i] = 'x'
		}
	}

	if got := ScanSeparators(data, sep); got != uint64(expected) {
		t.Errorf("ScanSeparators(large) = %v, want %v", got, expected)
	}
}

func TestScanSeparatorsTail(t *testing.T) {
	// specifically test boundaries near 32/64/256 bytes
	sizes := []int{1, 31, 32, 33, 63, 64, 65, 255, 256, 257}
	sep := byte(',')

	for _, size := range sizes {
		data := bytes.Repeat([]byte{'.'}, size)
		// Place separator at end
		data[size-1] = sep

		if got := ScanSeparators(data, sep); got != 1 {
			t.Errorf("ScanSeparators(size=%d) = %v, want 1", size, got)
		}
	}
}

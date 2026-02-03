package simd

import (
	"math/bits"
	"testing"
)

func TestScanBasic(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantQuotes   []int // positions of quotes
		wantCommas   []int // positions of commas
		wantNewlines []int // positions of newlines
	}{
		{
			name:         "simple CSV line",
			input:        "a,b,c\n",
			wantQuotes:   nil,
			wantCommas:   []int{1, 3},
			wantNewlines: []int{5},
		},
		{
			name:         "quoted field",
			input:        `"hello",world` + "\n",
			wantQuotes:   []int{0, 6},
			wantCommas:   []int{7},
			wantNewlines: []int{13},
		},
		{
			name:         "quoted comma",
			input:        `"a,b",c` + "\n",
			wantQuotes:   []int{0, 4},
			wantCommas:   []int{2, 5},
			wantNewlines: []int{7},
		},
		{
			name:         "escaped quote",
			input:        `"a""b",c` + "\n",
			wantQuotes:   []int{0, 2, 3, 5},
			wantCommas:   []int{6},
			wantNewlines: []int{8},
		},
		{
			name:         "multiple lines",
			input:        "a,b\nc,d\n",
			wantQuotes:   nil,
			wantCommas:   []int{1, 5},
			wantNewlines: []int{3, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []byte(tt.input)
			bitmapLen := (len(input) + 63) / 64
			quotes := make([]uint64, bitmapLen)
			commas := make([]uint64, bitmapLen)
			newlines := make([]uint64, bitmapLen)

			Scan(input, quotes, commas, newlines)

			gotQuotes := bitmapToPositions(quotes, len(input))
			gotCommas := bitmapToPositions(commas, len(input))
			gotNewlines := bitmapToPositions(newlines, len(input))

			if !equalIntSlices(gotQuotes, tt.wantQuotes) {
				t.Errorf("quotes: got %v, want %v", gotQuotes, tt.wantQuotes)
			}
			if !equalIntSlices(gotCommas, tt.wantCommas) {
				t.Errorf("commas: got %v, want %v", gotCommas, tt.wantCommas)
			}
			if !equalIntSlices(gotNewlines, tt.wantNewlines) {
				t.Errorf("newlines: got %v, want %v", gotNewlines, tt.wantNewlines)
			}
		})
	}
}

func TestScanLargeInput(t *testing.T) {
	// Create input larger than 64 bytes to test SIMD path
	input := make([]byte, 256)
	for i := range input {
		switch i % 10 {
		case 3:
			input[i] = ','
		case 7:
			input[i] = '"'
		case 9:
			input[i] = '\n'
		default:
			input[i] = 'x'
		}
	}

	bitmapLen := (len(input) + 63) / 64
	quotes := make([]uint64, bitmapLen)
	commas := make([]uint64, bitmapLen)
	newlines := make([]uint64, bitmapLen)

	Scan(input, quotes, commas, newlines)

	// Verify some positions
	for i := 0; i < len(input); i++ {
		isQuote := (quotes[i/64] & (1 << uint(i%64))) != 0
		isComma := (commas[i/64] & (1 << uint(i%64))) != 0
		isNewline := (newlines[i/64] & (1 << uint(i%64))) != 0

		shouldBeQuote := input[i] == '"'
		shouldBeComma := input[i] == ','
		shouldBeNewline := input[i] == '\n'

		if isQuote != shouldBeQuote {
			t.Errorf("position %d: quote mismatch, got %v want %v", i, isQuote, shouldBeQuote)
		}
		if isComma != shouldBeComma {
			t.Errorf("position %d: comma mismatch, got %v want %v", i, isComma, shouldBeComma)
		}
		if isNewline != shouldBeNewline {
			t.Errorf("position %d: newline mismatch, got %v want %v", i, isNewline, shouldBeNewline)
		}
	}
}

func TestScanWithSeparator(t *testing.T) {
	input := []byte("a;b;c\nd;e;f\n")
	bitmapLen := (len(input) + 63) / 64
	quotes := make([]uint64, bitmapLen)
	seps := make([]uint64, bitmapLen)
	newlines := make([]uint64, bitmapLen)

	ScanWithSeparator(input, ';', quotes, seps, newlines)

	gotSeps := bitmapToPositions(seps, len(input))
	wantSeps := []int{1, 3, 7, 9}
	if !equalIntSlices(gotSeps, wantSeps) {
		t.Errorf("seps: got %v, want %v", gotSeps, wantSeps)
	}
}

// bitmapToPositions converts a bitmap to a list of set bit positions
func bitmapToPositions(bitmap []uint64, maxLen int) []int {
	var positions []int
	for wordIdx, word := range bitmap {
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			pos := wordIdx*64 + tz
			if pos < maxLen {
				positions = append(positions, pos)
			}
			word &^= 1 << tz
		}
	}
	return positions
}

func equalIntSlices(a, b []int) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Benchmarks

func BenchmarkScan64(b *testing.B) {
	input := make([]byte, 64)
	for i := range input {
		input[i] = 'x'
	}
	input[10] = ','
	input[30] = '"'
	input[63] = '\n'

	bitmapLen := 1
	quotes := make([]uint64, bitmapLen)
	commas := make([]uint64, bitmapLen)
	newlines := make([]uint64, bitmapLen)

	b.ResetTimer()
	b.SetBytes(64)

	for i := 0; i < b.N; i++ {
		quotes[0] = 0
		commas[0] = 0
		newlines[0] = 0
		Scan(input, quotes, commas, newlines)
	}
}

func BenchmarkScan1KB(b *testing.B) {
	input := make([]byte, 1024)
	for i := range input {
		input[i] = 'x'
	}
	// Add some delimiters
	for i := 0; i < 1024; i += 10 {
		input[i] = ','
	}

	bitmapLen := (1024 + 63) / 64
	quotes := make([]uint64, bitmapLen)
	commas := make([]uint64, bitmapLen)
	newlines := make([]uint64, bitmapLen)

	b.ResetTimer()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		for j := range quotes {
			quotes[j] = 0
			commas[j] = 0
			newlines[j] = 0
		}
		Scan(input, quotes, commas, newlines)
	}
}

func BenchmarkScan1MB(b *testing.B) {
	input := make([]byte, 1024*1024)
	for i := range input {
		input[i] = 'x'
	}
	// Add some delimiters
	for i := 0; i < len(input); i += 50 {
		input[i] = ','
	}
	for i := 0; i < len(input); i += 100 {
		input[i] = '\n'
	}

	bitmapLen := (len(input) + 63) / 64
	quotes := make([]uint64, bitmapLen)
	commas := make([]uint64, bitmapLen)
	newlines := make([]uint64, bitmapLen)

	b.ResetTimer()
	b.SetBytes(int64(len(input)))

	for i := 0; i < b.N; i++ {
		for j := range quotes {
			quotes[j] = 0
			commas[j] = 0
			newlines[j] = 0
		}
		Scan(input, quotes, commas, newlines)
	}
}

// Fuzz test
func FuzzScan(f *testing.F) {
	// Seed corpus
	f.Add([]byte("a,b,c\n"))
	f.Add([]byte(`"hello",world` + "\n"))
	f.Add([]byte(`"a,b",c` + "\n"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}

		bitmapLen := (len(input) + 63) / 64
		quotes := make([]uint64, bitmapLen)
		commas := make([]uint64, bitmapLen)
		newlines := make([]uint64, bitmapLen)

		// Should not panic
		Scan(input, quotes, commas, newlines)

		// Verify correctness
		for i := 0; i < len(input); i++ {
			isQuote := (quotes[i/64] & (1 << uint(i%64))) != 0
			isComma := (commas[i/64] & (1 << uint(i%64))) != 0
			isNewline := (newlines[i/64] & (1 << uint(i%64))) != 0

			if isQuote != (input[i] == '"') {
				t.Errorf("quote mismatch at %d", i)
			}
			if isComma != (input[i] == ',') {
				t.Errorf("comma mismatch at %d", i)
			}
			if isNewline != (input[i] == '\n') {
				t.Errorf("newline mismatch at %d", i)
			}
		}
	})
}

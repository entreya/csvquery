package common

import (
	"bytes"
	"io"
	"testing"
)

func BenchmarkWriteRecord(b *testing.B) {
	var key [64]byte
	copy(key[:], "test_key_1234567890")
	rec := IndexRecord{
		Key:    key,
		Offset: 12345,
		Line:   67890,
	}
	// Pre-allocate buffer huge enough to avoid reallocations during test
	// But WriteRecord takes an io.Writer. We'll use bytes.Buffer but Reset it.
	// Actually, bytes.Buffer.Write() might alloc if growing.
	// Best to use io.Discard if we just want to test serialization overhead?
	// But WriteRecord does binary.Write (now direct write)...

	b.ReportAllocs()
	b.ResetTimer()

	var buf bytes.Buffer
	buf.Grow(RecordSize) // Ensure capacity

	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := WriteRecord(&buf, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadRecord(b *testing.B) {
	// Prepare a buffer with one record
	var buf bytes.Buffer
	var key [64]byte
	copy(key[:], "test_key_1234567890")
	rec := IndexRecord{
		Key:    key,
		Offset: 12345,
		Line:   67890,
	}
	_ = WriteRecord(&buf, rec)
	data := buf.Bytes()
	reader := bytes.NewReader(data)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader.Reset(data)
		_, err := ReadRecord(reader)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}

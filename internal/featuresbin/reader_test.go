package featuresbin

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func segmentFixture(t *testing.T, counts ...uint64) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteString(magic)
	b.WriteByte(versionV4)
	writeCount := func(n uint64) {
		var buf [binary.MaxVarintLen64]byte
		b.Write(buf[:binary.PutUvarint(buf[:], n)])
	}
	writeCount(uint64(len(counts)))
	for range 2 {
		if err := binary.Write(&b, binary.LittleEndian, float64(0)); err != nil {
			t.Fatal(err)
		}
	}
	writeCount(0) // routes
	writeCount(0) // string pieces
	for _, count := range counts {
		b.Write([]byte{0, 0, 0, 0}) // segment bounds
		writeCount(count)
		for range count {
			// No strings, priority 1, travelways source, no route or coordinates.
			b.Write([]byte{0, 0, 1, 0, 0, 0})
		}
	}
	return b.Bytes()
}

func TestReadAllSegmentFeatures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		counts []uint64
	}{
		{"no segments", nil},
		{"single segment", []uint64{3}},
		{"multiple segments", []uint64{2, 3}},
		{"empty first segment", []uint64{0, 2}},
		{"empty middle segment", []uint64{2, 0, 3}},
		{"empty last segment", []uint64{2, 0}},
		{"all empty", []uint64{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, _, _, err := Read(bytes.NewReader(segmentFixture(t, tc.counts...)))
			if err != nil {
				t.Fatal(err)
			}
			var want uint64
			for _, count := range tc.counts {
				want += count
			}
			if uint64(len(features)) != want {
				t.Fatalf("decoded %d features; want %d", len(features), want)
			}
		})
	}
}

func TestReadTruncatedLastSegment(t *testing.T) {
	data := segmentFixture(t, 2)
	if _, _, _, err := Read(bytes.NewReader(data[:len(data)-1])); err == nil {
		t.Fatal("expected an error for a truncated final feature")
	}
}

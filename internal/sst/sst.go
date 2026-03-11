// Package sst encodes and decodes the custom binary SST format used to
// snapshot RocksDB index data in S3.
//
// Binary layout
//
//	[8  bytes] xxhash64 checksum of everything after this field
//	[4  bytes] uint32 big-endian entry count
//	per entry:
//	  [4 bytes] uint32 key length
//	  [N bytes] key bytes
//	  [4 bytes] uint32 value length
//	  [M bytes] value bytes
package sst

import (
	"bytes"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Entry is a single key-value record inside an SST file.
type Entry struct {
	Key   string
	Value string
}

// Build serialises entries into the SST binary format and returns the bytes
// together with the xxhash64 checksum embedded in the header.
func Build(entries []Entry) ([]byte, uint64) {
	var buf bytes.Buffer

	// Reserve 8 bytes for the checksum (written last).
	buf.Write(make([]byte, 8))

	count := uint32(len(entries))
	buf.Write(uint32BE(count))

	for _, e := range entries {
		buf.Write(uint32BE(uint32(len(e.Key))))
		buf.WriteString(e.Key)
		buf.Write(uint32BE(uint32(len(e.Value))))
		buf.WriteString(e.Value)
	}

	data := buf.Bytes()
	checksum := xxhash.Sum64(data[8:])

	// Write checksum into the first 8 bytes.
	be64(data, checksum)

	return data, checksum
}

// Parse deserialises an SST byte slice, validates the checksum, and returns
// the entries it contains.
func Parse(data []byte) ([]Entry, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("sst: data too short (%d bytes)", len(data))
	}

	stored := readBE64(data, 0)
	actual := xxhash.Sum64(data[8:])
	if stored != actual {
		return nil, fmt.Errorf("sst: checksum mismatch (stored=%d actual=%d)", stored, actual)
	}

	count := readBE32(data, 8)
	entries := make([]Entry, 0, count)
	pos := 12

	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("sst: truncated at entry %d (key len)", i)
		}
		kLen := int(readBE32(data, pos))
		pos += 4

		if pos+kLen > len(data) {
			return nil, fmt.Errorf("sst: truncated at entry %d (key body)", i)
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+4 > len(data) {
			return nil, fmt.Errorf("sst: truncated at entry %d (value len)", i)
		}
		vLen := int(readBE32(data, pos))
		pos += 4

		if pos+vLen > len(data) {
			return nil, fmt.Errorf("sst: truncated at entry %d (value body)", i)
		}
		value := string(data[pos : pos+vLen])
		pos += vLen

		entries = append(entries, Entry{Key: key, Value: value})
	}

	return entries, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func uint32BE(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func be64(dst []byte, v uint64) {
	for i := 0; i < 8; i++ {
		dst[7-i] = byte(v >> (8 * i))
	}
}

func readBE64(d []byte, off int) uint64 {
	return uint64(d[off])<<56 | uint64(d[off+1])<<48 |
		uint64(d[off+2])<<40 | uint64(d[off+3])<<32 |
		uint64(d[off+4])<<24 | uint64(d[off+5])<<16 |
		uint64(d[off+6])<<8 | uint64(d[off+7])
}

func readBE32(d []byte, off int) uint32 {
	return uint32(d[off])<<24 | uint32(d[off+1])<<16 |
		uint32(d[off+2])<<8 | uint32(d[off+3])
}
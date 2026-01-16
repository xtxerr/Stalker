package wal

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/xtxerr/stalker/internal/storage/types"
)

// Sample encoding format (binary, little-endian):
// - Namespace length (2 bytes) + Namespace string
// - Target length (2 bytes) + Target string
// - Poller length (2 bytes) + Poller string
// - TimestampMs (8 bytes)
// - Value (8 bytes, float64)
// - Valid (1 byte, bool)
// - Error length (2 bytes) + Error string
// - PollMs (4 bytes)

// encodeSamples encodes a slice of samples into a binary format.
func encodeSamples(samples []types.Sample) ([]byte, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	// Estimate size: ~100 bytes per sample average
	buf := make([]byte, 0, len(samples)*100)

	// Write sample count
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(samples)))

	for _, s := range samples {
		// Namespace
		buf = appendString(buf, s.Namespace)
		// Target
		buf = appendString(buf, s.Target)
		// Poller
		buf = appendString(buf, s.Poller)
		// TimestampMs
		buf = binary.LittleEndian.AppendUint64(buf, uint64(s.TimestampMs))
		// Value
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.Value))
		// Valid
		if s.Valid {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		// Error
		buf = appendString(buf, s.Error)
		// PollMs
		buf = binary.LittleEndian.AppendUint32(buf, uint32(s.PollMs))
	}

	return buf, nil
}

// decodeSamples decodes a binary format into a slice of samples.
func decodeSamples(data []byte) ([]types.Sample, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for sample count")
	}

	count := int(binary.LittleEndian.Uint32(data[0:4]))
	if count == 0 {
		return nil, nil
	}

	samples := make([]types.Sample, count)
	offset := 4

	for i := 0; i < count; i++ {
		var s types.Sample
		var err error

		// Namespace
		s.Namespace, offset, err = readString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("sample %d namespace: %w", i, err)
		}

		// Target
		s.Target, offset, err = readString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("sample %d target: %w", i, err)
		}

		// Poller
		s.Poller, offset, err = readString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("sample %d poller: %w", i, err)
		}

		// TimestampMs
		if offset+8 > len(data) {
			return nil, fmt.Errorf("sample %d: data too short for timestamp", i)
		}
		s.TimestampMs = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Value
		if offset+8 > len(data) {
			return nil, fmt.Errorf("sample %d: data too short for value", i)
		}
		s.Value = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Valid
		if offset+1 > len(data) {
			return nil, fmt.Errorf("sample %d: data too short for valid", i)
		}
		s.Valid = data[offset] == 1
		offset++

		// Error
		s.Error, offset, err = readString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("sample %d error: %w", i, err)
		}

		// PollMs
		if offset+4 > len(data) {
			return nil, fmt.Errorf("sample %d: data too short for poll_ms", i)
		}
		s.PollMs = int32(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		samples[i] = s
	}

	return samples, nil
}

// appendString appends a length-prefixed string to the buffer.
func appendString(buf []byte, s string) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// readString reads a length-prefixed string from the buffer.
func readString(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", offset, fmt.Errorf("data too short for string length")
	}

	length := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	if offset+length > len(data) {
		return "", offset, fmt.Errorf("data too short for string content")
	}

	s := string(data[offset : offset+length])
	return s, offset + length, nil
}

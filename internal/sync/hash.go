package sync

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"sort"
)

// =============================================================================
// Hash Builder
// =============================================================================

// HashBuilder provides a fluent API for building content hashes.
//
// Usage:
//
//	hash := NewHashBuilder().
//	    String(entity.Name).
//	    String(entity.Description).
//	    StringMap(entity.Labels).
//	    Int(entity.Priority).
//	    Build()
//
// The hash is deterministic - same inputs always produce the same output.
// Order of operations matters.
type HashBuilder struct {
	h hash.Hash64
}

// NewHashBuilder creates a new hash builder.
func NewHashBuilder() *HashBuilder {
	h := fnv.New64a()
	return &HashBuilder{h: h}
}

// String adds a string value to the hash.
func (b *HashBuilder) String(s string) *HashBuilder {
	b.h.Write([]byte(s))
	b.h.Write([]byte{0}) // Separator to avoid collisions
	return b
}

// Strings adds multiple strings to the hash.
func (b *HashBuilder) Strings(ss []string) *HashBuilder {
	// Sort for deterministic ordering
	sorted := make([]string, len(ss))
	copy(sorted, ss)
	sort.Strings(sorted)

	b.Int(len(sorted)) // Length prefix
	for _, s := range sorted {
		b.String(s)
	}
	return b
}

// StringMap adds a map of strings to the hash.
// Keys are sorted for deterministic ordering.
func (b *HashBuilder) StringMap(m map[string]string) *HashBuilder {
	if m == nil {
		b.Int(0)
		return b
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b.Int(len(keys))
	for _, k := range keys {
		b.String(k)
		b.String(m[k])
	}
	return b
}

// Int adds an integer to the hash.
func (b *HashBuilder) Int(i int) *HashBuilder {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	b.h.Write(buf)
	return b
}

// Int32 adds an int32 to the hash.
func (b *HashBuilder) Int32(i int32) *HashBuilder {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(i))
	b.h.Write(buf)
	return b
}

// Int64 adds an int64 to the hash.
func (b *HashBuilder) Int64(i int64) *HashBuilder {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	b.h.Write(buf)
	return b
}

// Uint32 adds a uint32 to the hash.
func (b *HashBuilder) Uint32(i uint32) *HashBuilder {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	b.h.Write(buf)
	return b
}

// Uint64 adds a uint64 to the hash.
func (b *HashBuilder) Uint64(i uint64) *HashBuilder {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	b.h.Write(buf)
	return b
}

// Bool adds a boolean to the hash.
func (b *HashBuilder) Bool(v bool) *HashBuilder {
	if v {
		b.h.Write([]byte{1})
	} else {
		b.h.Write([]byte{0})
	}
	return b
}

// Bytes adds raw bytes to the hash.
func (b *HashBuilder) Bytes(data []byte) *HashBuilder {
	b.Int(len(data))
	b.h.Write(data)
	return b
}

// OptionalString adds a string pointer (nil-safe).
func (b *HashBuilder) OptionalString(s *string) *HashBuilder {
	if s == nil {
		b.Bool(false)
	} else {
		b.Bool(true)
		b.String(*s)
	}
	return b
}

// OptionalUint32 adds a uint32 pointer (nil-safe).
func (b *HashBuilder) OptionalUint32(i *uint32) *HashBuilder {
	if i == nil {
		b.Bool(false)
	} else {
		b.Bool(true)
		b.Uint32(*i)
	}
	return b
}

// Build returns the final hash value.
func (b *HashBuilder) Build() uint64 {
	return b.h.Sum64()
}

// =============================================================================
// Quick Hash Functions
// =============================================================================

// HashString returns the hash of a single string.
func HashString(s string) uint64 {
	return NewHashBuilder().String(s).Build()
}

// HashStrings returns the hash of multiple strings (sorted).
func HashStrings(ss []string) uint64 {
	return NewHashBuilder().Strings(ss).Build()
}

// HashStringMap returns the hash of a string map (sorted by key).
func HashStringMap(m map[string]string) uint64 {
	return NewHashBuilder().StringMap(m).Build()
}

// HashBytes returns the hash of raw bytes.
func HashBytes(data []byte) uint64 {
	return NewHashBuilder().Bytes(data).Build()
}

// LOCATION: internal/scheduler/pollerkey.go
// NEW FILE - Extrahiert PollerKey in separate Datei mit Optimierungen
//
// OPTIMIERUNGEN:
// 1. strings.Builder statt + Konkatenation
// 2. Optionales String-Caching für wiederholte Aufrufe
// 3. Pre-allokierte Größe für Builder

package scheduler

import (
	"fmt"
	"strings"
	"sync"
)

// PollerKey uniquely identifies a poller.
type PollerKey struct {
	Namespace string
	Target    string
	Poller    string
}

// String returns the string representation.
// OPTIMIZED: Uses strings.Builder with pre-allocated capacity
func (k PollerKey) String() string {
	// Geschätzte Größe: ns + target + poller + 2 Slashes
	var b strings.Builder
	b.Grow(len(k.Namespace) + len(k.Target) + len(k.Poller) + 2)
	b.WriteString(k.Namespace)
	b.WriteByte('/')
	b.WriteString(k.Target)
	b.WriteByte('/')
	b.WriteString(k.Poller)
	return b.String()
}

// ParsePollerKey parses a key string into a PollerKey.
func ParsePollerKey(s string) (PollerKey, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 {
		return PollerKey{}, fmt.Errorf("invalid poller key: %s", s)
	}
	return PollerKey{
		Namespace: parts[0],
		Target:    parts[1],
		Poller:    parts[2],
	}, nil
}

// MustParsePollerKey parses a key string, panics on error.
func MustParsePollerKey(s string) PollerKey {
	k, err := ParsePollerKey(s)
	if err != nil {
		panic(err)
	}
	return k
}

// ============================================================================
// CachedPollerKey - Für Hot Paths wo derselbe Key oft gebraucht wird
// ============================================================================

// CachedPollerKey ist ein PollerKey mit gecachtem String-Wert.
// Verwenden wenn derselbe Key mehrfach als String benötigt wird.
type CachedPollerKey struct {
	PollerKey
	once     sync.Once
	keyCache string
}

// NewCachedPollerKey erstellt einen CachedPollerKey.
func NewCachedPollerKey(namespace, target, poller string) *CachedPollerKey {
	return &CachedPollerKey{
		PollerKey: PollerKey{
			Namespace: namespace,
			Target:    target,
			Poller:    poller,
		},
	}
}

// String returns the cached string representation.
// Thread-safe und lazy-evaluated.
func (k *CachedPollerKey) String() string {
	k.once.Do(func() {
		k.keyCache = k.PollerKey.String()
	})
	return k.keyCache
}

// Key returns the underlying PollerKey.
func (k *CachedPollerKey) Key() PollerKey {
	return k.PollerKey
}

// ============================================================================
// KeyBuilder - Für Batch-Operationen
// ============================================================================

// KeyBuilder baut mehrere Keys effizient.
// Wiederverwendet internen Buffer.
type KeyBuilder struct {
	buf strings.Builder
}

// NewKeyBuilder erstellt einen neuen KeyBuilder.
func NewKeyBuilder() *KeyBuilder {
	kb := &KeyBuilder{}
	kb.buf.Grow(64) // Typische Key-Länge
	return kb
}

// Build erstellt einen Key-String ohne neue Allokation des Builders.
func (kb *KeyBuilder) Build(namespace, target, poller string) string {
	kb.buf.Reset()
	kb.buf.WriteString(namespace)
	kb.buf.WriteByte('/')
	kb.buf.WriteString(target)
	kb.buf.WriteByte('/')
	kb.buf.WriteString(poller)
	return kb.buf.String()
}

// BuildFromKey erstellt einen Key-String aus einem PollerKey.
func (kb *KeyBuilder) BuildFromKey(k PollerKey) string {
	return kb.Build(k.Namespace, k.Target, k.Poller)
}

// ============================================================================
// Pool für KeyBuilder (für concurrent use)
// ============================================================================

var keyBuilderPool = sync.Pool{
	New: func() interface{} {
		return NewKeyBuilder()
	},
}

// GetKeyBuilder holt einen KeyBuilder aus dem Pool.
func GetKeyBuilder() *KeyBuilder {
	return keyBuilderPool.Get().(*KeyBuilder)
}

// PutKeyBuilder gibt einen KeyBuilder zurück in den Pool.
func PutKeyBuilder(kb *KeyBuilder) {
	keyBuilderPool.Put(kb)
}

// BuildKeyString baut einen Key-String mit Pool-basiertem Builder.
// Convenience-Funktion für einmalige Nutzung.
func BuildKeyString(namespace, target, poller string) string {
	kb := GetKeyBuilder()
	s := kb.Build(namespace, target, poller)
	PutKeyBuilder(kb)
	return s
}

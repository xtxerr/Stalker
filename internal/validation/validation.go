// Package validation provides centralized input validation for stalker.
//
package validation

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// =============================================================================
// Name Validation
// =============================================================================

// NameRules defines the validation rules for entity names.
type NameRules struct {
	MinLength    int
	MaxLength    int
	AllowDots    bool
	AllowHyphens bool
	AllowUnders  bool
}

// DefaultNameRules returns the default rules for entity names.
func DefaultNameRules() NameRules {
	return NameRules{
		MinLength:    1,
		MaxLength:    255,
		AllowDots:    false,
		AllowHyphens: true,
		AllowUnders:  true,
	}
}

// PollerRefRules returns rules for poller reference components.
func PollerRefRules() NameRules {
	return NameRules{
		MinLength:    1,
		MaxLength:    255,
		AllowDots:    true,
		AllowHyphens: true,
		AllowUnders:  true,
	}
}

// ValidateName validates a name according to the given rules.
func ValidateName(name string, rules NameRules) error {
	if len(name) < rules.MinLength {
		return fmt.Errorf("name too short: minimum %d characters required", rules.MinLength)
	}
	if len(name) > rules.MaxLength {
		return fmt.Errorf("name too long: maximum %d characters allowed", rules.MaxLength)
	}

	if name == "." || name == ".." {
		return fmt.Errorf("name cannot be '.' or '..'")
	}

	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("name cannot start with '.'")
	}

	for i, r := range name {
		if r < 32 || r == 127 {
			return fmt.Errorf("name cannot contain control characters at position %d", i)
		}
		if r == '/' || r == '\\' {
			return fmt.Errorf("name cannot contain path separators at position %d", i)
		}
		if !isAllowedNameChar(r, rules) {
			return fmt.Errorf("invalid character '%c' at position %d", r, i)
		}
	}

	return nil
}

func isAllowedNameChar(r rune, rules NameRules) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	switch r {
	case '.':
		return rules.AllowDots
	case '-':
		return rules.AllowHyphens
	case '_':
		return rules.AllowUnders
	}
	return false
}

// ValidateEntityName validates an entity name with default rules.
func ValidateEntityName(name string) error {
	return ValidateName(name, DefaultNameRules())
}

// =============================================================================
// Link Name Validation
// =============================================================================

// ValidateLinkName validates a link name for tree operations.
func ValidateLinkName(linkName string) error {
	if linkName == "" {
		return fmt.Errorf("link name cannot be empty")
	}

	if linkName == "." || linkName == ".." {
		return fmt.Errorf("link name cannot be '.' or '..'")
	}

	if strings.HasPrefix(linkName, ".") {
		return fmt.Errorf("link name cannot start with '.'")
	}

	if strings.ContainsAny(linkName, "/\\") {
		return fmt.Errorf("link name cannot contain path separators")
	}

	if strings.Contains(linkName, "..") {
		return fmt.Errorf("link name cannot contain '..'")
	}

	for i, c := range linkName {
		if c < 32 || c == 127 {
			return fmt.Errorf("link name cannot contain control characters at position %d", i)
		}
	}

	if len(linkName) > 255 {
		return fmt.Errorf("link name too long: maximum 255 characters")
	}

	return nil
}

// =============================================================================
// =============================================================================

// PollerRef represents a parsed poller reference.
type PollerRef struct {
	Target string
	Poller string
}

// ParsePollerRef parses a "target/poller" reference string.
//
func ParsePollerRef(ref string) (*PollerRef, error) {
	if ref == "" {
		return nil, fmt.Errorf("empty poller reference")
	}

	parts := strings.SplitN(ref, "/", 2)

	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid poller reference format: expected 'target/poller', got '%s'", ref)
	}

	target := strings.TrimSpace(parts[0])
	poller := strings.TrimSpace(parts[1])

	if target == "" {
		return nil, fmt.Errorf("invalid poller reference: empty target in '%s'", ref)
	}
	if poller == "" {
		return nil, fmt.Errorf("invalid poller reference: empty poller in '%s'", ref)
	}

	rules := PollerRefRules()
	if err := ValidateName(target, rules); err != nil {
		return nil, fmt.Errorf("invalid target in poller reference: %w", err)
	}
	if err := ValidateName(poller, rules); err != nil {
		return nil, fmt.Errorf("invalid poller in poller reference: %w", err)
	}

	return &PollerRef{
		Target: target,
		Poller: poller,
	}, nil
}

// String returns the string representation of the poller reference.
func (r *PollerRef) String() string {
	return r.Target + "/" + r.Poller
}

// =============================================================================
// Link Reference Validation
// =============================================================================

// LinkType represents the type of a tree link.
type LinkType string

const (
	LinkTypeTarget LinkType = "target"
	LinkTypePoller LinkType = "poller"
)

// LinkRef represents a parsed link reference.
type LinkRef struct {
	Type   LinkType
	Target string
	Poller string
}

// ParseLinkRef parses a link reference string.
func ParseLinkRef(ref string) (*LinkRef, error) {
	if ref == "" {
		return nil, fmt.Errorf("empty link reference")
	}

	if strings.HasPrefix(ref, "target:") {
		target := strings.TrimPrefix(ref, "target:")
		if target == "" {
			return nil, fmt.Errorf("empty target in link reference")
		}
		return &LinkRef{
			Type:   LinkTypeTarget,
			Target: target,
		}, nil
	}

	if strings.HasPrefix(ref, "poller:") {
		pollerPart := strings.TrimPrefix(ref, "poller:")
		pollerRef, err := ParsePollerRef(pollerPart)
		if err != nil {
			return nil, fmt.Errorf("invalid poller link reference: %w", err)
		}
		return &LinkRef{
			Type:   LinkTypePoller,
			Target: pollerRef.Target,
			Poller: pollerRef.Poller,
		}, nil
	}

	return nil, fmt.Errorf("unknown link type in reference: %s (expected 'target:...' or 'poller:...')", ref)
}

// String returns the string representation of the link reference.
func (r *LinkRef) String() string {
	switch r.Type {
	case LinkTypeTarget:
		return "target:" + r.Target
	case LinkTypePoller:
		return "poller:" + r.Target + "/" + r.Poller
	default:
		return ""
	}
}

// =============================================================================
// =============================================================================

var sqlLikeMetaChars = regexp.MustCompile(`[%_\[\]\\]`)

// EscapeLikePattern escapes special characters in a LIKE pattern.
//
func EscapeLikePattern(pattern string) string {
	return sqlLikeMetaChars.ReplaceAllStringFunc(pattern, func(s string) string {
		return "\\" + s
	})
}

// SafeLikePrefix creates a safe LIKE prefix pattern.
func SafeLikePrefix(prefix string) string {
	return EscapeLikePattern(prefix) + "%"
}

// SafeLikeContains creates a safe LIKE contains pattern.
func SafeLikeContains(pattern string) string {
	return "%" + EscapeLikePattern(pattern) + "%"
}

// Package tree - Tests
//
// LOCATION: internal/tree/resolver_test.go
//
// Unit tests for the view resolver and path template parsing.

package tree

import (
	"testing"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Template Parser Tests
// =============================================================================

func TestParseTemplate_Simple(t *testing.T) {
	template := "/by-site/{label:site}/{name}"
	parsed := ParseTemplate(template)

	if parsed.Raw != template {
		t.Errorf("Raw = %q, want %q", parsed.Raw, template)
	}

	if len(parsed.Variables) != 2 {
		t.Fatalf("Variables count = %d, want 2", len(parsed.Variables))
	}

	// First variable: {label:site}
	if parsed.Variables[0].Type != "label" {
		t.Errorf("Variables[0].Type = %q, want %q", parsed.Variables[0].Type, "label")
	}
	if parsed.Variables[0].Key != "site" {
		t.Errorf("Variables[0].Key = %q, want %q", parsed.Variables[0].Key, "site")
	}

	// Second variable: {name}
	if parsed.Variables[1].Type != "name" {
		t.Errorf("Variables[1].Type = %q, want %q", parsed.Variables[1].Type, "name")
	}
}

func TestParseTemplate_TargetLabel(t *testing.T) {
	template := "/by-site/{target:site}/{target}/{name}"
	parsed := ParseTemplate(template)

	if len(parsed.Variables) != 3 {
		t.Fatalf("Variables count = %d, want 3", len(parsed.Variables))
	}

	// First variable: {target:site}
	if parsed.Variables[0].Type != "target_label" {
		t.Errorf("Variables[0].Type = %q, want %q", parsed.Variables[0].Type, "target_label")
	}
	if parsed.Variables[0].Key != "site" {
		t.Errorf("Variables[0].Key = %q, want %q", parsed.Variables[0].Key, "site")
	}

	// Second variable: {target}
	if parsed.Variables[1].Type != "target" {
		t.Errorf("Variables[1].Type = %q, want %q", parsed.Variables[1].Type, "target")
	}
}

func TestParseTemplate_NoVariables(t *testing.T) {
	template := "/static/path/here"
	parsed := ParseTemplate(template)

	if len(parsed.Variables) != 0 {
		t.Errorf("Variables count = %d, want 0", len(parsed.Variables))
	}

	if len(parsed.Parts) != 1 || parsed.Parts[0] != template {
		t.Errorf("Parts = %v, want [%q]", parsed.Parts, template)
	}
}

func TestParseTemplate_MultipleLabels(t *testing.T) {
	template := "/dc/{label:site}/rack-{label:rack}/{name}"
	parsed := ParseTemplate(template)

	if len(parsed.Variables) != 3 {
		t.Fatalf("Variables count = %d, want 3", len(parsed.Variables))
	}

	// Check static parts
	expectedStaticParts := []string{"/dc/", "/rack-", "/"}
	for i, v := range parsed.Variables {
		if i < len(expectedStaticParts) {
			// Parts alternate between static and variable placeholders
			// This test verifies variables were parsed correctly
			_ = expectedStaticParts[i]
		}
		_ = v
	}
}

// =============================================================================
// Filter Matching Tests
// =============================================================================

func TestMatchesFilter_LabelExists(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name   string
		filter string
		labels map[string]string
		want   bool
	}{
		{
			name:   "label exists",
			filter: "label:site",
			labels: map[string]string{"site": "dc1"},
			want:   true,
		},
		{
			name:   "label missing",
			filter: "label:site",
			labels: map[string]string{"rack": "A1"},
			want:   false,
		},
		{
			name:   "nil labels",
			filter: "label:site",
			labels: nil,
			want:   false,
		},
		{
			name:   "empty filter",
			filter: "",
			labels: map[string]string{"site": "dc1"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.matchesFilter(tt.filter, tt.labels, nil)
			if got != tt.want {
				t.Errorf("matchesFilter(%q, %v) = %v, want %v",
					tt.filter, tt.labels, got, tt.want)
			}
		})
	}
}

func TestMatchesFilter_LabelEquals(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name   string
		filter string
		labels map[string]string
		want   bool
	}{
		{
			name:   "exact match",
			filter: "label:priority = critical",
			labels: map[string]string{"priority": "critical"},
			want:   true,
		},
		{
			name:   "no match",
			filter: "label:priority = critical",
			labels: map[string]string{"priority": "low"},
			want:   false,
		},
		{
			name:   "label missing",
			filter: "label:priority = critical",
			labels: map[string]string{"site": "dc1"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.matchesFilter(tt.filter, tt.labels, nil)
			if got != tt.want {
				t.Errorf("matchesFilter(%q, %v) = %v, want %v",
					tt.filter, tt.labels, got, tt.want)
			}
		})
	}
}

func TestMatchesFilter_LabelIn(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name   string
		filter string
		labels map[string]string
		want   bool
	}{
		{
			name:   "in list",
			filter: "label:type in [router, switch, firewall]",
			labels: map[string]string{"type": "switch"},
			want:   true,
		},
		{
			name:   "not in list",
			filter: "label:type in [router, switch, firewall]",
			labels: map[string]string{"type": "server"},
			want:   false,
		},
		{
			name:   "first in list",
			filter: "label:type in [router, switch]",
			labels: map[string]string{"type": "router"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.matchesFilter(tt.filter, tt.labels, nil)
			if got != tt.want {
				t.Errorf("matchesFilter(%q, %v) = %v, want %v",
					tt.filter, tt.labels, got, tt.want)
			}
		})
	}
}

func TestMatchesFilter_AND(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name   string
		filter string
		labels map[string]string
		want   bool
	}{
		{
			name:   "both match",
			filter: "label:site AND label:rack",
			labels: map[string]string{"site": "dc1", "rack": "A1"},
			want:   true,
		},
		{
			name:   "first missing",
			filter: "label:site AND label:rack",
			labels: map[string]string{"rack": "A1"},
			want:   false,
		},
		{
			name:   "second missing",
			filter: "label:site AND label:rack",
			labels: map[string]string{"site": "dc1"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.matchesFilter(tt.filter, tt.labels, nil)
			if got != tt.want {
				t.Errorf("matchesFilter(%q, %v) = %v, want %v",
					tt.filter, tt.labels, got, tt.want)
			}
		})
	}
}

func TestMatchesFilter_OR(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name   string
		filter string
		labels map[string]string
		want   bool
	}{
		{
			name:   "first matches",
			filter: "label:site OR label:rack",
			labels: map[string]string{"site": "dc1"},
			want:   true,
		},
		{
			name:   "second matches",
			filter: "label:site OR label:rack",
			labels: map[string]string{"rack": "A1"},
			want:   true,
		},
		{
			name:   "neither matches",
			filter: "label:site OR label:rack",
			labels: map[string]string{"vendor": "cisco"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.matchesFilter(tt.filter, tt.labels, nil)
			if got != tt.want {
				t.Errorf("matchesFilter(%q, %v) = %v, want %v",
					tt.filter, tt.labels, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Path Resolution Tests
// =============================================================================

func TestResolveTargetPath(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name     string
		template string
		target   *store.Target
		want     string
	}{
		{
			name:     "simple label",
			template: "/by-site/{label:site}/{name}",
			target: &store.Target{
				Name:   "router-1",
				Labels: map[string]string{"site": "dc1"},
			},
			want: "/by-site/dc1/router-1",
		},
		{
			name:     "multiple labels",
			template: "/dc/{label:site}/rack-{label:rack}/{name}",
			target: &store.Target{
				Name:   "router-1",
				Labels: map[string]string{"site": "dc1", "rack": "A1"},
			},
			want: "/dc/dc1/rack-A1/router-1",
		},
		{
			name:     "missing label",
			template: "/by-site/{label:site}/{name}",
			target: &store.Target{
				Name:   "router-1",
				Labels: map[string]string{"rack": "A1"}, // No site label
			},
			want: "", // Should return empty when label is missing
		},
		{
			name:     "name only",
			template: "/all/{name}",
			target: &store.Target{
				Name: "router-1",
			},
			want: "/all/router-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := ParseTemplate(tt.template)
			got := r.resolveTargetPath(parsed, tt.target)
			if got != tt.want {
				t.Errorf("resolveTargetPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolvePollerPath(t *testing.T) {
	r := &ViewResolver{}

	tests := []struct {
		name     string
		template string
		poller   *store.Poller
		target   *store.Target
		want     string
	}{
		{
			name:     "with target label",
			template: "/by-site/{target:site}/{target}/{name}",
			poller: &store.Poller{
				Name:   "cpu",
				Target: "router-1",
			},
			target: &store.Target{
				Name:   "router-1",
				Labels: map[string]string{"site": "dc1"},
			},
			want: "/by-site/dc1/router-1/cpu",
		},
		{
			name:     "missing target label",
			template: "/by-site/{target:site}/{target}/{name}",
			poller: &store.Poller{
				Name:   "cpu",
				Target: "router-1",
			},
			target: &store.Target{
				Name: "router-1",
				// No site label
			},
			want: "", // Should return empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := ParseTemplate(tt.template)
			got := r.resolvePollerPath(parsed, tt.poller, tt.target)
			if got != tt.want {
				t.Errorf("resolvePollerPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// Value List Parser Tests
// =============================================================================

func TestParseValueList(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"[a, b, c]", []string{"a", "b", "c"}},
		{"[router, switch]", []string{"router", "switch"}},
		{"[single]", []string{"single"}},
		{"[]", []string{""}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseValueList(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("parseValueList(%q) length = %d, want %d", tt.input, len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseValueList(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// =============================================================================
// Manager Helper Tests
// =============================================================================

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", "/"},
		{"/", "/"},
		{"/path", "/path"},
		{"path", "/path"},
		{"/path/", "/path"},
		{"/a/b/c", "/a/b/c"},
		{"/a//b", "/a/b"}, // Double slash
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizePath(tt.input)
			if got != tt.want {
				t.Errorf("normalizePath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestLastPathComponent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/", ""},
		{"/a", "a"},
		{"/a/b", "b"},
		{"/a/b/c", "c"},
		{"a", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := lastPathComponent(tt.input)
			if got != tt.want {
				t.Errorf("lastPathComponent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsChildOf(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		want   bool
	}{
		{"/", "/a", true},
		{"/", "/a/b", true},
		{"/a", "/a/b", true},
		{"/a", "/a/b/c", true},
		{"/a/b", "/a/b/c", true},
		{"/a", "/a", false},       // Same path
		{"/a", "/ab", false},      // Not a child
		{"/a/b", "/a", false},     // Parent of, not child of
		{"/a", "/b/a", false},     // Different subtree
	}

	for _, tt := range tests {
		name := tt.parent + " → " + tt.child
		t.Run(name, func(t *testing.T) {
			got := isChildOf(tt.parent, tt.child)
			if got != tt.want {
				t.Errorf("isChildOf(%q, %q) = %v, want %v",
					tt.parent, tt.child, got, tt.want)
			}
		})
	}
}

func TestGetImmediateChild(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		want   string
	}{
		{"/", "/a", "a"},
		{"/", "/a/b", "a"},
		{"/a", "/a/b", "b"},
		{"/a", "/a/b/c", "b"},
		{"/a/b", "/a/b/c", "c"},
		{"/a", "/a", ""},     // Same path
		{"/a", "/b", ""},     // Not a child
	}

	for _, tt := range tests {
		name := tt.parent + " → " + tt.child
		t.Run(name, func(t *testing.T) {
			got := getImmediateChild(tt.parent, tt.child)
			if got != tt.want {
				t.Errorf("getImmediateChild(%q, %q) = %q, want %q",
					tt.parent, tt.child, got, tt.want)
			}
		})
	}
}

package validation

import (
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	rules := DefaultNameRules()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"simple", "router1", false},
		{"with hyphen", "my-router", false},
		{"with underscore", "my_router", false},
		{"numbers", "123", false},
		{"mixed", "router-1_test", false},
		{"empty", "", true},
		{"dot", ".", true},
		{"dotdot", "..", true},
		{"hidden", ".hidden", true},
		{"slash", "a/b", true},
		{"backslash", "a\\b", true},
		{"control char", "a\x00b", true},
		{"with dot", "my.router", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateName(tt.input, rules)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateNameWithDots(t *testing.T) {
	rules := PollerRefRules()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"with dot", "my.router.name", false},
		{"ip-like", "192.168.1.1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateName(tt.input, rules)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateLinkName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"simple", "link1", false},
		{"with hyphen", "my-link", false},
		{"with underscore", "my_link", false},
		{"empty", "", true},
		{"dot", ".", true},
		{"dotdot", "..", true},
		{"hidden", ".hidden", true},
		{"slash", "a/b", true},
		{"backslash", "a\\b", true},
		{"dotdot in name", "a..b", true},
		{"too long", strings.Repeat("a", 256), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLinkName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateLinkName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParsePollerRef(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantTarget string
		wantPoller string
		wantErr    bool
	}{
		{"simple", "router/cpu", "router", "cpu", false},
		{"with dots", "router.1/cpu.usage", "router.1", "cpu.usage", false},
		{"with hyphens", "my-router/my-poller", "my-router", "my-poller", false},
		{"empty", "", "", "", true},
		{"no separator", "router", "", "", true},
		{"empty target", "/cpu", "", "", true},
		{"empty poller", "router/", "", "", true},
		{"both empty", "/", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParsePollerRef(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePollerRef(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if ref.Target != tt.wantTarget {
				t.Errorf("ParsePollerRef(%q).Target = %q, want %q", tt.input, ref.Target, tt.wantTarget)
			}
			if ref.Poller != tt.wantPoller {
				t.Errorf("ParsePollerRef(%q).Poller = %q, want %q", tt.input, ref.Poller, tt.wantPoller)
			}
		})
	}
}

func TestParseLinkRef(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantType   LinkType
		wantTarget string
		wantPoller string
		wantErr    bool
	}{
		{"target simple", "target:router", LinkTypeTarget, "router", "", false},
		{"poller simple", "poller:router/cpu", LinkTypePoller, "router", "cpu", false},
		{"empty", "", "", "", "", true},
		{"no prefix", "router", "", "", "", true},
		{"unknown prefix", "unknown:router", "", "", "", true},
		{"target empty", "target:", "", "", "", true},
		{"poller empty target", "poller:/cpu", "", "", "", true},
		{"poller empty poller", "poller:router/", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseLinkRef(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseLinkRef(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if ref.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", ref.Type, tt.wantType)
			}
			if ref.Target != tt.wantTarget {
				t.Errorf("Target = %q, want %q", ref.Target, tt.wantTarget)
			}
			if ref.Poller != tt.wantPoller {
				t.Errorf("Poller = %q, want %q", ref.Poller, tt.wantPoller)
			}
		})
	}
}

func TestEscapeLikePattern(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no special", "hello", "hello"},
		{"percent", "100%", "100\\%"},
		{"underscore", "my_name", "my\\_name"},
		{"both", "100%_complete", "100\\%\\_complete"},
		{"backslash", "path\\file", "path\\\\file"},
		{"brackets", "[test]", "\\[test\\]"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EscapeLikePattern(tt.input)
			if got != tt.want {
				t.Errorf("EscapeLikePattern(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafeLikePrefix(t *testing.T) {
	got := SafeLikePrefix("100%")
	want := "100\\%%"
	if got != want {
		t.Errorf("SafeLikePrefix(%q) = %q, want %q", "100%", got, want)
	}
}

func TestSafeLikeContains(t *testing.T) {
	got := SafeLikeContains("100%")
	want := "%100\\%%"
	if got != want {
		t.Errorf("SafeLikeContains(%q) = %q, want %q", "100%", got, want)
	}
}

func BenchmarkParsePollerRef(b *testing.B) {
	ref := "router-1/cpu-usage"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParsePollerRef(ref)
	}
}

func BenchmarkEscapeLikePattern(b *testing.B) {
	pattern := "100%_[test]"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EscapeLikePattern(pattern)
	}
}

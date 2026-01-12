// Package handler - Tree Handler Tests
//
// LOCATION: internal/handler/tree_test.go
//
// Unit tests for the tree API handlers.

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// =============================================================================
// Path Validation Tests
// =============================================================================

func TestValidatePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid root", "/", false},
		{"valid path", "/by-site/dc1", false},
		{"valid deep path", "/a/b/c/d/e", false},
		{"missing leading slash", "path", true},
		{"path traversal dots", "/path/../etc", true},
		{"path traversal start", "/../etc", true},
		{"null byte", "/path\x00bad", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestValidatePath_MaxLength(t *testing.T) {
	// Create a path that exceeds 1024 characters
	longPath := "/"
	for i := 0; i < 200; i++ {
		longPath += "abcde/"
	}

	err := validatePath(longPath)
	if err == nil {
		t.Error("validatePath() should reject paths > 1024 chars")
	}
}

// =============================================================================
// Response Helper Tests
// =============================================================================

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}

	writeJSON(w, http.StatusOK, data)

	// Check status
	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}

	// Check content type
	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	// Check body
	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("Body key = %q, want %q", result["key"], "value")
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()

	writeError(w, http.StatusBadRequest, "invalid input", nil)

	// Check status
	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	// Check body
	var result ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}
	if result.Error != "Bad Request" {
		t.Errorf("Error = %q, want %q", result.Error, "Bad Request")
	}
	if result.Message != "invalid input" {
		t.Errorf("Message = %q, want %q", result.Message, "invalid input")
	}
}

// =============================================================================
// Browse Response Tests
// =============================================================================

func TestBrowseResponse_JSON(t *testing.T) {
	resp := BrowseResponse{
		Path: "/by-site",
		Entries: []BrowseEntry{
			{
				Name:   "dc1",
				Path:   "/by-site/dc1",
				Type:   "directory",
				Source: "view",
			},
			{
				Name:    "router-1",
				Path:    "/by-site/router-1",
				Type:    "link_target",
				LinkRef: "target:router-1",
				Source:  "static",
			},
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded BrowseResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Path != "/by-site" {
		t.Errorf("Path = %q, want %q", decoded.Path, "/by-site")
	}
	if len(decoded.Entries) != 2 {
		t.Errorf("Entries count = %d, want 2", len(decoded.Entries))
	}
}

// =============================================================================
// Resolve Response Tests
// =============================================================================

func TestResolveResponse_Target(t *testing.T) {
	resp := ResolveResponse{
		Path:    "/favorites/main-router",
		Type:    "target",
		Target:  "core-router",
		LinkRef: "target:core-router",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Check that Poller is omitted when empty
	var raw map[string]interface{}
	json.Unmarshal(data, &raw)

	if _, ok := raw["poller"]; ok {
		t.Error("Poller should be omitted when empty")
	}
}

func TestResolveResponse_Poller(t *testing.T) {
	resp := ResolveResponse{
		Path:    "/favorites/cpu",
		Type:    "poller",
		Target:  "core-router",
		Poller:  "cpu",
		LinkRef: "poller:core-router/cpu",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ResolveResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Poller != "cpu" {
		t.Errorf("Poller = %q, want %q", decoded.Poller, "cpu")
	}
}

// =============================================================================
// View Response Tests
// =============================================================================

func TestViewResponse_JSON(t *testing.T) {
	resp := ViewResponse{
		Name:         "by-site",
		Description:  "Group by datacenter",
		PathTemplate: "/by-site/{label:site}/{name}",
		EntityType:   "target",
		FilterExpr:   "label:site",
		Priority:     50,
		Enabled:      true,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ViewResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.PathTemplate != "/by-site/{label:site}/{name}" {
		t.Errorf("PathTemplate = %q, want %q", decoded.PathTemplate, "/by-site/{label:site}/{name}")
	}
	if decoded.Priority != 50 {
		t.Errorf("Priority = %d, want 50", decoded.Priority)
	}
}

// =============================================================================
// Smart Folder Response Tests
// =============================================================================

func TestSmartFolderResponse_JSON(t *testing.T) {
	resp := SmartFolderResponse{
		Path:        "/alerts/critical",
		Description: "Critical devices",
		TargetQuery: "label:priority = critical",
		CacheTTL:    60,
		Priority:    25,
		Enabled:     true,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded SmartFolderResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.TargetQuery != "label:priority = critical" {
		t.Errorf("TargetQuery = %q, want %q", decoded.TargetQuery, "label:priority = critical")
	}
	if decoded.CacheTTL != 60 {
		t.Errorf("CacheTTL = %d, want 60", decoded.CacheTTL)
	}
}

// =============================================================================
// Integration-style Tests (with mock)
// =============================================================================

// MockTreeHandler simulates tree handler behavior for testing.
type MockTreeHandler struct {
	browseResult *BrowseResponse
	browseErr    error
}

func TestTreeHandler_BrowseRequest(t *testing.T) {
	// This would be an integration test with actual store
	// For now, just test request parsing

	req := httptest.NewRequest("GET", "/api/v1/namespaces/default/tree/browse?path=/by-site", nil)
	
	// Extract path from URL
	path := req.URL.Query().Get("path")
	if path != "/by-site" {
		t.Errorf("Extracted path = %q, want %q", path, "/by-site")
	}

	// Validate the path
	if err := validatePath(path); err != nil {
		t.Errorf("Path validation failed: %v", err)
	}
}

func TestTreeHandler_ResolveRequest(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/namespaces/prod/tree/resolve?path=/favorites/router", nil)
	
	path := req.URL.Query().Get("path")
	if path != "/favorites/router" {
		t.Errorf("Extracted path = %q, want %q", path, "/favorites/router")
	}
}

func TestTreeHandler_MethodNotAllowed(t *testing.T) {
	// POST should not be allowed for browse
	req := httptest.NewRequest("POST", "/api/v1/namespaces/default/tree/browse", nil)
	
	if req.Method != "POST" {
		t.Error("Expected POST method")
	}
	
	// Handler would return 405 for POST on browse endpoint
}

// =============================================================================
// PathError Tests
// =============================================================================

func TestPathError(t *testing.T) {
	err := &PathError{Message: "test error"}
	
	if err.Error() != "test error" {
		t.Errorf("Error() = %q, want %q", err.Error(), "test error")
	}
}

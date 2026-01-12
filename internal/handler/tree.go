// Package handler - Tree API Handlers
//
// LOCATION: internal/handler/tree.go
//
// Provides HTTP handlers for virtual filesystem operations.
//
// Endpoints:
//   GET /api/v1/namespaces/{ns}/tree/browse?path=/...
//   GET /api/v1/namespaces/{ns}/tree/resolve?path=/...
//   GET /api/v1/namespaces/{ns}/tree/views
//   GET /api/v1/namespaces/{ns}/tree/smart

package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/xtxerr/stalker/internal/store"
	"github.com/xtxerr/stalker/internal/tree"
)

// =============================================================================
// Tree Handler
// =============================================================================

// TreeHandler handles tree API requests.
type TreeHandler struct {
	store   *store.Store
	treeMgr *tree.Manager
}

// NewTreeHandler creates a new tree handler.
func NewTreeHandler(st *store.Store, treeMgr *tree.Manager) *TreeHandler {
	return &TreeHandler{
		store:   st,
		treeMgr: treeMgr,
	}
}

// RegisterRoutes registers tree API routes.
//
// With a standard mux:
//   mux.HandleFunc("/api/v1/namespaces/", h.ServeHTTP)
//
// This handler expects URLs like:
//   /api/v1/namespaces/{namespace}/tree/browse?path=/...
//   /api/v1/namespaces/{namespace}/tree/resolve?path=/...
func (h *TreeHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/namespaces/", h.ServeHTTP)
}

// ServeHTTP handles HTTP requests.
func (h *TreeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/v1/namespaces/{ns}/tree/{action}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/")
	parts := strings.SplitN(path, "/", 3)

	if len(parts) < 3 || parts[1] != "tree" {
		http.NotFound(w, r)
		return
	}

	namespace := parts[0]
	action := parts[2]

	// Validate namespace exists
	exists, err := h.store.NamespaceExists(namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error", err)
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "namespace not found", nil)
		return
	}

	switch action {
	case "browse":
		h.handleBrowse(w, r, namespace)
	case "resolve":
		h.handleResolve(w, r, namespace)
	case "views":
		h.handleViews(w, r, namespace)
	case "smart":
		h.handleSmart(w, r, namespace)
	default:
		http.NotFound(w, r)
	}
}

// =============================================================================
// Browse Handler
// =============================================================================

// BrowseResponse is the response for browse requests.
type BrowseResponse struct {
	Path    string         `json:"path"`
	Entries []BrowseEntry  `json:"entries"`
}

// BrowseEntry represents a single entry in browse response.
type BrowseEntry struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	LinkRef     string `json:"link_ref,omitempty"`
	Description string `json:"description,omitempty"`
	Source      string `json:"source"`
	Priority    int    `json:"priority,omitempty"`
}

func (h *TreeHandler) handleBrowse(w http.ResponseWriter, r *http.Request, namespace string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		path = "/"
	}

	// Validate path
	if err := validatePath(path); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}

	result, err := h.treeMgr.Browse(r.Context(), namespace, path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "browse failed", err)
		return
	}

	// Convert to response format
	resp := BrowseResponse{
		Path:    result.Path,
		Entries: make([]BrowseEntry, len(result.Entries)),
	}

	for i, e := range result.Entries {
		resp.Entries[i] = BrowseEntry{
			Name:        e.Name,
			Path:        e.Path,
			Type:        e.Type,
			LinkRef:     e.LinkRef,
			Description: e.Description,
			Source:      e.Source,
			Priority:    e.Priority,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// Resolve Handler
// =============================================================================

// ResolveResponse is the response for resolve requests.
type ResolveResponse struct {
	Path    string `json:"path"`
	Type    string `json:"type"`
	Target  string `json:"target"`
	Poller  string `json:"poller,omitempty"`
	LinkRef string `json:"link_ref"`
}

func (h *TreeHandler) handleResolve(w http.ResponseWriter, r *http.Request, namespace string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "path is required", nil)
		return
	}

	// Validate path
	if err := validatePath(path); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}

	result, err := h.treeMgr.Resolve(r.Context(), namespace, path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "resolve failed", err)
		return
	}

	if result == nil {
		writeError(w, http.StatusNotFound, "path not found or is a directory", nil)
		return
	}

	resp := ResolveResponse{
		Path:    path,
		Type:    result.Type,
		Target:  result.Target,
		Poller:  result.Poller,
		LinkRef: result.LinkRef,
	}

	writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// Views Handler
// =============================================================================

// ViewResponse represents a view in the response.
type ViewResponse struct {
	Name         string `json:"name"`
	Description  string `json:"description,omitempty"`
	PathTemplate string `json:"path_template"`
	EntityType   string `json:"entity_type"`
	FilterExpr   string `json:"filter_expr,omitempty"`
	Priority     int    `json:"priority"`
	Enabled      bool   `json:"enabled"`
}

func (h *TreeHandler) handleViews(w http.ResponseWriter, r *http.Request, namespace string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}

	views, err := h.store.ListTreeViews(namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list views failed", err)
		return
	}

	resp := make([]ViewResponse, len(views))
	for i, v := range views {
		resp[i] = ViewResponse{
			Name:         v.Name,
			Description:  v.Description,
			PathTemplate: v.PathTemplate,
			EntityType:   v.EntityType,
			FilterExpr:   v.FilterExpr,
			Priority:     v.Priority,
			Enabled:      v.Enabled,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// Smart Folders Handler
// =============================================================================

// SmartFolderResponse represents a smart folder in the response.
type SmartFolderResponse struct {
	Path        string `json:"path"`
	Description string `json:"description,omitempty"`
	TargetQuery string `json:"target_query,omitempty"`
	PollerQuery string `json:"poller_query,omitempty"`
	CacheTTL    int    `json:"cache_ttl_sec"`
	Priority    int    `json:"priority"`
	Enabled     bool   `json:"enabled"`
}

func (h *TreeHandler) handleSmart(w http.ResponseWriter, r *http.Request, namespace string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}

	folders, err := h.store.ListSmartFolders(namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list smart folders failed", err)
		return
	}

	resp := make([]SmartFolderResponse, len(folders))
	for i, f := range folders {
		resp[i] = SmartFolderResponse{
			Path:        f.Path,
			Description: f.Description,
			TargetQuery: f.TargetQuery,
			PollerQuery: f.PollerQuery,
			CacheTTL:    f.CacheTTL,
			Priority:    f.Priority,
			Enabled:     f.Enabled,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// =============================================================================
// Path Validation (Security)
// =============================================================================

// validatePath validates a tree path for security.
func validatePath(path string) error {
	// Must start with /
	if !strings.HasPrefix(path, "/") {
		return &PathError{"path must start with /"}
	}

	// Check for path traversal
	if strings.Contains(path, "..") {
		return &PathError{"path traversal not allowed"}
	}

	// Check for null bytes
	if strings.ContainsRune(path, 0) {
		return &PathError{"null bytes not allowed"}
	}

	// Check maximum length
	if len(path) > 1024 {
		return &PathError{"path too long"}
	}

	return nil
}

// PathError represents a path validation error.
type PathError struct {
	Message string
}

func (e *PathError) Error() string {
	return e.Message
}

// =============================================================================
// Response Helpers
// =============================================================================

// ErrorResponse is the standard error response format.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string, err error) {
	resp := ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	}

	// Don't expose internal errors to client
	// In production, log err internally

	writeJSON(w, status, resp)
}

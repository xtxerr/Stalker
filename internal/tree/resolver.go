// Package tree provides the virtual filesystem implementation.
//
// LOCATION: internal/tree/resolver.go
//
// The resolver generates virtual paths from label-based views and queries
// smart folders to provide a unified view of the tree.

package tree

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Path Template Parser
// =============================================================================

// TemplateVar represents a variable in a path template.
type TemplateVar struct {
	Full   string // e.g., "{label:site}"
	Type   string // "label", "target", "name"
	Key    string // e.g., "site" for {label:site}
	Offset int    // Position in template
}

// ParsedTemplate is a pre-parsed path template.
type ParsedTemplate struct {
	Raw       string
	Parts     []string      // Static parts between variables
	Variables []TemplateVar // Variables in order
}

var templateVarRegex = regexp.MustCompile(`\{([^}]+)\}`)

// ParseTemplate parses a path template into its components.
//
// Supported variables:
//   - {name}         : Entity name
//   - {target}       : Parent target name (for pollers)
//   - {label:KEY}    : Value of label KEY
//   - {target:KEY}   : Parent target's label (for pollers)
func ParseTemplate(template string) *ParsedTemplate {
	matches := templateVarRegex.FindAllStringSubmatchIndex(template, -1)

	parsed := &ParsedTemplate{
		Raw:   template,
		Parts: make([]string, 0),
	}

	lastEnd := 0
	for _, match := range matches {
		// Static part before this variable
		if match[0] > lastEnd {
			parsed.Parts = append(parsed.Parts, template[lastEnd:match[0]])
		}

		// Parse variable
		varContent := template[match[2]:match[3]]
		v := TemplateVar{
			Full:   template[match[0]:match[1]],
			Offset: match[0],
		}

		if varContent == "name" {
			v.Type = "name"
		} else if varContent == "target" {
			v.Type = "target"
		} else if strings.HasPrefix(varContent, "label:") {
			v.Type = "label"
			v.Key = varContent[6:]
		} else if strings.HasPrefix(varContent, "target:") {
			v.Type = "target_label"
			v.Key = varContent[7:]
		}

		parsed.Variables = append(parsed.Variables, v)
		parsed.Parts = append(parsed.Parts, "") // Placeholder for variable value
		lastEnd = match[1]
	}

	// Final static part
	if lastEnd < len(template) {
		parsed.Parts = append(parsed.Parts, template[lastEnd:])
	}

	return parsed
}

// =============================================================================
// View Resolver
// =============================================================================

// ViewResolver generates virtual paths for targets/pollers based on views.
type ViewResolver struct {
	store *store.Store
	views map[string][]*store.TreeView // namespace → views

	mu sync.RWMutex

	// Parsed templates cache
	templates map[string]*ParsedTemplate // view key → parsed template
}

// NewViewResolver creates a new view resolver.
func NewViewResolver(s *store.Store) *ViewResolver {
	return &ViewResolver{
		store:     s,
		views:     make(map[string][]*store.TreeView),
		templates: make(map[string]*ParsedTemplate),
	}
}

// LoadViews loads all enabled views from the database.
func (r *ViewResolver) LoadViews(namespace string) error {
	views, err := r.store.ListEnabledTreeViews(namespace)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.views[namespace] = views

	// Pre-parse templates
	for _, v := range views {
		key := namespace + "/" + v.Name
		r.templates[key] = ParseTemplate(v.PathTemplate)
	}

	return nil
}

// ResolveTarget generates all virtual paths for a target based on views.
func (r *ViewResolver) ResolveTarget(target *store.Target) []ResolvedPath {
	r.mu.RLock()
	views := r.views[target.Namespace]
	r.mu.RUnlock()

	var paths []ResolvedPath

	for _, view := range views {
		if view.EntityType != "target" {
			continue
		}

		// Check filter
		if !r.matchesFilter(view.FilterExpr, target.Labels, nil) {
			continue
		}

		// Resolve template
		key := target.Namespace + "/" + view.Name
		r.mu.RLock()
		template := r.templates[key]
		r.mu.RUnlock()

		if template == nil {
			continue
		}

		path := r.resolveTargetPath(template, target)
		if path == "" {
			continue // Template couldn't be fully resolved
		}

		paths = append(paths, ResolvedPath{
			Path:     path,
			ViewName: view.Name,
			Priority: view.Priority,
			LinkRef:  "target:" + target.Name,
		})
	}

	return paths
}

// ResolvePoller generates all virtual paths for a poller based on views.
func (r *ViewResolver) ResolvePoller(poller *store.Poller, target *store.Target) []ResolvedPath {
	r.mu.RLock()
	views := r.views[poller.Namespace]
	r.mu.RUnlock()

	var paths []ResolvedPath

	for _, view := range views {
		if view.EntityType != "poller" {
			continue
		}

		// Check filter
		if !r.matchesFilter(view.FilterExpr, target.Labels, nil) {
			continue
		}

		// Resolve template
		key := poller.Namespace + "/" + view.Name
		r.mu.RLock()
		template := r.templates[key]
		r.mu.RUnlock()

		if template == nil {
			continue
		}

		path := r.resolvePollerPath(template, poller, target)
		if path == "" {
			continue
		}

		paths = append(paths, ResolvedPath{
			Path:     path,
			ViewName: view.Name,
			Priority: view.Priority,
			LinkRef:  "poller:" + poller.Target + "/" + poller.Name,
		})
	}

	return paths
}

// ResolvedPath represents a resolved virtual path.
type ResolvedPath struct {
	Path     string
	ViewName string
	Priority int
	LinkRef  string
}

// resolveTargetPath applies a template to a target.
func (r *ViewResolver) resolveTargetPath(template *ParsedTemplate, target *store.Target) string {
	var result strings.Builder

	varIdx := 0
	for i, part := range template.Parts {
		result.WriteString(part)

		if varIdx < len(template.Variables) && i < len(template.Parts)-1 {
			v := template.Variables[varIdx]
			value := ""

			switch v.Type {
			case "name":
				value = target.Name
			case "label":
				if target.Labels != nil {
					value = target.Labels[v.Key]
				}
			}

			if value == "" {
				return "" // Template requires value that doesn't exist
			}

			result.WriteString(value)
			varIdx++
		}
	}

	return result.String()
}

// resolvePollerPath applies a template to a poller.
func (r *ViewResolver) resolvePollerPath(template *ParsedTemplate, poller *store.Poller, target *store.Target) string {
	var result strings.Builder

	varIdx := 0
	for i, part := range template.Parts {
		result.WriteString(part)

		if varIdx < len(template.Variables) && i < len(template.Parts)-1 {
			v := template.Variables[varIdx]
			value := ""

			switch v.Type {
			case "name":
				value = poller.Name
			case "target":
				value = poller.Target
			case "label":
				// Poller's own labels (if any)
				value = ""
			case "target_label":
				if target != nil && target.Labels != nil {
					value = target.Labels[v.Key]
				}
			}

			if value == "" {
				return ""
			}

			result.WriteString(value)
			varIdx++
		}
	}

	return result.String()
}

// matchesFilter checks if labels match a filter expression.
//
// Filter syntax:
//   - "label:KEY" - label exists
//   - "label:KEY = VALUE" - label equals value
//   - "label:KEY in [a,b,c]" - label is one of values
//   - Expressions can be combined with AND, OR
func (r *ViewResolver) matchesFilter(filter string, labels, pollerState map[string]string) bool {
	if filter == "" {
		return true
	}

	// Simple implementation for common cases
	filter = strings.TrimSpace(filter)

	// Handle AND
	if strings.Contains(filter, " AND ") {
		parts := strings.Split(filter, " AND ")
		for _, part := range parts {
			if !r.matchesFilter(strings.TrimSpace(part), labels, pollerState) {
				return false
			}
		}
		return true
	}

	// Handle OR
	if strings.Contains(filter, " OR ") {
		parts := strings.Split(filter, " OR ")
		for _, part := range parts {
			if r.matchesFilter(strings.TrimSpace(part), labels, pollerState) {
				return true
			}
		}
		return false
	}

	// Single condition
	return r.matchesSingleCondition(filter, labels, pollerState)
}

func (r *ViewResolver) matchesSingleCondition(cond string, labels, pollerState map[string]string) bool {
	cond = strings.TrimSpace(cond)

	// "label:KEY = VALUE"
	if strings.Contains(cond, " = ") {
		parts := strings.SplitN(cond, " = ", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if strings.HasPrefix(key, "label:") {
			labelKey := key[6:]
			if labels == nil {
				return false
			}
			return labels[labelKey] == value
		}

		if strings.HasPrefix(key, "state") {
			if pollerState == nil {
				return false
			}
			return pollerState["state"] == value
		}
	}

	// "label:KEY in [a,b,c]"
	if strings.Contains(cond, " in ") {
		parts := strings.SplitN(cond, " in ", 2)
		key := strings.TrimSpace(parts[0])
		values := parseValueList(strings.TrimSpace(parts[1]))

		if strings.HasPrefix(key, "label:") {
			labelKey := key[6:]
			if labels == nil {
				return false
			}
			labelValue := labels[labelKey]
			for _, v := range values {
				if labelValue == v {
					return true
				}
			}
			return false
		}
	}

	// "label:KEY" (exists check)
	if strings.HasPrefix(cond, "label:") {
		labelKey := cond[6:]
		if labels == nil {
			return false
		}
		_, exists := labels[labelKey]
		return exists
	}

	// "target:KEY" (exists check for parent target's label)
	if strings.HasPrefix(cond, "target:") {
		labelKey := cond[7:]
		if labels == nil {
			return false
		}
		_, exists := labels[labelKey]
		return exists
	}

	return false
}

// parseValueList parses "[a, b, c]" into ["a", "b", "c"].
func parseValueList(s string) []string {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")

	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		result = append(result, strings.TrimSpace(p))
	}
	return result
}

// =============================================================================
// Smart Folder Resolver
// =============================================================================

// SmartFolderResolver executes smart folder queries.
type SmartFolderResolver struct {
	store *store.Store

	// Cache
	cache    map[string]*smartFolderCache
	cacheMu  sync.RWMutex
}

type smartFolderCache struct {
	entries   []SmartFolderEntry
	expiresAt time.Time
}

// SmartFolderEntry represents an entry in a smart folder.
type SmartFolderEntry struct {
	Name    string
	Type    string // "target" or "poller"
	LinkRef string
}

// NewSmartFolderResolver creates a new smart folder resolver.
func NewSmartFolderResolver(s *store.Store) *SmartFolderResolver {
	return &SmartFolderResolver{
		store: s,
		cache: make(map[string]*smartFolderCache),
	}
}

// Resolve returns the contents of a smart folder.
func (r *SmartFolderResolver) Resolve(folder *store.TreeSmartFolder) ([]SmartFolderEntry, error) {
	key := folder.Namespace + ":" + folder.Path

	// Check cache
	r.cacheMu.RLock()
	cached := r.cache[key]
	r.cacheMu.RUnlock()

	if cached != nil && time.Now().Before(cached.expiresAt) {
		return cached.entries, nil
	}

	// Execute queries
	var entries []SmartFolderEntry

	// Target query
	if folder.TargetQuery != "" {
		targets, err := r.queryTargets(folder.Namespace, folder.TargetQuery)
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			entries = append(entries, SmartFolderEntry{
				Name:    t.Name,
				Type:    "target",
				LinkRef: "target:" + t.Name,
			})
		}
	}

	// Poller query
	if folder.PollerQuery != "" {
		pollers, err := r.queryPollers(folder.Namespace, folder.PollerQuery)
		if err != nil {
			return nil, err
		}
		for _, p := range pollers {
			entries = append(entries, SmartFolderEntry{
				Name:    p.Target + "/" + p.Name,
				Type:    "poller",
				LinkRef: "poller:" + p.Target + "/" + p.Name,
			})
		}
	}

	// Update cache
	r.cacheMu.Lock()
	r.cache[key] = &smartFolderCache{
		entries:   entries,
		expiresAt: time.Now().Add(time.Duration(folder.CacheTTL) * time.Second),
	}
	r.cacheMu.Unlock()

	return entries, nil
}

// InvalidateCache clears the cache for a folder.
func (r *SmartFolderResolver) InvalidateCache(namespace, path string) {
	key := namespace + ":" + path
	r.cacheMu.Lock()
	delete(r.cache, key)
	r.cacheMu.Unlock()
}

// InvalidateAll clears all caches.
func (r *SmartFolderResolver) InvalidateAll() {
	r.cacheMu.Lock()
	r.cache = make(map[string]*smartFolderCache)
	r.cacheMu.Unlock()
}

// queryTargets executes a target query.
// This is a simplified implementation - in production you'd use a proper query parser.
func (r *SmartFolderResolver) queryTargets(namespace, query string) ([]*store.Target, error) {
	// Parse simple queries like "label:priority = critical"
	query = strings.TrimSpace(query)

	// "label:KEY = VALUE"
	if strings.Contains(query, " = ") {
		parts := strings.SplitN(query, " = ", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if strings.HasPrefix(key, "label:") {
			labelKey := key[6:]
			return r.store.ListTargetsByLabel(namespace, labelKey, value)
		}
	}

	// "label:KEY in [a,b,c]"
	if strings.Contains(query, " in ") {
		parts := strings.SplitN(query, " in ", 2)
		key := strings.TrimSpace(parts[0])
		values := parseValueList(strings.TrimSpace(parts[1]))

		if strings.HasPrefix(key, "label:") {
			labelKey := key[6:]
			var result []*store.Target
			for _, v := range values {
				targets, err := r.store.ListTargetsByLabel(namespace, labelKey, v)
				if err != nil {
					return nil, err
				}
				result = append(result, targets...)
			}
			return result, nil
		}
	}

	// Fallback: return all targets
	return r.store.ListTargets(namespace)
}

// queryPollers executes a poller query.
func (r *SmartFolderResolver) queryPollers(namespace, query string) ([]*store.Poller, error) {
	// For now, return all pollers and filter in memory
	// In production, you'd use a more efficient query

	// This would need poller state joins - simplified for now
	return nil, nil
}

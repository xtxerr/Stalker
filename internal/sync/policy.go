package sync

import (
	"strings"
	"sync"
)

// =============================================================================
// Policy Router
// =============================================================================

// PolicyRouter resolves section paths to policies.
//
// It supports hierarchical resolution:
//
//	"tree.views.by-site" → tries "tree.views.by-site", then "tree.views", then "tree", then default
//
// This allows setting broad policies with specific overrides:
//
//	policies:
//	  entities: create-only       # Default for all entities
//	  entities.targets: full-sync # But targets are fully synced
type PolicyRouter struct {
	mu            sync.RWMutex
	defaultPolicy Policy
	policies      map[string]Policy

	// Cache for resolved policies (path → policy)
	cache map[string]Policy
}

// NewPolicyRouter creates a new policy router with the given default policy.
func NewPolicyRouter(defaultPolicy Policy) *PolicyRouter {
	if !defaultPolicy.IsValid() {
		defaultPolicy = PolicyCreateOnly
	}

	return &PolicyRouter{
		defaultPolicy: defaultPolicy,
		policies:      make(map[string]Policy),
		cache:         make(map[string]Policy),
	}
}

// Set configures a policy for a specific path.
// This invalidates the cache.
func (r *PolicyRouter) Set(path string, policy Policy) {
	if !policy.IsValid() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.policies[path] = policy
	r.cache = make(map[string]Policy) // Invalidate cache
}

// SetAll configures multiple policies at once.
func (r *PolicyRouter) SetAll(policies map[string]Policy) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for path, policy := range policies {
		if policy.IsValid() {
			r.policies[path] = policy
		}
	}
	r.cache = make(map[string]Policy) // Invalidate cache
}

// Get resolves the policy for a path using hierarchical lookup.
//
// Resolution order:
//  1. Exact match for path
//  2. Parent paths (removing rightmost segments)
//  3. Default policy
//
// Example for path "entities.targets.router-1":
//  1. Check "entities.targets.router-1" (exact)
//  2. Check "entities.targets"
//  3. Check "entities"
//  4. Return default
func (r *PolicyRouter) Get(path string) Policy {
	r.mu.RLock()

	// Check cache first
	if cached, ok := r.cache[path]; ok {
		r.mu.RUnlock()
		return cached
	}

	// Need to resolve - upgrade to write lock
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check cache (another goroutine might have populated it)
	if cached, ok := r.cache[path]; ok {
		return cached
	}

	policy := r.resolve(path)
	r.cache[path] = policy
	return policy
}

// resolve performs the actual hierarchical lookup without locking.
func (r *PolicyRouter) resolve(path string) Policy {
	// Exact match
	if policy, ok := r.policies[path]; ok {
		return policy
	}

	// Hierarchical search
	parts := strings.Split(path, ".")
	for i := len(parts) - 1; i > 0; i-- {
		prefix := strings.Join(parts[:i], ".")
		if policy, ok := r.policies[prefix]; ok {
			return policy
		}
	}

	return r.defaultPolicy
}

// GetDefault returns the default policy.
func (r *PolicyRouter) GetDefault() Policy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultPolicy
}

// SetDefault changes the default policy.
// This invalidates the cache.
func (r *PolicyRouter) SetDefault(policy Policy) {
	if !policy.IsValid() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.defaultPolicy = policy
	r.cache = make(map[string]Policy) // Invalidate cache
}

// All returns all explicitly configured policies (excludes default).
func (r *PolicyRouter) All() map[string]Policy {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]Policy, len(r.policies))
	for k, v := range r.policies {
		result[k] = v
	}
	return result
}

// Clear removes all policies and resets to default.
func (r *PolicyRouter) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.policies = make(map[string]Policy)
	r.cache = make(map[string]Policy)
}

// =============================================================================
// Policy Helpers
// =============================================================================

// ShouldCreate returns true if the policy allows creating new entities.
func ShouldCreate(policy Policy) bool {
	return policy != PolicyIgnore
}

// ShouldUpdate returns true if the policy allows updating entities.
func ShouldUpdate(policy Policy, source Source) bool {
	switch policy {
	case PolicyIgnore, PolicyCreateOnly:
		return false
	case PolicySourceAware:
		return source == SourceYAML
	case PolicyFullSync:
		return true
	default:
		return false
	}
}

// ShouldDelete returns true if the policy allows deleting orphaned entities.
func ShouldDelete(policy Policy) bool {
	return policy == PolicyFullSync
}

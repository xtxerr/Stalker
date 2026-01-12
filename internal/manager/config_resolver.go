// Package manager provides business logic and entity management for stalker.
//
// This file implements the ConfigResolver which resolves inherited configuration
// for pollers with caching for performance.
package manager

import (
	"sync"
	"time"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/store"
	"golang.org/x/sync/singleflight"
)

var resolverLog = logging.Component("config_resolver")

// =============================================================================
// Resolved Configuration
// =============================================================================

// ResolvedPollerConfig contains the fully resolved configuration for a poller
// with source attribution for each value.
type ResolvedPollerConfig struct {
	// SNMP v2c
	Community       string
	CommunitySource string

	// SNMPv3
	SecurityName        string
	SecurityNameSource  string
	SecurityLevel       string
	SecurityLevelSource string
	AuthProtocol        string
	AuthProtocolSource  string
	AuthPassword        string
	AuthPasswordSource  string
	PrivProtocol        string
	PrivProtocolSource  string
	PrivPassword        string
	PrivPasswordSource  string

	// Timing
	IntervalMs       uint32
	IntervalMsSource string
	TimeoutMs        uint32
	TimeoutMsSource  string
	Retries          uint32
	RetriesSource    string
	BufferSize       uint32
	BufferSizeSource string
}

// =============================================================================
// Cache Entry
// =============================================================================

type configCacheEntry struct {
	config    *ResolvedPollerConfig
	createdAt time.Time
}

// =============================================================================
// Config Resolver
// =============================================================================

// ConfigResolver resolves inherited configuration for pollers with caching.
//
// The configuration inheritance chain is:
//   Server Defaults → Namespace Defaults → Target Defaults → Poller Explicit
//
// ConfigResolver is safe for concurrent use.
type ConfigResolver struct {
	store *store.Store

	// Primary cache: key → config entry
	cache sync.Map

	// Maps namespace → set of cache keys
	// This allows O(1) invalidation of all configs in a namespace
	// instead of O(n) iteration over all cache entries.
	byNamespace   map[string]map[string]struct{}
	byNamespaceMu sync.RWMutex

	// Singleflight to prevent thundering herd on cache misses
	group singleflight.Group

	// Configuration
	cacheTTL time.Duration
}

// NewConfigResolver creates a new config resolver.
func NewConfigResolver(s *store.Store) *ConfigResolver {
	return &ConfigResolver{
		store:       s,
		byNamespace: make(map[string]map[string]struct{}),
		cacheTTL:    time.Duration(config.DefaultConfigCacheTTLSec) * time.Second,
	}
}

// Resolve returns the fully resolved configuration for a poller.
// Results are cached for cacheTTL duration.
func (r *ConfigResolver) Resolve(namespace, target, poller string) (*ResolvedPollerConfig, error) {
	key := namespace + "/" + target + "/" + poller

	// Check cache
	if entry, ok := r.cache.Load(key); ok {
		cached := entry.(*configCacheEntry)
		if time.Since(cached.createdAt) < r.cacheTTL {
			return cached.config, nil
		}
	}

	// Use singleflight to prevent thundering herd
	result, err, _ := r.group.Do(key, func() (interface{}, error) {
		return r.doResolve(namespace, target, poller)
	})

	if err != nil {
		return nil, err
	}

	return result.(*ResolvedPollerConfig), nil
}

// doResolve performs the actual config resolution.
// This is called via singleflight to ensure only one goroutine
// computes the config at a time.
func (r *ConfigResolver) doResolve(namespace, target, poller string) (*ResolvedPollerConfig, error) {
	key := namespace + "/" + target + "/" + poller
	result := &ResolvedPollerConfig{}

	// 1. Server defaults
	serverCfg, err := r.store.GetServerConfig()
	if err != nil {
		return nil, err
	}
	r.applyServerDefaults(result, serverCfg)

	// 2. Namespace defaults
	ns, err := r.store.GetNamespace(namespace)
	if err != nil {
		return nil, err
	}
	if ns != nil && ns.Config != nil && ns.Config.Defaults != nil {
		r.applyDefaults(result, ns.Config.Defaults, "namespace:"+namespace)
	}

	// 3. Target defaults
	t, err := r.store.GetTarget(namespace, target)
	if err != nil {
		return nil, err
	}
	if t != nil && t.Config != nil && t.Config.Defaults != nil {
		r.applyDefaults(result, t.Config.Defaults, "target:"+target)
	}

	// 4. Poller explicit settings
	p, err := r.store.GetPoller(namespace, target, poller)
	if err != nil {
		return nil, err
	}
	if p != nil && p.PollingConfig != nil {
		r.applyPollerConfig(result, p.PollingConfig, "poller:"+poller)
	}

	// Store in cache
	r.cache.Store(key, &configCacheEntry{
		config:    result,
		createdAt: time.Now(),
	})

	r.addToNamespaceIndex(namespace, key)

	return result, nil
}

// =============================================================================
// Apply Configuration Helpers
// =============================================================================

// applyServerDefaults applies server-level defaults.
func (r *ConfigResolver) applyServerDefaults(result *ResolvedPollerConfig, cfg *store.ServerConfig) {
	if cfg == nil {
		// Use hardcoded defaults if no server config
		result.TimeoutMs = uint32(config.DefaultSNMPTimeoutMs)
		result.TimeoutMsSource = "default"
		result.Retries = uint32(config.DefaultSNMPRetries)
		result.RetriesSource = "default"
		result.IntervalMs = uint32(config.DefaultSNMPIntervalMs)
		result.IntervalMsSource = "default"
		result.BufferSize = uint32(config.DefaultSNMPBufferSize)
		result.BufferSizeSource = "default"
		return
	}

	if cfg.DefaultTimeoutMs > 0 {
		result.TimeoutMs = uint32(cfg.DefaultTimeoutMs)
		result.TimeoutMsSource = "server"
	} else {
		result.TimeoutMs = uint32(config.DefaultSNMPTimeoutMs)
		result.TimeoutMsSource = "default"
	}

	if cfg.DefaultRetries > 0 {
		result.Retries = uint32(cfg.DefaultRetries)
		result.RetriesSource = "server"
	} else {
		result.Retries = uint32(config.DefaultSNMPRetries)
		result.RetriesSource = "default"
	}

	if cfg.DefaultIntervalMs > 0 {
		result.IntervalMs = uint32(cfg.DefaultIntervalMs)
		result.IntervalMsSource = "server"
	} else {
		result.IntervalMs = uint32(config.DefaultSNMPIntervalMs)
		result.IntervalMsSource = "default"
	}

	if cfg.DefaultBufferSize > 0 {
		result.BufferSize = uint32(cfg.DefaultBufferSize)
		result.BufferSizeSource = "server"
	} else {
		result.BufferSize = uint32(config.DefaultSNMPBufferSize)
		result.BufferSizeSource = "default"
	}
}

// applyDefaults applies namespace or target defaults.
func (r *ConfigResolver) applyDefaults(result *ResolvedPollerConfig, defaults *store.PollerDefaults, source string) {
	if defaults == nil {
		return
	}

	// SNMP v2c
	if defaults.Community != "" {
		result.Community = defaults.Community
		result.CommunitySource = source
	}

	// SNMPv3
	if defaults.SecurityName != "" {
		result.SecurityName = defaults.SecurityName
		result.SecurityNameSource = source
	}
	if defaults.SecurityLevel != "" {
		result.SecurityLevel = defaults.SecurityLevel
		result.SecurityLevelSource = source
	}
	if defaults.AuthProtocol != "" {
		result.AuthProtocol = defaults.AuthProtocol
		result.AuthProtocolSource = source
	}
	if defaults.AuthPassword != "" {
		result.AuthPassword = defaults.AuthPassword
		result.AuthPasswordSource = source
	}
	if defaults.PrivProtocol != "" {
		result.PrivProtocol = defaults.PrivProtocol
		result.PrivProtocolSource = source
	}
	if defaults.PrivPassword != "" {
		result.PrivPassword = defaults.PrivPassword
		result.PrivPasswordSource = source
	}

	// Timing
	if defaults.IntervalMs != nil && *defaults.IntervalMs > 0 {
		result.IntervalMs = *defaults.IntervalMs
		result.IntervalMsSource = source
	}
	if defaults.TimeoutMs != nil && *defaults.TimeoutMs > 0 {
		result.TimeoutMs = *defaults.TimeoutMs
		result.TimeoutMsSource = source
	}
	if defaults.Retries != nil {
		result.Retries = *defaults.Retries
		result.RetriesSource = source
	}
	if defaults.BufferSize != nil && *defaults.BufferSize > 0 {
		result.BufferSize = *defaults.BufferSize
		result.BufferSizeSource = source
	}
}

// applyPollerConfig applies poller-specific polling configuration.
func (r *ConfigResolver) applyPollerConfig(result *ResolvedPollerConfig, cfg *store.PollingConfig, source string) {
	if cfg == nil {
		return
	}

	if cfg.IntervalMs != nil && *cfg.IntervalMs > 0 {
		result.IntervalMs = *cfg.IntervalMs
		result.IntervalMsSource = source
	}
	if cfg.TimeoutMs != nil && *cfg.TimeoutMs > 0 {
		result.TimeoutMs = *cfg.TimeoutMs
		result.TimeoutMsSource = source
	}
	if cfg.Retries != nil {
		result.Retries = *cfg.Retries
		result.RetriesSource = source
	}
	if cfg.BufferSize != nil && *cfg.BufferSize > 0 {
		result.BufferSize = *cfg.BufferSize
		result.BufferSizeSource = source
	}
}

// =============================================================================
// Cache Invalidation
// =============================================================================

// Invalidate removes a specific poller's config from cache.
func (r *ConfigResolver) Invalidate(namespace, target, poller string) {
	key := namespace + "/" + target + "/" + poller
	r.cache.Delete(key)
	r.removeFromNamespaceIndex(namespace, key)

	resolverLog.Debug("invalidated config cache", "key", key)
}

// InvalidateTarget removes all cached configs for pollers in a target.
func (r *ConfigResolver) InvalidateTarget(namespace, target string) {
	prefix := namespace + "/" + target + "/"

	r.cache.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			if len(k) > len(prefix) && k[:len(prefix)] == prefix {
				r.cache.Delete(key)
				r.removeFromNamespaceIndex(namespace, k)
			}
		}
		return true
	})

	resolverLog.Debug("invalidated target config cache", "namespace", namespace, "target", target)
}

// InvalidateNamespace removes all cached configs for pollers in a namespace.
//
func (r *ConfigResolver) InvalidateNamespace(namespace string) {
	r.byNamespaceMu.Lock()
	keys, ok := r.byNamespace[namespace]
	if ok {
		// Remove all keys for this namespace
		for key := range keys {
			r.cache.Delete(key)
		}
		delete(r.byNamespace, namespace)
	}
	r.byNamespaceMu.Unlock()

	resolverLog.Debug("invalidated namespace config cache",
		"namespace", namespace,
		"keys_removed", len(keys))
}

// =============================================================================
// Secondary Index Helpers
// =============================================================================

// addToNamespaceIndex adds a key to the namespace secondary index.
func (r *ConfigResolver) addToNamespaceIndex(namespace, key string) {
	r.byNamespaceMu.Lock()
	defer r.byNamespaceMu.Unlock()

	if r.byNamespace[namespace] == nil {
		r.byNamespace[namespace] = make(map[string]struct{})
	}
	r.byNamespace[namespace][key] = struct{}{}
}

// removeFromNamespaceIndex removes a key from the namespace secondary index.
func (r *ConfigResolver) removeFromNamespaceIndex(namespace, key string) {
	r.byNamespaceMu.Lock()
	defer r.byNamespaceMu.Unlock()

	if keys, ok := r.byNamespace[namespace]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(r.byNamespace, namespace)
		}
	}
}

// =============================================================================
// Statistics
// =============================================================================

// CacheStats returns cache statistics.
type CacheStats struct {
	Size       int
	Namespaces int
}

// GetCacheStats returns current cache statistics.
func (r *ConfigResolver) GetCacheStats() CacheStats {
	var size int
	r.cache.Range(func(key, value interface{}) bool {
		size++
		return true
	})

	r.byNamespaceMu.RLock()
	namespaces := len(r.byNamespace)
	r.byNamespaceMu.RUnlock()

	return CacheStats{
		Size:       size,
		Namespaces: namespaces,
	}
}

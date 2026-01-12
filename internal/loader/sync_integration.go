// Package loader - Sync Integration
//
// LOCATION: internal/loader/sync_integration.go
//
// Integrates the YAML loader with the sync engine for policy-based
// configuration reconciliation.

package loader

import (
	"context"
	"time"

	"github.com/xtxerr/stalker/internal/store"
	"github.com/xtxerr/stalker/internal/sync"
)

// =============================================================================
// Sync Configuration Types
// =============================================================================

// SyncConfig holds sync engine configuration from YAML.
type SyncConfig struct {
	// Default policy for sections without explicit policy
	Default string `yaml:"default"`

	// Section-specific policies
	Policies map[string]string `yaml:"policies"`
}

// TreeConfig holds tree configuration from YAML.
type TreeConfig struct {
	// Static links (manual paths)
	Static map[string]interface{} `yaml:"static"`

	// Views (label-based projections)
	Views map[string]*ViewConfig `yaml:"views"`

	// Smart folders (query-based)
	Smart map[string]*SmartFolderConfig `yaml:"smart"`
}

// ViewConfig represents a view definition in YAML.
type ViewConfig struct {
	Path        string `yaml:"path"`
	Match       string `yaml:"match"` // "targets" or "pollers"
	When        string `yaml:"when"`  // Filter expression
	Description string `yaml:"description"`
	Priority    int    `yaml:"priority"`
}

// SmartFolderConfig represents a smart folder definition in YAML.
type SmartFolderConfig struct {
	Description string `yaml:"description"`
	Targets     string `yaml:"targets"` // Target query
	Pollers     string `yaml:"pollers"` // Poller query
	Cache       string `yaml:"cache"`   // e.g., "60s"
}

// =============================================================================
// Sync Engine Setup
// =============================================================================

// SetupSyncEngine creates and configures a sync engine from YAML config.
func SetupSyncEngine(db interface{}, cfg *SyncConfig) (*sync.Engine, error) {
	// Parse default policy
	defaultPolicy := sync.PolicyCreateOnly
	if cfg != nil && cfg.Default != "" {
		defaultPolicy = sync.Policy(cfg.Default)
		if !defaultPolicy.IsValid() {
			defaultPolicy = sync.PolicyCreateOnly
		}
	}

	// Parse section policies
	policies := make(map[string]sync.Policy)
	if cfg != nil {
		for path, policyStr := range cfg.Policies {
			policy := sync.Policy(policyStr)
			if policy.IsValid() {
				policies[path] = policy
			}
		}
	}

	// Apply defaults for tree sections if not specified
	if _, ok := policies["defaults"]; !ok {
		policies["defaults"] = sync.PolicyFullSync
	}
	if _, ok := policies["tree.views"]; !ok {
		policies["tree.views"] = sync.PolicyFullSync
	}
	if _, ok := policies["tree.smart"]; !ok {
		policies["tree.smart"] = sync.PolicyFullSync
	}

	engine := sync.NewEngine(&sync.EngineConfig{
		DefaultPolicy: defaultPolicy,
		Policies:      policies,
	})

	return engine, nil
}

// =============================================================================
// Entity Conversion
// =============================================================================

// ConvertNamespaces converts YAML namespace configs to sync-ready entities.
func ConvertNamespaces(yamlConfig map[string]*NamespaceConfig) []*sync.SyncableNamespace {
	var result []*sync.SyncableNamespace

	for name, cfg := range yamlConfig {
		ns := &store.Namespace{
			Name:        name,
			Description: cfg.Description,
			Source:      "yaml",
		}

		if cfg.Defaults != nil {
			ns.Config = &store.NamespaceConfig{
				Defaults: convertPollerDefaults(cfg.Defaults),
			}
		}

		if cfg.SessionCleanupIntervalSec != nil {
			if ns.Config == nil {
				ns.Config = &store.NamespaceConfig{}
			}
			ns.Config.SessionCleanupIntervalSec = cfg.SessionCleanupIntervalSec
		}

		result = append(result, sync.WrapNamespace(ns))
	}

	return result
}

// ConvertTargets converts YAML target configs to sync-ready entities.
func ConvertTargets(yamlConfig map[string]*NamespaceConfig) []*sync.SyncableTarget {
	var result []*sync.SyncableTarget

	for nsName, nsCfg := range yamlConfig {
		for targetName, targetCfg := range nsCfg.Targets {
			target := &store.Target{
				Namespace:   nsName,
				Name:        targetName,
				Description: targetCfg.Description,
				Labels:      targetCfg.Labels,
				Source:      "yaml",
			}

			if targetCfg.Defaults != nil {
				target.Config = &store.TargetConfig{
					Defaults: convertPollerDefaults(targetCfg.Defaults),
				}
			}

			result = append(result, sync.WrapTarget(target))
		}
	}

	return result
}

// ConvertPollers converts YAML poller configs to sync-ready entities.
func ConvertPollers(yamlConfig map[string]*NamespaceConfig) []*sync.SyncablePoller {
	var result []*sync.SyncablePoller

	for nsName, nsCfg := range yamlConfig {
		for targetName, targetCfg := range nsCfg.Targets {
			for pollerName, pollerCfg := range targetCfg.Pollers {
				poller := &store.Poller{
					Namespace:   nsName,
					Target:      targetName,
					Name:        pollerName,
					Description: pollerCfg.Description,
					Protocol:    pollerCfg.Protocol,
					AdminState:  pollerCfg.AdminState,
					Source:      "yaml",
				}

				if pollerCfg.ProtocolConfig != nil {
					// Convert map to JSON
					// (simplified - in production marshal to JSON)
				}

				if pollerCfg.PollingConfig != nil {
					poller.PollingConfig = &store.PollingConfig{
						IntervalMs: pollerCfg.PollingConfig.IntervalMs,
						TimeoutMs:  pollerCfg.PollingConfig.TimeoutMs,
						Retries:    pollerCfg.PollingConfig.Retries,
						BufferSize: pollerCfg.PollingConfig.BufferSize,
					}
				}

				if poller.AdminState == "" {
					poller.AdminState = "disabled"
				}

				result = append(result, sync.WrapPoller(poller))
			}
		}
	}

	return result
}

// ConvertViews converts YAML view configs to sync-ready entities.
func ConvertViews(namespace string, yamlConfig map[string]*ViewConfig) []*sync.SyncableTreeView {
	var result []*sync.SyncableTreeView

	for name, cfg := range yamlConfig {
		entityType := "target"
		if cfg.Match == "pollers" {
			entityType = "poller"
		}

		view := &sync.TreeView{
			Namespace:    namespace,
			Name:         name,
			Description:  cfg.Description,
			PathTemplate: cfg.Path,
			EntityType:   entityType,
			FilterExpr:   cfg.When,
			Priority:     cfg.Priority,
			Enabled:      true,
			Source:       "yaml",
		}

		if view.Priority == 0 {
			view.Priority = 50
		}

		result = append(result, sync.WrapTreeView(view))
	}

	return result
}

// ConvertSmartFolders converts YAML smart folder configs to sync-ready entities.
func ConvertSmartFolders(namespace string, yamlConfig map[string]*SmartFolderConfig) []*sync.SyncableSmartFolder {
	var result []*sync.SyncableSmartFolder

	for path, cfg := range yamlConfig {
		cacheTTL := parseDuration(cfg.Cache)
		if cacheTTL == 0 {
			cacheTTL = 60
		}

		folder := &sync.TreeSmartFolder{
			Namespace:   namespace,
			Path:        path,
			Description: cfg.Description,
			TargetQuery: cfg.Targets,
			PollerQuery: cfg.Pollers,
			CacheTTL:    cacheTTL,
			Priority:    25,
			Enabled:     true,
			Source:      "yaml",
		}

		result = append(result, sync.WrapSmartFolder(folder))
	}

	return result
}

// parseDuration parses a duration string like "60s", "2m", "1h".
func parseDuration(s string) int {
	if s == "" {
		return 0
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}

	return int(d.Seconds())
}

// =============================================================================
// Register Reconcilers
// =============================================================================

// RegisterReconcilers sets up all reconcilers with the sync engine.
func RegisterReconcilers(engine *sync.Engine, st *store.Store, cfg *Config) {
	// Namespace reconciler (order 10)
	engine.Register("namespaces", 10, func(ctx context.Context, policy sync.Policy) (*sync.SectionResult, error) {
		yamlEntities := ConvertNamespaces(cfg.Namespaces)
		
		reconciler := sync.NewGenericReconciler(sync.GenericReconcilerConfig[*sync.SyncableNamespace]{
			Name:       "namespaces",
			EntityType: "namespace",
			Store:      sync.NewNamespaceSyncStore(st.DB()),
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Target reconciler (order 20)
	engine.Register("targets", 20, func(ctx context.Context, policy sync.Policy) (*sync.SectionResult, error) {
		yamlEntities := ConvertTargets(cfg.Namespaces)

		reconciler := sync.NewGenericReconciler(sync.GenericReconcilerConfig[*sync.SyncableTarget]{
			Name:       "targets",
			EntityType: "target",
			Store:      sync.NewTargetSyncStore(st.DB()),
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Poller reconciler (order 30)
	engine.Register("pollers", 30, func(ctx context.Context, policy sync.Policy) (*sync.SectionResult, error) {
		yamlEntities := ConvertPollers(cfg.Namespaces)

		reconciler := sync.NewGenericReconciler(sync.GenericReconcilerConfig[*sync.SyncablePoller]{
			Name:       "pollers",
			EntityType: "poller",
			Store:      sync.NewPollerSyncStore(st.DB()),
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Tree views reconciler (order 51)
	// (Would need TreeViewSyncStore implementation)

	// Smart folders reconciler (order 52)
	// (Would need SmartFolderSyncStore implementation)
}

// =============================================================================
// Apply with Sync
// =============================================================================

// ApplyWithSync applies configuration using the sync engine.
//
// This replaces the old Apply() function with policy-aware sync.
func ApplyWithSync(ctx context.Context, cfg *Config, st *store.Store) (*sync.SyncResult, error) {
	// Setup sync engine
	engine, err := SetupSyncEngine(st.DB(), cfg.Sync)
	if err != nil {
		return nil, err
	}

	// Register reconcilers
	RegisterReconcilers(engine, st, cfg)

	// Run sync
	return engine.Sync(ctx)
}

// =============================================================================
// Dry Run
// =============================================================================

// DryRun shows what changes would be made without applying them.
func DryRun(ctx context.Context, cfg *Config, st *store.Store) (*sync.SyncResult, error) {
	engine, err := SetupSyncEngine(st.DB(), cfg.Sync)
	if err != nil {
		return nil, err
	}

	RegisterReconcilers(engine, st, cfg)

	return engine.DryRun(ctx)
}

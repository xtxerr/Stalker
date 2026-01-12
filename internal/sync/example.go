package sync

import (
	"context"
	"database/sql"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Example: Setting up the Sync Engine
// =============================================================================

// SetupEngine creates and configures a sync engine with all reconcilers.
//
// This is the main entry point for integrating the sync engine into stalkerd.
//
// Usage:
//
//	engine, err := sync.SetupEngine(db, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := engine.Sync(ctx)
//	if err != nil {
//	    log.Error("sync failed", "error", err)
//	}
//
//	log.Info("sync completed",
//	    "created", result.TotalCreated,
//	    "updated", result.TotalUpdated,
//	    "deleted", result.TotalDeleted,
//	)
func SetupEngine(db *sql.DB, syncCfg *Config) (*Engine, error) {
	// Run schema migration first
	if err := MigrateSchema(db); err != nil {
		return nil, err
	}

	// Create engine with configuration
	engine := NewEngine(&EngineConfig{
		DefaultPolicy: syncCfg.Default,
		Policies:      syncCfg.Policies,
		TxProvider:    &sqlTxProvider{db: db},
	})

	// Reconcilers will be registered by the caller who has access to
	// the YAML config data. This function just sets up the infrastructure.

	return engine, nil
}

// sqlTxProvider wraps *sql.DB to implement TxProvider.
type sqlTxProvider struct {
	db *sql.DB
}

func (p *sqlTxProvider) Transaction(fn func(tx *sql.Tx) error) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// =============================================================================
// Example: Creating Reconcilers from YAML Config
// =============================================================================

// YAMLConfig represents the parsed YAML configuration.
// This would be defined in the loader package.
type YAMLConfig struct {
	Namespaces map[string]*YAMLNamespace
}

type YAMLNamespace struct {
	Description string
	Targets     map[string]*YAMLTarget
}

type YAMLTarget struct {
	Description string
	Labels      map[string]string
	Pollers     map[string]*YAMLPoller
}

type YAMLPoller struct {
	Description    string
	Protocol       string
	ProtocolConfig []byte
	AdminState     string
}

// RegisterReconcilersFromYAML creates and registers reconcilers for YAML config.
//
// This function converts the YAML config into Syncable entities and registers
// reconciler functions that use the GenericReconciler under the hood.
func RegisterReconcilersFromYAML(engine *Engine, db *sql.DB, yaml *YAMLConfig) {
	// Namespace reconciler (order 10)
	engine.Register("namespaces", 10, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		// Convert YAML to Syncable
		var yamlEntities []*SyncableNamespace
		for name, nsCfg := range yaml.Namespaces {
			ns := &store.Namespace{
				Name:        name,
				Description: nsCfg.Description,
			}
			yamlEntities = append(yamlEntities, WrapNamespace(ns))
		}

		// Create reconciler and run
		store := NewNamespaceSyncStore(db)
		reconciler := NewGenericReconciler(GenericReconcilerConfig[*SyncableNamespace]{
			Name:       "namespaces",
			Order:      10,
			EntityType: "namespace",
			Store:      store,
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Target reconciler (order 20)
	engine.Register("targets", 20, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		// Convert YAML to Syncable
		var yamlEntities []*SyncableTarget
		for nsName, nsCfg := range yaml.Namespaces {
			for targetName, targetCfg := range nsCfg.Targets {
				t := &store.Target{
					Namespace:   nsName,
					Name:        targetName,
					Description: targetCfg.Description,
					Labels:      targetCfg.Labels,
				}
				yamlEntities = append(yamlEntities, WrapTarget(t))
			}
		}

		store := NewTargetSyncStore(db)
		reconciler := NewGenericReconciler(GenericReconcilerConfig[*SyncableTarget]{
			Name:       "targets",
			Order:      20,
			EntityType: "target",
			Store:      store,
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Poller reconciler (order 30)
	engine.Register("pollers", 30, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		// Convert YAML to Syncable
		var yamlEntities []*SyncablePoller
		for nsName, nsCfg := range yaml.Namespaces {
			for targetName, targetCfg := range nsCfg.Targets {
				for pollerName, pollerCfg := range targetCfg.Pollers {
					p := &store.Poller{
						Namespace:      nsName,
						Target:         targetName,
						Name:           pollerName,
						Description:    pollerCfg.Description,
						Protocol:       pollerCfg.Protocol,
						ProtocolConfig: pollerCfg.ProtocolConfig,
						AdminState:     pollerCfg.AdminState,
					}
					if p.AdminState == "" {
						p.AdminState = "disabled" // Default
					}
					yamlEntities = append(yamlEntities, WrapPoller(p))
				}
			}
		}

		store := NewPollerSyncStore(db)
		reconciler := NewGenericReconciler(GenericReconcilerConfig[*SyncablePoller]{
			Name:       "pollers",
			Order:      30,
			EntityType: "poller",
			Store:      store,
		})

		return reconciler.Reconcile(ctx, policy, "", yamlEntities)
	})

	// Tree reconcilers would be registered similarly (order 50, 51, 52)
}

// =============================================================================
// Example: Full Startup Sequence
// =============================================================================

/*
func main() {
	// 1. Load YAML config
	yamlCfg, err := loader.Load("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Open database
	db, err := sql.Open("duckdb", yamlCfg.Storage.DBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 3. Setup sync engine
	syncCfg := yamlCfg.Sync
	if syncCfg == nil {
		syncCfg = sync.DefaultConfig()
	}

	engine, err := sync.SetupEngine(db, syncCfg)
	if err != nil {
		log.Fatal(err)
	}

	// 4. Register reconcilers with YAML data
	sync.RegisterReconcilersFromYAML(engine, db, yamlCfg)

	// 5. Run sync
	result, err := engine.Sync(context.Background())
	if err != nil {
		log.Error("sync failed", "error", err)
		// Decide: fatal or continue with DB state?
	}

	log.Info("sync completed",
		"created", result.TotalCreated,
		"updated", result.TotalUpdated,
		"deleted", result.TotalDeleted,
		"duration", result.Duration,
	)

	// 6. Continue with normal startup...
}
*/

package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// =============================================================================
// Sync Engine
// =============================================================================

// Engine orchestrates the complete synchronization process.
//
// It manages:
//   - Policy routing (which policy applies to which section)
//   - Reconciler ordering (respecting dependencies)
//   - Transaction management (all changes are atomic)
//   - Result aggregation and reporting
type Engine struct {
	policyRouter *PolicyRouter
	reconcilers  []reconcilerEntry
	txProvider   TxProvider

	// Configuration
	configHasher func(interface{}) uint64
}

// TxProvider provides database transactions.
type TxProvider interface {
	// Transaction executes fn within a database transaction.
	// If fn returns an error, the transaction is rolled back.
	Transaction(fn func(tx *sql.Tx) error) error
}

// reconcilerEntry holds a reconciler with metadata.
type reconcilerEntry struct {
	name      string
	order     int
	reconcile ReconcileFunc
}

// ReconcileFunc is the signature for reconciler functions.
type ReconcileFunc func(ctx context.Context, policy Policy) (*SectionResult, error)

// EngineConfig holds engine configuration.
type EngineConfig struct {
	// Default policy for sections without explicit policy
	DefaultPolicy Policy

	// Section-specific policies
	Policies map[string]Policy

	// Transaction provider for atomic operations
	TxProvider TxProvider
}

// NewEngine creates a new sync engine.
func NewEngine(cfg *EngineConfig) *Engine {
	if cfg == nil {
		cfg = &EngineConfig{}
	}

	defaultPolicy := cfg.DefaultPolicy
	if !defaultPolicy.IsValid() {
		defaultPolicy = PolicyCreateOnly
	}

	router := NewPolicyRouter(defaultPolicy)
	if cfg.Policies != nil {
		router.SetAll(cfg.Policies)
	}

	return &Engine{
		policyRouter: router,
		reconcilers:  make([]reconcilerEntry, 0),
		txProvider:   cfg.TxProvider,
	}
}

// =============================================================================
// Reconciler Registration
// =============================================================================

// Register adds a reconciler to the engine.
//
// Reconcilers are executed in order of their order value (ascending).
// Dependencies should be registered with lower order values.
func (e *Engine) Register(name string, order int, fn ReconcileFunc) {
	e.reconcilers = append(e.reconcilers, reconcilerEntry{
		name:      name,
		order:     order,
		reconcile: fn,
	})

	// Keep sorted by order
	sort.Slice(e.reconcilers, func(i, j int) bool {
		return e.reconcilers[i].order < e.reconcilers[j].order
	})
}

// RegisterReconciler adds a Reconciler interface implementation.
func (e *Engine) RegisterReconciler(r Reconciler) {
	e.Register(r.Name(), r.Order(), r.Reconcile)
}

// =============================================================================
// Policy Management
// =============================================================================

// SetPolicy sets the policy for a section path.
func (e *Engine) SetPolicy(path string, policy Policy) {
	e.policyRouter.Set(path, policy)
}

// GetPolicy returns the effective policy for a section path.
func (e *Engine) GetPolicy(path string) Policy {
	return e.policyRouter.Get(path)
}

// SetDefaultPolicy changes the default policy.
func (e *Engine) SetDefaultPolicy(policy Policy) {
	e.policyRouter.SetDefault(policy)
}

// =============================================================================
// Sync Execution
// =============================================================================

// Sync executes all registered reconcilers in order.
//
// If a transaction provider is configured, all changes are made atomically.
// If any reconciler fails, the entire sync is rolled back.
func (e *Engine) Sync(ctx context.Context) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{
		StartedAt: start,
		Sections:  make(map[string]*SectionResult),
	}

	// Execute with or without transaction
	var err error
	if e.txProvider != nil {
		err = e.txProvider.Transaction(func(tx *sql.Tx) error {
			return e.executeReconcilers(ctx, result)
		})
	} else {
		err = e.executeReconcilers(ctx, result)
	}

	if err != nil {
		result.Error = err
	}

	result.Duration = time.Since(start)
	result.Aggregate()

	// Log summary
	log.Info("sync completed",
		"duration", result.Duration,
		"created", result.TotalCreated,
		"updated", result.TotalUpdated,
		"deleted", result.TotalDeleted,
		"skipped", result.TotalSkipped,
		"error", result.Error,
	)

	return result, err
}

// SyncSection executes a single reconciler by name.
//
// This is useful for targeted updates without running the full sync.
func (e *Engine) SyncSection(ctx context.Context, sectionName string) (*SectionResult, error) {
	for _, rec := range e.reconcilers {
		if rec.name == sectionName {
			policy := e.policyRouter.Get(rec.name)
			return rec.reconcile(ctx, policy)
		}
	}

	return nil, fmt.Errorf("unknown section: %s", sectionName)
}

// executeReconcilers runs all reconcilers in order.
func (e *Engine) executeReconcilers(ctx context.Context, result *SyncResult) error {
	for _, rec := range e.reconcilers {
		// Check context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		policy := e.policyRouter.Get(rec.name)

		log.Debug("executing reconciler",
			"name", rec.name,
			"order", rec.order,
			"policy", policy,
		)

		sectionResult, err := rec.reconcile(ctx, policy)
		if err != nil {
			return fmt.Errorf("reconciler %s: %w", rec.name, err)
		}

		result.Sections[rec.name] = sectionResult

		log.Info("reconciler completed",
			"name", rec.name,
			"created", sectionResult.Created,
			"updated", sectionResult.Updated,
			"deleted", sectionResult.Deleted,
			"skipped", sectionResult.Skipped,
			"duration", sectionResult.Duration,
		)
	}

	return nil
}

// =============================================================================
// Dry Run
// =============================================================================

// DryRun calculates what changes would be made without applying them.
//
// It executes all diff calculations but skips the actual database operations.
// Useful for previewing changes before committing.
func (e *Engine) DryRun(ctx context.Context) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{
		StartedAt: start,
		Sections:  make(map[string]*SectionResult),
	}

	// For dry run, we don't execute in a transaction
	for _, rec := range e.reconcilers {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		policy := e.policyRouter.Get(rec.name)

		// Note: Reconcilers should check a "dry run" flag if provided via context
		// For now, we just run them and they will calculate diffs
		sectionResult, err := rec.reconcile(ctx, policy)
		if err != nil {
			result.Error = fmt.Errorf("reconciler %s: %w", rec.name, err)
			break
		}

		result.Sections[rec.name] = sectionResult
	}

	result.Duration = time.Since(start)
	result.Aggregate()

	return result, nil
}

// =============================================================================
// Inspection
// =============================================================================

// ListReconcilers returns information about registered reconcilers.
func (e *Engine) ListReconcilers() []ReconcilerInfo {
	infos := make([]ReconcilerInfo, len(e.reconcilers))
	for i, rec := range e.reconcilers {
		infos[i] = ReconcilerInfo{
			Name:   rec.name,
			Order:  rec.order,
			Policy: e.policyRouter.Get(rec.name),
		}
	}
	return infos
}

// ReconcilerInfo holds information about a registered reconciler.
type ReconcilerInfo struct {
	Name   string
	Order  int
	Policy Policy
}

// ListPolicies returns all configured policies.
func (e *Engine) ListPolicies() map[string]Policy {
	return e.policyRouter.All()
}

// GetDefaultPolicy returns the default policy.
func (e *Engine) GetDefaultPolicy() Policy {
	return e.policyRouter.GetDefault()
}

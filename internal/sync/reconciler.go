package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/xtxerr/stalker/internal/logging"
)

var log = logging.Component("sync")

// =============================================================================
// Generic Reconciler
// =============================================================================

// GenericReconciler implements reconciliation for any Syncable type.
//
// It orchestrates:
//  1. Loading state from YAML and DB
//  2. Calculating diff
//  3. Executing changes in batches
//  4. Reporting results
//
// Type parameter T must implement Syncable.
type GenericReconciler[T Syncable] struct {
	name       string
	order      int
	entityType string
	store      SyncStore[T]
	batchSize  int
}

// GenericReconcilerConfig holds configuration for a generic reconciler.
type GenericReconcilerConfig[T Syncable] struct {
	// Name is the reconciler's name (e.g., "entities.targets")
	Name string

	// Order determines execution sequence (lower = earlier)
	Order int

	// EntityType for logging (e.g., "target")
	EntityType string

	// Store provides database access
	Store SyncStore[T]

	// BatchSize for bulk operations (default: 1000)
	BatchSize int
}

// NewGenericReconciler creates a new generic reconciler.
func NewGenericReconciler[T Syncable](cfg GenericReconcilerConfig[T]) *GenericReconciler[T] {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	return &GenericReconciler[T]{
		name:       cfg.Name,
		order:      cfg.Order,
		entityType: cfg.EntityType,
		store:      cfg.Store,
		batchSize:  batchSize,
	}
}

// Name implements Reconciler.
func (r *GenericReconciler[T]) Name() string {
	return r.name
}

// Order implements Reconciler.
func (r *GenericReconciler[T]) Order() int {
	return r.order
}

// Reconcile performs reconciliation with the given YAML state.
//
// The namespace parameter filters which entities to load from DB.
// If empty, all entities are loaded (for cross-namespace reconciliation).
func (r *GenericReconciler[T]) Reconcile(
	ctx context.Context,
	policy Policy,
	namespace string,
	yamlEntities []T,
) (*SectionResult, error) {
	start := time.Now()
	result := &SectionResult{
		Section: r.name,
		Policy:  policy,
	}

	// Skip if policy is ignore
	if policy == PolicyIgnore {
		result.Skipped = len(yamlEntities)
		result.Total = len(yamlEntities)
		result.Duration = time.Since(start)
		log.Debug("reconciler skipped", "name", r.name, "policy", policy)
		return result, nil
	}

	// Load DB state
	dbEntities, err := r.store.ListAll(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("load db state: %w", err)
	}

	// Calculate diff
	diffEngine := NewDiffEngine[T](r.entityType, policy)
	entries := diffEngine.Diff(yamlEntities, dbEntities)
	stats := CalculateStats(entries)

	result.Total = stats.Total
	result.Skipped = stats.Skipped
	result.Entries = entries

	log.Debug("diff calculated",
		"name", r.name,
		"yaml_count", len(yamlEntities),
		"db_count", len(dbEntities),
		"creates", stats.Creates,
		"updates", stats.Updates,
		"deletes", stats.Deletes,
		"skipped", stats.Skipped,
	)

	// Execute changes
	if stats.HasChanges() {
		if err := r.execute(ctx, entries, yamlEntities, result); err != nil {
			return result, err
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// execute applies the diff entries to the database.
func (r *GenericReconciler[T]) execute(
	ctx context.Context,
	entries []DiffEntry,
	yamlEntities []T,
	result *SectionResult,
) error {
	// Build index of YAML entities for lookup
	yamlByKey := make(map[string]T, len(yamlEntities))
	for _, e := range yamlEntities {
		yamlByKey[e.SyncKey()] = e
	}

	// Collect entities by action
	var creates, updates []T
	var deletes []string

	for _, entry := range entries {
		switch entry.Action {
		case ActionCreate:
			if entity, ok := yamlByKey[entry.Key]; ok {
				entity.SetSyncSource(SourceYAML)
				creates = append(creates, entity)
			}

		case ActionUpdate:
			if entity, ok := yamlByKey[entry.Key]; ok {
				entity.SetSyncSource(SourceYAML)
				updates = append(updates, entity)
			}

		case ActionDelete:
			deletes = append(deletes, entry.Key)
		}
	}

	// Execute creates in batches
	if len(creates) > 0 {
		if err := r.executeBatched(ctx, creates, r.store.BulkCreate, "create"); err != nil {
			return err
		}
		result.Created = len(creates)
	}

	// Execute updates in batches
	if len(updates) > 0 {
		if err := r.executeBatched(ctx, updates, r.store.BulkUpdate, "update"); err != nil {
			return err
		}
		result.Updated = len(updates)
	}

	// Execute deletes in batches
	if len(deletes) > 0 {
		if err := r.executeDeleteBatched(ctx, deletes); err != nil {
			return err
		}
		result.Deleted = len(deletes)
	}

	return nil
}

// executeBatched executes a bulk operation in batches.
func (r *GenericReconciler[T]) executeBatched(
	ctx context.Context,
	entities []T,
	fn func(context.Context, []T) error,
	operation string,
) error {
	for i := 0; i < len(entities); i += r.batchSize {
		end := min(i+r.batchSize, len(entities))
		batch := entities[i:end]

		if err := fn(ctx, batch); err != nil {
			return fmt.Errorf("bulk %s batch %d-%d: %w", operation, i, end, err)
		}

		log.Debug("batch executed",
			"name", r.name,
			"operation", operation,
			"batch_start", i,
			"batch_end", end,
			"batch_size", len(batch),
		)
	}

	return nil
}

// executeDeleteBatched executes delete operations in batches.
func (r *GenericReconciler[T]) executeDeleteBatched(
	ctx context.Context,
	keys []string,
) error {
	for i := 0; i < len(keys); i += r.batchSize {
		end := min(i+r.batchSize, len(keys))
		batch := keys[i:end]

		if err := r.store.BulkDelete(ctx, batch); err != nil {
			return fmt.Errorf("bulk delete batch %d-%d: %w", i, end, err)
		}

		log.Debug("delete batch executed",
			"name", r.name,
			"batch_start", i,
			"batch_end", end,
			"batch_size", len(batch),
		)
	}

	return nil
}

// =============================================================================
// Helper
// =============================================================================

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

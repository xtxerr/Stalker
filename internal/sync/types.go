// Package sync provides configuration reconciliation between YAML and database.
//
// The sync engine implements a Kubernetes-style reconciliation loop:
//
//  1. Load desired state from YAML config
//  2. Load actual state from database
//  3. Calculate diff based on configured policy
//  4. Apply changes atomically
//
// Different policies control how conflicts are resolved:
//
//   - create-only:   Only create new entities, never modify existing
//   - full-sync:     YAML is authoritative, creates/updates/deletes
//   - source-aware:  Only modify entities with source='yaml'
//   - ignore:        Skip section entirely, DB is authoritative
//
// The engine processes entities in dependency order (namespaces → targets →
// pollers → tree) to maintain referential integrity.
package sync

import (
	"context"
	"time"
)

// =============================================================================
// Policies
// =============================================================================

// Policy defines how the sync engine handles conflicts between YAML and DB.
type Policy string

const (
	// PolicyCreateOnly creates new entities but never modifies or deletes existing ones.
	// This is the safest default - API changes are always preserved.
	PolicyCreateOnly Policy = "create-only"

	// PolicyFullSync treats YAML as the single source of truth.
	// Entities not in YAML will be DELETED from the database.
	// Use with caution - this can cause data loss.
	PolicyFullSync Policy = "full-sync"

	// PolicySourceAware only updates entities that were originally created from YAML.
	// Entities created via API (source='api') are never modified.
	// Best balance between declarative config and API flexibility.
	PolicySourceAware Policy = "source-aware"

	// PolicyIgnore skips the section entirely.
	// The database is authoritative, YAML config is not processed.
	PolicyIgnore Policy = "ignore"
)

// ValidPolicies contains all valid policy values.
var ValidPolicies = []Policy{
	PolicyCreateOnly,
	PolicyFullSync,
	PolicySourceAware,
	PolicyIgnore,
}

// IsValid returns true if the policy is a known valid policy.
func (p Policy) IsValid() bool {
	for _, valid := range ValidPolicies {
		if p == valid {
			return true
		}
	}
	return false
}

// =============================================================================
// Actions
// =============================================================================

// Action represents the operation to perform on an entity.
type Action string

const (
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
	ActionSkip   Action = "skip"
)

// =============================================================================
// Sources
// =============================================================================

// Source indicates where an entity was created from.
type Source string

const (
	SourceYAML   Source = "yaml"   // Created/updated from YAML config
	SourceAPI    Source = "api"    // Created via API
	SourceImport Source = "import" // Imported from external source
)

// =============================================================================
// Syncable Interface
// =============================================================================

// Syncable is the interface that all synchronizable entities must implement.
//
// This interface enables the generic reconciler to work with any entity type
// while maintaining type safety through generics.
type Syncable interface {
	// SyncKey returns the unique identifier for this entity.
	// Format varies by type:
	//   - Namespace: "namespace-name"
	//   - Target:    "namespace/target"
	//   - Poller:    "namespace/target/poller"
	//   - TreeNode:  "namespace:/path/to/node"
	SyncKey() string

	// SyncHash returns a hash of the entity's content.
	// Used for efficient change detection - if hashes match, no update needed.
	// The hash should include all fields that affect entity behavior,
	// but exclude metadata like created_at, updated_at, version.
	SyncHash() uint64

	// SyncSource returns where this entity was created from.
	SyncSource() Source

	// SetSyncSource sets the source field.
	SetSyncSource(Source)

	// SyncDependencies returns keys of entities this one depends on.
	// Used to determine sync order. For example:
	//   - Target depends on Namespace
	//   - Poller depends on Target
	//   - TreeNode may depend on Target or Poller
	SyncDependencies() []string
}

// =============================================================================
// Diff Entry
// =============================================================================

// DiffEntry represents a single change to be applied.
type DiffEntry struct {
	// Action to perform
	Action Action

	// Key is the entity's unique identifier
	Key string

	// EntityType for logging/metrics (e.g., "target", "poller")
	EntityType string

	// Reason explains why this action was chosen
	Reason string

	// SourceState is the source of the DB entity (if exists)
	SourceState Source

	// YAMLHash is the hash of the YAML entity (0 if not in YAML)
	YAMLHash uint64

	// DBHash is the hash of the DB entity (0 if not in DB)
	DBHash uint64
}

// =============================================================================
// Results
// =============================================================================

// SectionResult holds the result of reconciling a single section.
type SectionResult struct {
	// Section name (e.g., "targets", "tree.views")
	Section string

	// Policy that was applied
	Policy Policy

	// Counts by action
	Created int
	Updated int
	Deleted int
	Skipped int

	// Total entities processed
	Total int

	// Duration of this section's reconciliation
	Duration time.Duration

	// Errors encountered (non-fatal)
	Warnings []string

	// Detailed diff entries (optional, for debugging)
	Entries []DiffEntry
}

// SyncResult holds the result of a complete sync operation.
type SyncResult struct {
	// When the sync started
	StartedAt time.Time

	// Total duration
	Duration time.Duration

	// Whether sync was skipped (e.g., config unchanged)
	Skipped bool

	// Reason for skipping (if Skipped is true)
	SkipReason string

	// Config hash that was synced
	ConfigHash uint64

	// Results by section
	Sections map[string]*SectionResult

	// Aggregated counts
	TotalCreated int
	TotalUpdated int
	TotalDeleted int
	TotalSkipped int

	// Fatal error (if sync failed)
	Error error
}

// Aggregate calculates totals from section results.
func (r *SyncResult) Aggregate() {
	r.TotalCreated = 0
	r.TotalUpdated = 0
	r.TotalDeleted = 0
	r.TotalSkipped = 0

	for _, section := range r.Sections {
		r.TotalCreated += section.Created
		r.TotalUpdated += section.Updated
		r.TotalDeleted += section.Deleted
		r.TotalSkipped += section.Skipped
	}
}

// HasChanges returns true if any changes were made.
func (r *SyncResult) HasChanges() bool {
	return r.TotalCreated > 0 || r.TotalUpdated > 0 || r.TotalDeleted > 0
}

// =============================================================================
// Store Interface
// =============================================================================

// SyncStore is the interface for database operations needed by the sync engine.
// Each entity type implements this interface.
type SyncStore[T Syncable] interface {
	// ListAll returns all entities of this type in the given namespace.
	// If namespace is empty, returns entities from all namespaces.
	ListAll(ctx context.Context, namespace string) ([]T, error)

	// BulkCreate creates multiple entities in a single operation.
	// All entities must have their SyncSource set before calling.
	BulkCreate(ctx context.Context, entities []T) error

	// BulkUpdate updates multiple entities in a single operation.
	// Only entities with matching keys are updated.
	BulkUpdate(ctx context.Context, entities []T) error

	// BulkDelete deletes entities by their keys.
	BulkDelete(ctx context.Context, keys []string) error
}

// =============================================================================
// Reconciler Interface
// =============================================================================

// Reconciler reconciles a specific entity type.
type Reconciler interface {
	// Name returns the reconciler's name (e.g., "entities.targets")
	Name() string

	// Order returns the execution order (lower = earlier)
	Order() int

	// Reconcile performs the reconciliation and returns the result.
	Reconcile(ctx context.Context, policy Policy) (*SectionResult, error)
}

// =============================================================================
// Engine Configuration
// =============================================================================

// Config holds the sync engine configuration.
type Config struct {
	// Default policy for sections without explicit policy
	Default Policy `yaml:"default"`

	// Section-specific policies (path → policy)
	// Paths are hierarchical: "tree.views" inherits from "tree" if not set
	Policies map[string]Policy `yaml:"policies"`
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() *Config {
	return &Config{
		Default: PolicyCreateOnly,
		Policies: map[string]Policy{
			"defaults":   PolicyFullSync,   // Server defaults always from YAML
			"tree.views": PolicyFullSync,   // Views are declarative
			"tree.smart": PolicyFullSync,   // Smart folders are declarative
		},
	}
}

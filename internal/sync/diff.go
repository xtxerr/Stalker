package sync

// =============================================================================
// Diff Engine
// =============================================================================

// DiffEngine calculates the differences between YAML and DB state.
//
// It's generic over any Syncable type, enabling reuse across entity types.
type DiffEngine[T Syncable] struct {
	entityType string
	policy     Policy
}

// NewDiffEngine creates a new diff engine for the given entity type.
func NewDiffEngine[T Syncable](entityType string, policy Policy) *DiffEngine[T] {
	return &DiffEngine[T]{
		entityType: entityType,
		policy:     policy,
	}
}

// Diff calculates the required changes to reconcile YAML state with DB state.
//
// The algorithm:
//  1. Build index of DB entities by key
//  2. For each YAML entity:
//     - If not in DB → CREATE (if policy allows)
//     - If in DB but different → UPDATE (if policy allows)
//     - If in DB and same → SKIP
//  3. For each DB entity not in YAML:
//     - DELETE (only if policy is full-sync)
//
// Returns entries in a deterministic order (same input = same output).
func (d *DiffEngine[T]) Diff(yaml, db []T) []DiffEntry {
	// Build indices for O(1) lookup
	dbByKey := make(map[string]T, len(db))
	for _, entity := range db {
		dbByKey[entity.SyncKey()] = entity
	}

	yamlByKey := make(map[string]T, len(yaml))
	for _, entity := range yaml {
		yamlByKey[entity.SyncKey()] = entity
	}

	var entries []DiffEntry

	// Process YAML entities (creates and updates)
	for _, yamlEntity := range yaml {
		key := yamlEntity.SyncKey()
		dbEntity, existsInDB := dbByKey[key]

		if !existsInDB {
			// New entity - CREATE
			entry := d.diffCreate(yamlEntity)
			entries = append(entries, entry)
		} else {
			// Existing entity - check for UPDATE
			entry := d.diffUpdate(yamlEntity, dbEntity)
			entries = append(entries, entry)
		}
	}

	// Process orphaned DB entities (deletes)
	if ShouldDelete(d.policy) {
		for _, dbEntity := range db {
			key := dbEntity.SyncKey()
			if _, inYAML := yamlByKey[key]; !inYAML {
				entry := d.diffDelete(dbEntity)
				entries = append(entries, entry)
			}
		}
	}

	return entries
}

// diffCreate returns a CREATE entry if policy allows.
func (d *DiffEngine[T]) diffCreate(yaml T) DiffEntry {
	entry := DiffEntry{
		Key:        yaml.SyncKey(),
		EntityType: d.entityType,
		YAMLHash:   yaml.SyncHash(),
	}

	if !ShouldCreate(d.policy) {
		entry.Action = ActionSkip
		entry.Reason = "policy: ignore"
		return entry
	}

	entry.Action = ActionCreate
	entry.Reason = "new entity"
	return entry
}

// diffUpdate returns UPDATE or SKIP entry based on policy and content.
func (d *DiffEngine[T]) diffUpdate(yaml, db T) DiffEntry {
	key := yaml.SyncKey()
	yamlHash := yaml.SyncHash()
	dbHash := db.SyncHash()
	dbSource := db.SyncSource()

	entry := DiffEntry{
		Key:         key,
		EntityType:  d.entityType,
		YAMLHash:    yamlHash,
		DBHash:      dbHash,
		SourceState: dbSource,
	}

	// Content unchanged?
	if yamlHash == dbHash {
		entry.Action = ActionSkip
		entry.Reason = "unchanged"
		return entry
	}

	// Check if update is allowed
	if !ShouldUpdate(d.policy, dbSource) {
		entry.Action = ActionSkip
		switch d.policy {
		case PolicyIgnore:
			entry.Reason = "policy: ignore"
		case PolicyCreateOnly:
			entry.Reason = "policy: create-only"
		case PolicySourceAware:
			entry.Reason = "policy: source-aware (source=" + string(dbSource) + ")"
		default:
			entry.Reason = "update not allowed"
		}
		return entry
	}

	entry.Action = ActionUpdate
	entry.Reason = "content changed"
	return entry
}

// diffDelete returns a DELETE entry.
func (d *DiffEngine[T]) diffDelete(db T) DiffEntry {
	return DiffEntry{
		Action:      ActionDelete,
		Key:         db.SyncKey(),
		EntityType:  d.entityType,
		Reason:      "orphan (not in yaml)",
		SourceState: db.SyncSource(),
		DBHash:      db.SyncHash(),
	}
}

// =============================================================================
// Diff Statistics
// =============================================================================

// DiffStats calculates statistics from diff entries.
type DiffStats struct {
	Creates int
	Updates int
	Deletes int
	Skipped int
	Total   int
}

// CalculateStats returns statistics for a set of diff entries.
func CalculateStats(entries []DiffEntry) DiffStats {
	stats := DiffStats{Total: len(entries)}

	for _, e := range entries {
		switch e.Action {
		case ActionCreate:
			stats.Creates++
		case ActionUpdate:
			stats.Updates++
		case ActionDelete:
			stats.Deletes++
		case ActionSkip:
			stats.Skipped++
		}
	}

	return stats
}

// HasChanges returns true if there are any non-skip actions.
func (s DiffStats) HasChanges() bool {
	return s.Creates > 0 || s.Updates > 0 || s.Deletes > 0
}

// =============================================================================
// Diff Filters
// =============================================================================

// FilterByAction returns only entries with the given action.
func FilterByAction(entries []DiffEntry, action Action) []DiffEntry {
	var filtered []DiffEntry
	for _, e := range entries {
		if e.Action == action {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// FilterActionable returns entries that require database changes.
func FilterActionable(entries []DiffEntry) []DiffEntry {
	var filtered []DiffEntry
	for _, e := range entries {
		if e.Action != ActionSkip {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

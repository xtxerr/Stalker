package sync

import (
	"database/sql"
	"fmt"
)

// =============================================================================
// Schema Migration
// =============================================================================

// MigrateSchema adds sync-related columns to existing tables.
//
// This is idempotent - safe to run multiple times.
// Uses ALTER TABLE with IF NOT EXISTS (DuckDB specific).
//
// Added columns:
//   - source: Where the entity was created ('yaml', 'api', 'import')
//   - content_hash: Hash of entity content for efficient diff
//   - synced_at: When the entity was last synced from YAML
func MigrateSchema(db *sql.DB) error {
	migrations := []struct {
		name string
		sql  string
	}{
		// Namespaces
		{
			name: "namespaces.source",
			sql:  `ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'api'`,
		},
		{
			name: "namespaces.content_hash",
			sql:  `ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS content_hash UBIGINT`,
		},
		{
			name: "namespaces.synced_at",
			sql:  `ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP`,
		},

		// Targets
		{
			name: "targets.source",
			sql:  `ALTER TABLE targets ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'api'`,
		},
		{
			name: "targets.content_hash",
			sql:  `ALTER TABLE targets ADD COLUMN IF NOT EXISTS content_hash UBIGINT`,
		},
		{
			name: "targets.synced_at",
			sql:  `ALTER TABLE targets ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP`,
		},

		// Pollers
		{
			name: "pollers.source",
			sql:  `ALTER TABLE pollers ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'api'`,
		},
		{
			name: "pollers.content_hash",
			sql:  `ALTER TABLE pollers ADD COLUMN IF NOT EXISTS content_hash UBIGINT`,
		},
		{
			name: "pollers.synced_at",
			sql:  `ALTER TABLE pollers ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP`,
		},

		// Tree Nodes
		{
			name: "tree_nodes.source",
			sql:  `ALTER TABLE tree_nodes ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'api'`,
		},
		{
			name: "tree_nodes.content_hash",
			sql:  `ALTER TABLE tree_nodes ADD COLUMN IF NOT EXISTS content_hash UBIGINT`,
		},
		{
			name: "tree_nodes.synced_at",
			sql:  `ALTER TABLE tree_nodes ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP`,
		},

		// Secrets
		{
			name: "secrets.source",
			sql:  `ALTER TABLE secrets ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'api'`,
		},
		{
			name: "secrets.synced_at",
			sql:  `ALTER TABLE secrets ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP`,
		},

		// New table: Tree Views (label-based projections)
		{
			name: "tree_views",
			sql: `CREATE TABLE IF NOT EXISTS tree_views (
				namespace VARCHAR NOT NULL,
				name VARCHAR NOT NULL,
				description VARCHAR,
				path_template VARCHAR NOT NULL,
				entity_type VARCHAR NOT NULL,
				filter_expr VARCHAR,
				priority INTEGER DEFAULT 50,
				enabled BOOLEAN DEFAULT true,
				source VARCHAR DEFAULT 'api',
				content_hash UBIGINT,
				synced_at TIMESTAMP,
				created_at TIMESTAMP DEFAULT now(),
				updated_at TIMESTAMP DEFAULT now(),
				PRIMARY KEY (namespace, name)
			)`,
		},

		// New table: Tree Smart Folders (query-based)
		{
			name: "tree_smart_folders",
			sql: `CREATE TABLE IF NOT EXISTS tree_smart_folders (
				namespace VARCHAR NOT NULL,
				path VARCHAR NOT NULL,
				description VARCHAR,
				target_query VARCHAR,
				poller_query VARCHAR,
				cache_ttl_sec INTEGER DEFAULT 60,
				priority INTEGER DEFAULT 25,
				enabled BOOLEAN DEFAULT true,
				source VARCHAR DEFAULT 'api',
				content_hash UBIGINT,
				synced_at TIMESTAMP,
				created_at TIMESTAMP DEFAULT now(),
				updated_at TIMESTAMP DEFAULT now(),
				PRIMARY KEY (namespace, path)
			)`,
		},

		// New table: Sync State (singleton)
		{
			name: "sync_state",
			sql: `CREATE TABLE IF NOT EXISTS sync_state (
				id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
				last_sync_at TIMESTAMP,
				last_config_hash UBIGINT,
				sync_count INTEGER DEFAULT 0,
				last_result JSON
			)`,
		},
		{
			name: "sync_state.init",
			sql:  `INSERT INTO sync_state (id) VALUES (1) ON CONFLICT DO NOTHING`,
		},

		// Indices for source lookups
		{
			name: "idx_namespaces_source",
			sql:  `CREATE INDEX IF NOT EXISTS idx_namespaces_source ON namespaces(source)`,
		},
		{
			name: "idx_targets_source",
			sql:  `CREATE INDEX IF NOT EXISTS idx_targets_source ON targets(namespace, source)`,
		},
		{
			name: "idx_pollers_source",
			sql:  `CREATE INDEX IF NOT EXISTS idx_pollers_source ON pollers(namespace, target, source)`,
		},
		{
			name: "idx_tree_nodes_source",
			sql:  `CREATE INDEX IF NOT EXISTS idx_tree_nodes_source ON tree_nodes(namespace, source)`,
		},
	}

	for _, m := range migrations {
		if _, err := db.Exec(m.sql); err != nil {
			return fmt.Errorf("migration %s: %w", m.name, err)
		}
		log.Debug("migration applied", "name", m.name)
	}

	log.Info("schema migration completed", "migrations", len(migrations))
	return nil
}

// =============================================================================
// Sync State Persistence
// =============================================================================

// SyncState holds the persistent state of the sync engine.
type SyncState struct {
	LastSyncAt     *sql.NullTime
	LastConfigHash uint64
	SyncCount      int
}

// LoadSyncState loads the current sync state from the database.
func LoadSyncState(db *sql.DB) (*SyncState, error) {
	state := &SyncState{}
	var lastSyncAt sql.NullTime
	var lastConfigHash sql.NullInt64

	err := db.QueryRow(`
		SELECT last_sync_at, last_config_hash, sync_count
		FROM sync_state WHERE id = 1
	`).Scan(&lastSyncAt, &lastConfigHash, &state.SyncCount)

	if err == sql.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load sync state: %w", err)
	}

	if lastSyncAt.Valid {
		state.LastSyncAt = &lastSyncAt
	}
	if lastConfigHash.Valid {
		state.LastConfigHash = uint64(lastConfigHash.Int64)
	}

	return state, nil
}

// SaveSyncState saves the current sync state to the database.
func SaveSyncState(db *sql.DB, configHash uint64) error {
	_, err := db.Exec(`
		UPDATE sync_state 
		SET last_sync_at = now(),
		    last_config_hash = ?,
		    sync_count = sync_count + 1
		WHERE id = 1
	`, int64(configHash))

	if err != nil {
		return fmt.Errorf("save sync state: %w", err)
	}

	return nil
}

// ConfigHashUnchanged returns true if the config hash matches the last sync.
func ConfigHashUnchanged(db *sql.DB, currentHash uint64) (bool, error) {
	state, err := LoadSyncState(db)
	if err != nil {
		return false, err
	}

	return state.LastConfigHash == currentHash, nil
}

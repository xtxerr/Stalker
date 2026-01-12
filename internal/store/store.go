// Package store provides database operations for the stalker application.
//
// This package handles all persistence operations including pollers, secrets,
// samples, and tree structures. It uses DuckDB as the backing database.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"
)

// =============================================================================
// Store Configuration
// =============================================================================

// Config holds store configuration options.
type Config struct {
	// DBPath is the path to the database file.
	// If empty and InMemory is false, defaults to "stalker.db".
	DBPath string

	// SecretKeyPath is the path to the encryption key for secrets.
	// If empty, secret encryption is disabled.
	SecretKeyPath string

	// InMemory uses an in-memory database (for testing).
	// When true, DBPath is ignored.
	InMemory bool

	// DSN is the database connection string (optional).
	// If set, overrides DBPath and InMemory.
	DSN string

	// MaxOpenConns is the maximum number of open connections.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of idle connections.
	MaxIdleConns int

	// ConnMaxLifetime is the maximum lifetime of a connection.
	ConnMaxLifetime time.Duration

	// QueryTimeout is the default timeout for queries.
	QueryTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DBPath:          "stalker.db",
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		QueryTimeout:    30 * time.Second,
	}
}

// getDSN returns the DSN to use for database connection.
func (c *Config) getDSN() string {
	if c.DSN != "" {
		return c.DSN
	}
	if c.InMemory {
		return ":memory:"
	}
	if c.DBPath == "" {
		return "stalker.db"
	}
	return c.DBPath
}

// =============================================================================
// Store
// =============================================================================

// Store provides database operations.
//
// Store is safe for concurrent use.
type Store struct {
	db        *sql.DB
	config    Config
	secretKey []byte // 32-byte AES-256 key for secret encryption
	mu        sync.RWMutex
	closed    bool
}

// New creates a new Store with the given configuration.
func New(cfg *Config) (*Store, error) {
	if cfg == nil {
		defaultCfg := DefaultConfig()
		cfg = &defaultCfg
	}

	dsn := cfg.getDSN()
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Apply connection pool settings
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	// Load secret key if configured
	var secretKey []byte
	if cfg.SecretKeyPath != "" {
		key, err := os.ReadFile(cfg.SecretKeyPath)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("read secret key: %w", err)
		}
		if len(key) != 32 {
			db.Close()
			return nil, fmt.Errorf("secret key must be exactly 32 bytes, got %d", len(key))
		}
		secretKey = key
	}

	return &Store{
		db:        db,
		config:    *cfg,
		secretKey: secretKey,
	}, nil
}

// Close closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	return s.db.Close()
}

// DB returns the underlying database connection.
// Use with caution - prefer using Store methods.
func (s *Store) DB() *sql.DB {
	return s.db
}

// =============================================================================
// Query Helpers
// =============================================================================

// QueryContext executes a query with context and returns rows.
func (s *Store) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query with context and returns a single row.
func (s *Store) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a statement with context.
func (s *Store) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

// =============================================================================
// Health Check
// =============================================================================

// Health checks database connectivity.
func (s *Store) Health(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

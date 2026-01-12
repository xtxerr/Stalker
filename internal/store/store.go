// Package store provides database operations for the stalker application.
//
// This package handles all persistence operations including pollers, secrets,
// samples, and tree structures. It uses DuckDB as the backing database.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// =============================================================================
// Store Configuration
// =============================================================================

// Config holds store configuration options.
type Config struct {
	// DSN is the database connection string.
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
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		QueryTimeout:    30 * time.Second,
	}
}

// =============================================================================
// Store
// =============================================================================

// Store provides database operations.
//
// Store is safe for concurrent use.
type Store struct {
	db     *sql.DB
	config Config
	mu     sync.RWMutex
	closed bool
}

// New creates a new Store with the given configuration.
func New(cfg Config) (*Store, error) {
	db, err := sql.Open("duckdb", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Store{
		db:     db,
		config: cfg,
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
// Transaction Support
// =============================================================================

// Transaction executes a function within a database transaction.
//
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is committed.
func (s *Store) Transaction(fn func(*sql.Tx) error) error {
	return s.TransactionContext(context.Background(), fn)
}

// TransactionContext executes a function within a database transaction with context.
//
// FIX #5/#11: This method supports context cancellation and timeouts.
func (s *Store) TransactionContext(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
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

// Package store - Context-aware Transaction Support
//
// Die anderen Context-aware Methoden (InsertSamplesBatchContext, etc.) sind
// bereits in den jeweiligen Dateien (sample.go, poller.go, etc.) definiert.
//
// 1. Falls store.go eine TransactionContext Methode hat, diese ENTFERNEN
// 2. Falls store.go eine Transaction Methode hat, diese ENTFERNEN
// 3. Diese Datei nach internal/store/ kopieren

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// =============================================================================
// Context Helpers
// =============================================================================

// defaultContext creates a Context with Store.config.QueryTimeout.
func (s *Store) defaultContext() (context.Context, context.CancelFunc) {
	timeout := s.config.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

// =============================================================================
// Enhanced Transaction Support
// =============================================================================

// TransactionContext executes fn within a transaction.
//
func (s *Store) TransactionContext(ctx context.Context, fn func(*sql.Tx) error) error {
	// Check context before starting
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before transaction: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Ensure rollback on panic
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				// Note: In production, use your logging package here instead of fmt
				// e.g.: log.Error("rollback failed during panic recovery", "error", rbErr)
				fmt.Printf("WARN: rollback failed during panic recovery: %v\n", rbErr)
			}
			panic(p) // Re-panic after rollback
		}
	}()

	// Execute the function
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	// CRITICAL: Check context BEFORE commit
	// This prevents committing after a timeout, which could leave the DB
	// in an inconsistent state if the client has already given up.
	if err := ctx.Err(); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed after context cancel: %v (context error: %w)", rbErr, err)
		}
		return fmt.Errorf("context cancelled before commit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Transaction executes fn within a transaction using the default timeout.
func (s *Store) Transaction(fn func(*sql.Tx) error) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.TransactionContext(ctx, fn)
}

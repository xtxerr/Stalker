// Package store - Context-aware Transaction Support
//
// FIX #23: This file provides the corrected TransactionContext implementation.
//
// WICHTIG: Diese Datei enthält NUR TransactionContext und die Helper-Methode.
// Die anderen Context-aware Methoden (InsertSamplesBatchContext, etc.) sind
// bereits in den jeweiligen Dateien (sample.go, poller.go, etc.) definiert.
//
// INSTALLATION:
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
// FIX #23: This implementation checks the context BEFORE commit to prevent
// committing after a timeout has occurred. This is the canonical implementation.
//
// Key safety features:
//   - Checks context before starting transaction
//   - Recovers from panics and rolls back
//   - Checks context AGAIN before commit (critical!)
//   - Logs rollback errors during panic recovery (FIX #24)
//   - Proper error wrapping for debugging
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
			// FIX #24: Log rollback error during panic recovery
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

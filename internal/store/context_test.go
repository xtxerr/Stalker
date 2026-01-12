// Package store - Context Propagation Tests
//
// FIX #10: Tests für Context-Propagation in Store-Operationen.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

// =============================================================================
// Timeout Tests
// =============================================================================

func TestInsertSamplesBatchContext_Timeout(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Sehr kurzer Timeout der sofort abläuft
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	samples := generateTestSamples(100)
	err := store.InsertSamplesBatchContext(ctx, samples)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestBatchUpdatePollerStatesContext_Timeout(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	states := generateTestStates(100)
	err := store.BatchUpdatePollerStatesContext(ctx, states)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestBatchUpdatePollerStatsContext_Timeout(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	stats := generateTestStats(100)
	err := store.BatchUpdatePollerStatsContext(ctx, stats)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// =============================================================================
// Cancellation Tests
// =============================================================================

func TestInsertSamplesBatchContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Sofort abbrechen

	samples := generateTestSamples(100)
	err := store.InsertSamplesBatchContext(ctx, samples)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestBatchUpdatePollerStatesContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	states := generateTestStates(100)
	err := store.BatchUpdatePollerStatesContext(ctx, states)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestTransactionContext_CancelBeforeCommit(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())

	err := store.TransactionContext(ctx, func(tx *sql.Tx) error {
		// Cancel während der Transaktion
		cancel()
		return nil
	})

	// Sollte "context cancelled before commit" zurückgeben
	if err == nil {
		t.Error("expected context error, got nil")
	}
	// Der Fehler sollte "context cancelled" enthalten
	if err != nil && !errors.Is(err, context.Canceled) {
		// Prüfe auch auf String-Match falls wrapped
		if !containsContextCancelled(err) {
			t.Errorf("expected context cancelled error, got %v", err)
		}
	}
}

func containsContextCancelled(err error) bool {
	return err != nil && (errors.Is(err, context.Canceled) || 
		errors.Is(err, context.DeadlineExceeded) ||
		err.Error() == "context cancelled before commit: context canceled")
}

// =============================================================================
// Success Tests
// =============================================================================

func TestInsertSamplesBatchContext_Success(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	samples := generateTestSamples(100)
	err := store.InsertSamplesBatchContext(ctx, samples)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestBatchUpdatePollerStatesContext_Success(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Erst Pollers erstellen
	pollers := generateTestPollers(100)
	if err := store.BulkCreatePollers(pollers); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	states := generateTestStates(100)
	err := store.BatchUpdatePollerStatesContext(ctx, states)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestBatchUpdatePollerStatsContext_Success(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Erst Pollers erstellen
	pollers := generateTestPollers(100)
	if err := store.BulkCreatePollers(pollers); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats := generateTestStats(100)
	err := store.BatchUpdatePollerStatsContext(ctx, stats)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// =============================================================================
// Large Batch Tests (Context Check in Loop)
// =============================================================================

func TestInsertSamplesBatchContext_LargeBatch_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Großer Batch - sollte Context in Loop prüfen
	samples := generateTestSamples(1000)

	ctx, cancel := context.WithCancel(context.Background())

	// Start insert in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- store.InsertSamplesBatchContext(ctx, samples)
	}()

	// Cancel nach kurzer Zeit
	time.Sleep(10 * time.Millisecond)
	cancel()

	err := <-errCh

	// Sollte entweder cancelled sein oder erfolgreich (je nach Timing)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("expected nil or Canceled, got %v", err)
	}
}

// =============================================================================
// Legacy Method Tests (sollten mit defaultContext funktionieren)
// =============================================================================

func TestInsertSamplesBatch_Legacy(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	samples := generateTestSamples(100)
	err := store.InsertSamplesBatch(samples) // Legacy-Methode

	if err != nil {
		t.Errorf("legacy method failed: %v", err)
	}
}

func TestBatchUpdatePollerStates_Legacy(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	pollers := generateTestPollers(100)
	if err := store.BulkCreatePollers(pollers); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	states := generateTestStates(100)
	err := store.BatchUpdatePollerStates(states) // Legacy-Methode

	if err != nil {
		t.Errorf("legacy method failed: %v", err)
	}
}

// =============================================================================
// Bulk Operations Tests
// =============================================================================

func TestBulkCreatePollersContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pollers := generateTestPollers(100)
	err := store.BulkCreatePollersContext(ctx, pollers)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestBulkUpdatePollersContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// Setup
	pollers := generateTestPollers(100)
	if err := store.BulkCreatePollers(pollers); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.BulkUpdatePollersContext(ctx, pollers)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestBulkDeletePollersContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	keys := []string{"ns/target/poller1", "ns/target/poller2"}
	err := store.BulkDeletePollersContext(ctx, keys)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

func TestBulkCreateTargetsContext_Cancellation(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	targets := generateTestTargets(100)
	err := store.BulkCreateTargetsContext(ctx, targets)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func setupTestStore(t *testing.T) *Store {
	t.Helper()
	cfg := Config{
		DSN:          ":memory:",
		QueryTimeout: 30 * time.Second,
	}
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	return store
}

func generateTestSamples(count int) []*Sample {
	samples := make([]*Sample, count)
	now := time.Now().UnixMilli()
	for i := 0; i < count; i++ {
		v := uint64(i * 100)
		samples[i] = &Sample{
			Namespace:    "test",
			Target:       "target1",
			Poller:       fmt.Sprintf("poller%d", i%10),
			TimestampMs:  now + int64(i),
			ValueCounter: &v,
			Valid:        true,
			PollMs:       5,
		}
	}
	return samples
}

func generateTestStates(count int) []*PollerState {
	states := make([]*PollerState, count)
	now := time.Now()
	for i := 0; i < count; i++ {
		states[i] = &PollerState{
			Namespace:           "test",
			Target:              "target1",
			Poller:              fmt.Sprintf("poller%d", i),
			OperState:           "running",
			HealthState:         "up",
			LastError:           "",
			ConsecutiveFailures: 0,
			LastPollAt:          &now,
			LastSuccessAt:       &now,
		}
	}
	return states
}

func generateTestStats(count int) []*PollerStatsRecord {
	stats := make([]*PollerStatsRecord, count)
	for i := 0; i < count; i++ {
		minMs := 5
		maxMs := 50
		stats[i] = &PollerStatsRecord{
			Namespace:    "test",
			Target:       "target1",
			Poller:       fmt.Sprintf("poller%d", i),
			PollsTotal:   100,
			PollsSuccess: 98,
			PollsFailed:  2,
			PollsTimeout: 0,
			PollMsSum:    1000,
			PollMsMin:    &minMs,
			PollMsMax:    &maxMs,
			PollMsCount:  100,
		}
	}
	return stats
}

func generateTestPollers(count int) []*Poller {
	pollers := make([]*Poller, count)
	for i := 0; i < count; i++ {
		pollers[i] = &Poller{
			Namespace:   "test",
			Target:      "target1",
			Name:        fmt.Sprintf("poller%d", i),
			Description: fmt.Sprintf("Test poller %d", i),
			Protocol:    "snmp",
			AdminState:  "enabled",
			Source:      "yaml",
		}
	}
	return pollers
}

func generateTestTargets(count int) []*Target {
	targets := make([]*Target, count)
	for i := 0; i < count; i++ {
		targets[i] = &Target{
			Namespace:   "test",
			Name:        fmt.Sprintf("target%d", i),
			Description: fmt.Sprintf("Test target %d", i),
			Source:      "yaml",
		}
	}
	return targets
}

// Package testing provides test utilities for the stalker project.
//
// This file contains example tests demonstrating the error channel pattern.
package testing

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestGoroutineTestBasic demonstrates basic usage of GoroutineTest.
func TestGoroutineTestBasic(t *testing.T) {
	gt := NewGoroutineTest(t)
	defer gt.Wait()

	// Run multiple goroutines
	for i := 0; i < 5; i++ {
		i := i // capture loop variable
		gt.Go(func() error {
			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			// Return error instead of t.Fatal
			if i < 0 {
				return fmt.Errorf("unexpected negative index: %d", i)
			}
			return nil
		})
	}
}

// TestGoroutineTestWithContext demonstrates context-aware goroutines.
func TestGoroutineTestWithContext(t *testing.T) {
	gt := NewGoroutineTestWithTimeout(t, 5*time.Second)
	defer gt.Wait()

	gt.GoWithContext(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})
}

// TestParallelRunnerBasic demonstrates the parallel test runner.
func TestParallelRunnerBasic(t *testing.T) {
	runner := NewParallelRunner(t)

	runner.Add("test1", func() error {
		return AssertEqual(1+1, 2, "addition")
	})

	runner.Add("test2", func() error {
		return AssertEqual("hello", "hello", "string equality")
	})

	runner.Add("test3", func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	runner.Run()
}

// TestWithTimeout demonstrates timeout handling.
func TestWithTimeout(t *testing.T) {
	// This should succeed
	err := WithTimeout(1*time.Second, func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// This would timeout (commented out to not slow down tests)
	// err = WithTimeout(10*time.Millisecond, func() error {
	//     time.Sleep(1 * time.Second)
	//     return nil
	// })
	// if err == nil {
	//     t.Error("expected timeout error")
	// }
}

// TestRetry demonstrates retry functionality.
func TestRetry(t *testing.T) {
	var attempts atomic.Int32

	err := Retry(5, 10*time.Millisecond, func() error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("attempt %d failed", n)
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

// TestEventually demonstrates waiting for a condition.
func TestEventually(t *testing.T) {
	var ready atomic.Bool

	// Set ready after a delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		ready.Store(true)
	}()

	err := Eventually(1*time.Second, 20*time.Millisecond, func() bool {
		return ready.Load()
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// =============================================================================
// Example: Scheduler Test Migration
// =============================================================================

// The following examples show how to migrate from t.Fatal to error channel pattern.

// ExampleMigration_Before shows the WRONG way (would hang if poll fails).
//
//	func TestSchedulerPollWrong(t *testing.T) {
//	    var pollCount atomic.Int32
//	    
//	    go func() {
//	        count := pollCount.Add(1)
//	        if count > 100 {
//	            t.Fatal("too many polls") // WRONG! Causes hang
//	        }
//	    }()
//	}

// ExampleMigration_After shows the CORRECT way using error channel.
func ExampleMigration_After(t *testing.T) {
	gt := NewGoroutineTest(t)
	defer gt.Wait()

	var pollCount atomic.Int32

	gt.Go(func() error {
		count := pollCount.Add(1)
		if count > 100 {
			return fmt.Errorf("too many polls: %d", count) // CORRECT!
		}
		return nil
	})
}

// =============================================================================
// Example: Concurrent Worker Test
// =============================================================================

// TestConcurrentWorkers demonstrates testing multiple concurrent workers.
func TestConcurrentWorkers(t *testing.T) {
	gt := NewGoroutineTestWithTimeout(t, 10*time.Second)
	defer gt.Wait()

	results := make(chan int, 10)

	// Start 5 workers
	for i := 0; i < 5; i++ {
		workerID := i
		gt.Go(func() error {
			// Simulate work
			time.Sleep(time.Duration(workerID*10) * time.Millisecond)
			results <- workerID
			return nil
		})
	}

	// Collector goroutine
	gt.Go(func() error {
		collected := 0
		timeout := time.After(5 * time.Second)

		for collected < 5 {
			select {
			case <-results:
				collected++
			case <-timeout:
				return fmt.Errorf("timeout waiting for workers, got %d/5", collected)
			}
		}
		return nil
	})
}

// =============================================================================
// Example: Server Start/Stop Test
// =============================================================================

// TestServerLifecycle demonstrates testing server lifecycle with goroutines.
func TestServerLifecycle(t *testing.T) {
	gt := NewGoroutineTest(t)
	defer gt.Wait()

	// Simulated server state
	var (
		started atomic.Bool
		stopped atomic.Bool
	)

	// Start server in goroutine
	gt.Go(func() error {
		started.Store(true)
		// Simulate server running
		time.Sleep(100 * time.Millisecond)
		stopped.Store(true)
		return nil
	})

	// Wait for server to start
	err := Eventually(1*time.Second, 10*time.Millisecond, func() bool {
		return started.Load()
	})
	if err != nil {
		t.Errorf("server did not start: %v", err)
	}

	// Wait for server to stop
	err = Eventually(1*time.Second, 10*time.Millisecond, func() bool {
		return stopped.Load()
	})
	if err != nil {
		t.Errorf("server did not stop: %v", err)
	}
}

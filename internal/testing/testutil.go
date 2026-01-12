// Package testing provides test utilities for the stalker project.
//
// FIX #18: This package provides utilities for safe testing with goroutines,
// including the error channel pattern to avoid t.Fatal in goroutines.
package testing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Error Channel Pattern (FIX #18)
// =============================================================================

// GoroutineTest provides safe testing utilities for goroutines.
//
// Using t.Fatal or t.FailNow in a goroutine causes the test to hang because
// these functions call runtime.Goexit() which only exits the current goroutine,
// not the test goroutine. This type provides the error channel pattern as a
// safe alternative.
//
// Example usage:
//
//	func TestConcurrentOperations(t *testing.T) {
//	    gt := testing.NewGoroutineTest(t)
//	    defer gt.Wait()
//	
//	    gt.Go(func() error {
//	        result, err := someOperation()
//	        if err != nil {
//	            return fmt.Errorf("operation failed: %w", err)
//	        }
//	        if result != expected {
//	            return fmt.Errorf("got %v, want %v", result, expected)
//	        }
//	        return nil
//	    })
//	}
type GoroutineTest struct {
	t      *testing.T
	wg     sync.WaitGroup
	errors chan error
	ctx    context.Context
	cancel context.CancelFunc
}

// NewGoroutineTest creates a new GoroutineTest helper.
func NewGoroutineTest(t *testing.T) *GoroutineTest {
	ctx, cancel := context.WithCancel(context.Background())
	return &GoroutineTest{
		t:      t,
		errors: make(chan error, 100), // buffered to avoid blocking
		ctx:    ctx,
		cancel: cancel,
	}
}

// NewGoroutineTestWithTimeout creates a GoroutineTest with a timeout.
func NewGoroutineTestWithTimeout(t *testing.T, timeout time.Duration) *GoroutineTest {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &GoroutineTest{
		t:      t,
		errors: make(chan error, 100),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Go runs a function in a goroutine and collects any errors.
//
// The function should return an error instead of calling t.Fatal.
// All errors are collected and reported when Wait() is called.
func (gt *GoroutineTest) Go(fn func() error) {
	gt.wg.Add(1)
	go func() {
		defer gt.wg.Done()
		if err := fn(); err != nil {
			select {
			case gt.errors <- err:
			default:
				// Buffer full, log to prevent blocking
				gt.t.Logf("Error channel full, dropping error: %v", err)
			}
		}
	}()
}

// GoWithContext runs a function with context in a goroutine.
func (gt *GoroutineTest) GoWithContext(fn func(ctx context.Context) error) {
	gt.wg.Add(1)
	go func() {
		defer gt.wg.Done()
		if err := fn(gt.ctx); err != nil {
			select {
			case gt.errors <- err:
			case <-gt.ctx.Done():
				// Context cancelled, ignore error
			}
		}
	}()
}

// Wait waits for all goroutines to complete and fails the test if any errors occurred.
//
// This should be called with defer right after creating the GoroutineTest:
//
//	gt := testing.NewGoroutineTest(t)
//	defer gt.Wait()
func (gt *GoroutineTest) Wait() {
	gt.wg.Wait()
	gt.cancel()
	close(gt.errors)

	var errs []error
	for err := range gt.errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		gt.t.Errorf("Goroutine test failed with %d error(s):", len(errs))
		for i, err := range errs {
			gt.t.Errorf("  [%d] %v", i+1, err)
		}
		gt.t.FailNow()
	}
}

// Context returns the context for this test.
func (gt *GoroutineTest) Context() context.Context {
	return gt.ctx
}

// Cancel cancels the context, signaling goroutines to stop.
func (gt *GoroutineTest) Cancel() {
	gt.cancel()
}

// =============================================================================
// Parallel Test Runner
// =============================================================================

// ParallelRunner runs multiple test cases in parallel.
//
// Example:
//
//	func TestParallel(t *testing.T) {
//	    runner := testing.NewParallelRunner(t)
//	    
//	    runner.Add("case1", func() error {
//	        // test case 1
//	        return nil
//	    })
//	    
//	    runner.Add("case2", func() error {
//	        // test case 2
//	        return nil
//	    })
//	    
//	    runner.Run()
//	}
type ParallelRunner struct {
	t     *testing.T
	cases []testCase
}

type testCase struct {
	name string
	fn   func() error
}

// NewParallelRunner creates a new parallel test runner.
func NewParallelRunner(t *testing.T) *ParallelRunner {
	return &ParallelRunner{t: t}
}

// Add adds a test case to the runner.
func (r *ParallelRunner) Add(name string, fn func() error) {
	r.cases = append(r.cases, testCase{name: name, fn: fn})
}

// Run executes all test cases in parallel and reports any failures.
func (r *ParallelRunner) Run() {
	type result struct {
		name string
		err  error
	}

	results := make(chan result, len(r.cases))
	var wg sync.WaitGroup

	for _, tc := range r.cases {
		wg.Add(1)
		go func(tc testCase) {
			defer wg.Done()
			err := tc.fn()
			results <- result{name: tc.name, err: err}
		}(tc)
	}

	wg.Wait()
	close(results)

	var failures []result
	for res := range results {
		if res.err != nil {
			failures = append(failures, res)
		}
	}

	if len(failures) > 0 {
		r.t.Errorf("Parallel test failed with %d failure(s):", len(failures))
		for _, f := range failures {
			r.t.Errorf("  [%s] %v", f.name, f.err)
		}
		r.t.FailNow()
	}
}

// =============================================================================
// Assertion Helpers
// =============================================================================

// AssertEqual returns an error if got != want.
func AssertEqual[T comparable](got, want T, msg string) error {
	if got != want {
		return fmt.Errorf("%s: got %v, want %v", msg, got, want)
	}
	return nil
}

// AssertNotNil returns an error if v is nil.
func AssertNotNil(v interface{}, msg string) error {
	if v == nil {
		return fmt.Errorf("%s: expected non-nil value", msg)
	}
	return nil
}

// AssertNil returns an error if v is not nil.
func AssertNil(v interface{}, msg string) error {
	if v != nil {
		return fmt.Errorf("%s: expected nil, got %v", msg, v)
	}
	return nil
}

// AssertNoError returns an error if err is not nil.
func AssertNoError(err error, msg string) error {
	if err != nil {
		return fmt.Errorf("%s: unexpected error: %w", msg, err)
	}
	return nil
}

// AssertError returns an error if err is nil.
func AssertError(err error, msg string) error {
	if err == nil {
		return fmt.Errorf("%s: expected error, got nil", msg)
	}
	return nil
}

// =============================================================================
// Example: Scheduler Test with Error Channel Pattern
// =============================================================================

// ExampleSchedulerTest demonstrates the error channel pattern for scheduler tests.
//
// BEFORE (WRONG - t.Fatal in goroutine):
//
//	func TestSchedulerWrong(t *testing.T) {
//	    sched := scheduler.New(cfg)
//	    sched.Start()
//	    defer sched.Stop()
//	
//	    go func() {
//	        result := sched.Poll(key)
//	        if !result.Success {
//	            t.Fatal("poll failed") // WRONG! Causes test to hang
//	        }
//	    }()
//	}
//
// AFTER (CORRECT - error channel pattern):
//
//	func TestSchedulerCorrect(t *testing.T) {
//	    gt := testing.NewGoroutineTest(t)
//	    defer gt.Wait()
//	
//	    sched := scheduler.New(cfg)
//	    sched.Start()
//	    defer sched.Stop()
//	
//	    gt.Go(func() error {
//	        result := sched.Poll(key)
//	        if !result.Success {
//	            return fmt.Errorf("poll failed: %s", result.Error)
//	        }
//	        return nil
//	    })
//	}
func ExampleSchedulerTest() {
	// This is just documentation, not actual test code
}

// =============================================================================
// Test Timeout Helper
// =============================================================================

// WithTimeout runs a function with a timeout.
//
// Example:
//
//	err := testing.WithTimeout(5*time.Second, func() error {
//	    // long running operation
//	    return nil
//	})
//	if err != nil {
//	    t.Fatal(err)
//	}
func WithTimeout(timeout time.Duration, fn func() error) error {
	done := make(chan error, 1)

	go func() {
		done <- fn()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("operation timed out after %v", timeout)
	}
}

// =============================================================================
// Retry Helper
// =============================================================================

// Retry retries a function until it succeeds or max attempts is reached.
//
// Example:
//
//	err := testing.Retry(3, 100*time.Millisecond, func() error {
//	    return checkCondition()
//	})
func Retry(maxAttempts int, delay time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := fn(); err != nil {
			lastErr = err
			time.Sleep(delay)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed after %d attempts: %w", maxAttempts, lastErr)
}

// Eventually waits for a condition to become true.
//
// Example:
//
//	err := testing.Eventually(5*time.Second, 100*time.Millisecond, func() bool {
//	    return server.IsReady()
//	})
func Eventually(timeout, interval time.Duration, condition func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("condition not met within %v", timeout)
}

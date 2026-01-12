// Package testing provides test helpers for the stalker application.
//
// Using t.Fatal() or t.FailNow() in goroutines causes undefined behavior because
// these methods call runtime.Goexit() which only terminates the current goroutine,
// not the test goroutine.
package testing

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Error Channel Pattern (Recommended)
// =============================================================================

// TestHelper manages error collection from goroutines.
//
// Usage:
//
//	func TestConcurrent(t *testing.T) {
//	    h := NewTestHelper(t)
//	    defer h.Wait()
//
//	    for i := 0; i < 10; i++ {
//	        go func(id int) {
//	            defer h.Done()
//	            result, err := doSomething()
//	            if err != nil {
//	                h.Errorf("goroutine %d: %v", id, err)
//	                return
//	            }
//	            if result != expected {
//	                h.Errorf("goroutine %d: got %v, want %v", id, result, expected)
//	            }
//	        }(i)
//	        h.Add(1)
//	    }
//	}
type TestHelper struct {
	t      *testing.T
	wg     sync.WaitGroup
	errors chan error
	done   chan struct{}
}

// NewTestHelper creates a new test helper.
func NewTestHelper(t *testing.T) *TestHelper {
	return &TestHelper{
		t:      t,
		errors: make(chan error, 100),
		done:   make(chan struct{}),
	}
}

// Add increments the goroutine counter.
func (h *TestHelper) Add(delta int) {
	h.wg.Add(delta)
}

// Done decrements the goroutine counter.
func (h *TestHelper) Done() {
	h.wg.Done()
}

// Errorf records a test error from a goroutine.
// This is safe to call from any goroutine.
func (h *TestHelper) Errorf(format string, args ...interface{}) {
	select {
	case h.errors <- fmt.Errorf(format, args...):
	default:
		// Buffer full, error will be lost but test will still fail
	}
}

// Error records a test error from a goroutine.
func (h *TestHelper) Error(err error) {
	if err == nil {
		return
	}
	select {
	case h.errors <- err:
	default:
	}
}

// Wait waits for all goroutines and reports any errors.
// Must be called (typically via defer) to ensure errors are reported.
func (h *TestHelper) Wait() {
	h.wg.Wait()
	close(h.errors)

	var failed bool
	for err := range h.errors {
		h.t.Errorf("goroutine error: %v", err)
		failed = true
	}

	if failed {
		h.t.FailNow()
	}
}

// =============================================================================
// Alternative: Assertion Collector
// =============================================================================

// AssertionCollector collects assertions from multiple goroutines.
//
// Usage:
//
//	func TestParallel(t *testing.T) {
//	    ac := NewAssertionCollector()
//
//	    var wg sync.WaitGroup
//	    for i := 0; i < 5; i++ {
//	        wg.Add(1)
//	        go func(id int) {
//	            defer wg.Done()
//	            ac.Equal(id, "expected", actual, "context")
//	        }(i)
//	    }
//
//	    wg.Wait()
//	    ac.Assert(t) // Reports all failures
//	}
type AssertionCollector struct {
	mu       sync.Mutex
	failures []string
}

// NewAssertionCollector creates a new assertion collector.
func NewAssertionCollector() *AssertionCollector {
	return &AssertionCollector{}
}

// Equal asserts that expected equals actual.
func (ac *AssertionCollector) Equal(id interface{}, expected, actual interface{}, msg string) {
	if expected != actual {
		ac.mu.Lock()
		ac.failures = append(ac.failures,
			fmt.Sprintf("[%v] %s: expected %v, got %v", id, msg, expected, actual))
		ac.mu.Unlock()
	}
}

// True asserts that condition is true.
func (ac *AssertionCollector) True(id interface{}, condition bool, msg string) {
	if !condition {
		ac.mu.Lock()
		ac.failures = append(ac.failures,
			fmt.Sprintf("[%v] %s: expected true", id, msg))
		ac.mu.Unlock()
	}
}

// NoError asserts that err is nil.
func (ac *AssertionCollector) NoError(id interface{}, err error, msg string) {
	if err != nil {
		ac.mu.Lock()
		ac.failures = append(ac.failures,
			fmt.Sprintf("[%v] %s: unexpected error: %v", id, msg, err))
		ac.mu.Unlock()
	}
}

// Assert reports all collected failures.
func (ac *AssertionCollector) Assert(t *testing.T) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	for _, f := range ac.failures {
		t.Error(f)
	}
}

// HasFailures returns true if any assertions failed.
func (ac *AssertionCollector) HasFailures() bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return len(ac.failures) > 0
}

// =============================================================================
// Example Tests (showing correct patterns)
// =============================================================================

// ExampleTestHelper demonstrates the error channel pattern.
func ExampleTestHelper() {
	// This would be in a real test file
	/*
		func TestConcurrentOperations(t *testing.T) {
			h := NewTestHelper(t)
			defer h.Wait()

			for i := 0; i < 10; i++ {
				h.Add(1)
				go func(id int) {
					defer h.Done()

					// Simulate work
					result := id * 2

					// CORRECT: Use h.Errorf instead of t.Fatal
					if result != id*2 {
						h.Errorf("goroutine %d: math is broken", id)
						return
					}
				}(i)
			}
		}
	*/
}

// =============================================================================
// Timeout Helper
// =============================================================================

// RunWithTimeout runs a function with a timeout.
// Returns error if function doesn't complete in time.
// Note: Use WithTimeout from testutil.go if the function returns an error.
func RunWithTimeout(timeout time.Duration, fn func()) error {
	done := make(chan struct{})

	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout after %v", timeout)
	}
}

// =============================================================================
// Bad Patterns (DO NOT USE - for documentation only)
// =============================================================================

/*
BAD PATTERN - DO NOT USE:

func TestBadPattern(t *testing.T) {
    go func() {
        // WRONG: t.Fatal in goroutine causes undefined behavior
        // It only terminates this goroutine, not the test
        if err := doSomething(); err != nil {
            t.Fatal(err)  // ← NEVER DO THIS
        }
    }()
    time.Sleep(time.Second)
}

CORRECT PATTERN:

func TestGoodPattern(t *testing.T) {
    errCh := make(chan error, 1)

    go func() {
        if err := doSomething(); err != nil {
            errCh <- err  // ← Send error to channel
            return
        }
        errCh <- nil
    }()

    select {
    case err := <-errCh:
        if err != nil {
            t.Fatal(err)  // ← Safe: called from test goroutine
        }
    case <-time.After(5 * time.Second):
        t.Fatal("timeout")
    }
}
*/

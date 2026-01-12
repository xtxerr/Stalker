// Package sync provides thread-safe synchronization primitives.
//
// FIX #4: This package provides a thread-safe resettable Once that can be
// safely reset for reconnection scenarios, unlike sync.Once which cannot
// be safely reset while other goroutines might be calling Do().
package sync

import (
	"sync"
	"sync/atomic"
)

// ResettableOnce is like sync.Once but can be safely reset.
//
// Unlike sync.Once, ResettableOnce can be reset to allow the function
// to be called again. This is useful for reconnection scenarios where
// you need to re-run initialization after a disconnect.
//
// ResettableOnce is safe for concurrent use.
//
// Example usage:
//
//	var once ResettableOnce
//
//	// First call executes
//	once.Do(func() { fmt.Println("init") })
//
//	// Second call is a no-op
//	once.Do(func() { fmt.Println("init") })
//
//	// Reset allows the next call to execute
//	once.Reset()
//
//	// This call executes again
//	once.Do(func() { fmt.Println("init") })
type ResettableOnce struct {
	done uint32
	m    sync.Mutex
}

// Do calls the function f if and only if Do has not been called
// since the last Reset (or ever, if Reset was never called).
//
// If multiple goroutines call Do simultaneously, only one will execute f.
// The other calls block until f returns, then return without calling f.
func (o *ResettableOnce) Do(f func()) {
	// Fast path: check if already done
	if atomic.LoadUint32(&o.done) == 1 {
		return
	}

	// Slow path: acquire lock and double-check
	o.m.Lock()
	defer o.m.Unlock()

	if o.done == 0 {
		defer atomic.StoreUint32(&o.done, 1)
		f()
	}
}

// DoWithError calls the function f if and only if Do has not been called
// since the last Reset. Returns the error from f.
//
// If f returns an error, the Once is NOT marked as done, allowing retry.
// This is useful for initialization that might fail temporarily.
func (o *ResettableOnce) DoWithError(f func() error) error {
	// Fast path: check if already done
	if atomic.LoadUint32(&o.done) == 1 {
		return nil
	}

	// Slow path: acquire lock and double-check
	o.m.Lock()
	defer o.m.Unlock()

	if o.done == 0 {
		if err := f(); err != nil {
			return err
		}
		atomic.StoreUint32(&o.done, 1)
	}

	return nil
}

// Reset allows Do to be called again.
//
// This is safe to call concurrently with Do. If a Do is in progress,
// Reset will block until it completes, then reset the state.
//
// After Reset returns, the next call to Do will execute its function.
func (o *ResettableOnce) Reset() {
	o.m.Lock()
	defer o.m.Unlock()
	atomic.StoreUint32(&o.done, 0)
}

// Done returns true if Do has been called and completed successfully
// since the last Reset.
func (o *ResettableOnce) Done() bool {
	return atomic.LoadUint32(&o.done) == 1
}

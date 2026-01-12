package sync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResettableOnce_Do(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32

	once.Do(func() { count.Add(1) })
	if c := count.Load(); c != 1 {
		t.Errorf("first Do: count = %d, want 1", c)
	}

	once.Do(func() { count.Add(1) })
	if c := count.Load(); c != 1 {
		t.Errorf("second Do: count = %d, want 1", c)
	}
}

func TestResettableOnce_Reset(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32

	once.Do(func() { count.Add(1) })
	once.Reset()
	once.Do(func() { count.Add(1) })

	if c := count.Load(); c != 2 {
		t.Errorf("after reset: count = %d, want 2", c)
	}
}

func TestResettableOnce_Done(t *testing.T) {
	var once ResettableOnce

	if once.Done() {
		t.Error("Done() should be false initially")
	}

	once.Do(func() {})
	if !once.Done() {
		t.Error("Done() should be true after Do")
	}

	once.Reset()
	if once.Done() {
		t.Error("Done() should be false after Reset")
	}
}

func TestResettableOnce_DoWithError(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32
	testErr := &testError{}

	err := once.DoWithError(func() error {
		count.Add(1)
		return testErr
	})

	if err != testErr {
		t.Errorf("DoWithError returned %v, want %v", err, testErr)
	}
	if once.Done() {
		t.Error("Done() should be false after error")
	}

	err = once.DoWithError(func() error {
		count.Add(1)
		return nil
	})

	if err != nil {
		t.Errorf("DoWithError returned %v, want nil", err)
	}
	if c := count.Load(); c != 2 {
		t.Errorf("count = %d, want 2", c)
	}
	if !once.Done() {
		t.Error("Done() should be true after success")
	}
}

type testError struct{}

func (e *testError) Error() string { return "test error" }

func TestResettableOnce_Concurrent(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			once.Do(func() { count.Add(1) })
		}()
	}

	wg.Wait()

	if c := count.Load(); c != 1 {
		t.Errorf("concurrent Do: count = %d, want 1", c)
	}
}

func TestResettableOnce_ConcurrentReset(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32

	const cycles = 10
	const goroutines = 10

	for cycle := 0; cycle < cycles; cycle++ {
		once.Reset()

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				once.Do(func() { count.Add(1) })
			}()
		}

		wg.Wait()
	}

	if c := count.Load(); c != cycles {
		t.Errorf("concurrent reset: count = %d, want %d", c, cycles)
	}
}

func TestResettableOnce_ResetDuringDo(t *testing.T) {
	var once ResettableOnce
	var count atomic.Int32
	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		once.Do(func() {
			close(started)
			time.Sleep(50 * time.Millisecond)
			count.Add(1)
		})
		close(done)
	}()

	<-started

	resetDone := make(chan struct{})
	go func() {
		once.Reset()
		close(resetDone)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-resetDone:
		t.Error("Reset returned before Do completed")
	default:
	}

	<-done
	<-resetDone

	once.Do(func() { count.Add(1) })

	if c := count.Load(); c != 2 {
		t.Errorf("count = %d, want 2", c)
	}
}

func BenchmarkResettableOnce_Do(b *testing.B) {
	var once ResettableOnce
	once.Do(func() {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		once.Do(func() {})
	}
}

func BenchmarkResettableOnce_Parallel(b *testing.B) {
	var once ResettableOnce
	once.Do(func() {})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			once.Do(func() {})
		}
	})
}

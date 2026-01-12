package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestParsePollerKey(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    PollerKey
		wantErr bool
	}{
		{
			name:  "valid key",
			input: "ns1/target1/poller1",
			want:  PollerKey{Namespace: "ns1", Target: "target1", Poller: "poller1"},
		},
		{
			name:  "key with special chars",
			input: "prod-ns/my-target/cpu_usage",
			want:  PollerKey{Namespace: "prod-ns", Target: "my-target", Poller: "cpu_usage"},
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing parts",
			input:   "ns1/target1",
			wantErr: true,
		},
		{
			name:    "too few slashes",
			input:   "ns1target1poller1",
			wantErr: true,
		},
		{
			name:    "empty namespace",
			input:   "/target1/poller1",
			wantErr: true,
		},
		{
			name:    "empty target",
			input:   "ns1//poller1",
			wantErr: true,
		},
		{
			name:    "empty poller",
			input:   "ns1/target1/",
			wantErr: true,
		},
		{
			name:  "poller with slash",
			input: "ns1/target1/poller/with/slashes",
			want:  PollerKey{Namespace: "ns1", Target: "target1", Poller: "poller/with/slashes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePollerKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePollerKey(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParsePollerKey(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestPollerKeyString(t *testing.T) {
	key := PollerKey{Namespace: "ns1", Target: "target1", Poller: "poller1"}
	want := "ns1/target1/poller1"
	if got := key.String(); got != want {
		t.Errorf("PollerKey.String() = %q, want %q", got, want)
	}
}

func TestPollerKeyRoundTrip(t *testing.T) {
	original := PollerKey{Namespace: "prod", Target: "router-1", Poller: "cpu"}
	str := original.String()
	parsed, err := ParsePollerKey(str)
	if err != nil {
		t.Fatalf("ParsePollerKey(%q) error = %v", str, err)
	}
	if parsed != original {
		t.Errorf("Round trip failed: got %v, want %v", parsed, original)
	}
}

func TestSchedulerBasic(t *testing.T) {
	sched := New(&Config{
		Workers:   2,
		QueueSize: 100,
	})

	var pollCount atomic.Int32

	// FIX: Use correct signature with context.Context
	sched.SetPollFunc(func(ctx context.Context, key PollerKey) PollResult {
		pollCount.Add(1)
		return PollResult{
			Key:         key,
			TimestampMs: time.Now().UnixMilli(),
			Success:     true,
		}
	})

	sched.Start()
	defer sched.Stop()

	// Add poller with 50ms interval
	key := PollerKey{Namespace: "ns", Target: "target", Poller: "poller"}
	sched.Add(key, 50)

	// Wait for a few polls
	time.Sleep(200 * time.Millisecond)

	count := pollCount.Load()
	if count < 2 {
		t.Errorf("Expected at least 2 polls, got %d", count)
	}

	// Check stats
	heapSize, queueUsed, active, backpressure := sched.Stats()
	_ = queueUsed
	_ = active
	_ = backpressure
	if heapSize != 1 {
		t.Errorf("Stats: heap=%d, want 1", heapSize)
	}

	// Remove
	sched.Remove(key)

	// Give some time for cleanup
	time.Sleep(50 * time.Millisecond)

	heapSize, _, _, _ = sched.Stats()
	if heapSize != 0 {
		t.Errorf("After remove: heap=%d, want 0", heapSize)
	}
}

func TestSchedulerRemoveDuringPolling(t *testing.T) {
	// This test verifies FIX #21 - memory leak when removing during polling
	sched := New(&Config{
		Workers:   1, // Single worker to control timing
		QueueSize: 100,
	})

	pollStarted := make(chan struct{})
	pollContinue := make(chan struct{})

	sched.SetPollFunc(func(ctx context.Context, key PollerKey) PollResult {
		close(pollStarted) // Signal that poll has started
		<-pollContinue     // Wait for signal to continue
		return PollResult{
			Key:         key,
			TimestampMs: time.Now().UnixMilli(),
			Success:     true,
		}
	})

	sched.Start()
	defer sched.Stop()

	key := PollerKey{Namespace: "ns", Target: "target", Poller: "poller"}
	sched.Add(key, 10) // Very short interval to trigger quickly

	// Wait for poll to start
	<-pollStarted

	// Remove while polling is in progress
	sched.Remove(key)

	// Let poll complete
	close(pollContinue)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Verify: item should be completely cleaned up
	heapSize, _, _, _ := sched.Stats()
	if heapSize != 0 {
		t.Errorf("Memory leak: heap size = %d after remove during polling, want 0", heapSize)
	}

	// Also verify Contains returns false
	if sched.Contains(key) {
		t.Error("Contains() returned true for removed key")
	}
}

func TestSchedulerUpdateInterval(t *testing.T) {
	sched := New(&Config{
		Workers:   2,
		QueueSize: 100,
	})

	var pollCount atomic.Int32

	sched.SetPollFunc(func(ctx context.Context, key PollerKey) PollResult {
		pollCount.Add(1)
		return PollResult{
			Key:         key,
			TimestampMs: time.Now().UnixMilli(),
			Success:     true,
		}
	})

	sched.Start()
	defer sched.Stop()

	key := PollerKey{Namespace: "ns", Target: "target", Poller: "poller"}

	// Start with 500ms interval
	sched.Add(key, 500)
	time.Sleep(100 * time.Millisecond)

	// Update to 50ms interval
	sched.UpdateInterval(key, 50)

	// Wait and check
	time.Sleep(200 * time.Millisecond)
	count := pollCount.Load()

	// Should have polled more than once with faster interval
	if count < 2 {
		t.Errorf("Expected more polls after interval update, got %d", count)
	}
}

func TestSchedulerMultiplePollers(t *testing.T) {
	sched := New(&Config{
		Workers:   4,
		QueueSize: 100,
	})

	pollCounts := make(map[string]*atomic.Int32)

	sched.SetPollFunc(func(ctx context.Context, key PollerKey) PollResult {
		keyStr := key.String()
		if counter, ok := pollCounts[keyStr]; ok {
			counter.Add(1)
		}
		return PollResult{
			Key:         key,
			TimestampMs: time.Now().UnixMilli(),
			Success:     true,
		}
	})

	sched.Start()
	defer sched.Stop()

	// Add multiple pollers
	keys := []PollerKey{
		{Namespace: "ns1", Target: "t1", Poller: "p1"},
		{Namespace: "ns1", Target: "t1", Poller: "p2"},
		{Namespace: "ns1", Target: "t2", Poller: "p1"},
		{Namespace: "ns2", Target: "t1", Poller: "p1"},
	}

	for i, k := range keys {
		pollCounts[k.String()] = &atomic.Int32{}
		sched.Add(k, uint32(50+i*10)) // Slightly different intervals
	}

	// Wait for polls
	time.Sleep(300 * time.Millisecond)

	// All should have been polled at least once
	for _, k := range keys {
		keyStr := k.String()
		if pollCounts[keyStr].Load() < 1 {
			t.Errorf("Poller %s was not polled", keyStr)
		}
	}

	// Cleanup
	for _, k := range keys {
		sched.Remove(k)
	}
}

func TestSchedulerContains(t *testing.T) {
	sched := New(&Config{
		Workers:   1,
		QueueSize: 10,
	})

	key := PollerKey{Namespace: "ns", Target: "t", Poller: "p"}

	// Should not contain before adding
	if sched.Contains(key) {
		t.Error("Contains() returned true before Add()")
	}

	sched.Add(key, 1000)

	// Should contain after adding
	if !sched.Contains(key) {
		t.Error("Contains() returned false after Add()")
	}

	sched.Remove(key)

	// Should not contain after removing
	if sched.Contains(key) {
		t.Error("Contains() returned true after Remove()")
	}
}

func TestSchedulerCount(t *testing.T) {
	sched := New(&Config{
		Workers:   1,
		QueueSize: 10,
	})

	if got := sched.Count(); got != 0 {
		t.Errorf("Count() = %d before adding, want 0", got)
	}

	keys := []PollerKey{
		{Namespace: "ns1", Target: "t1", Poller: "p1"},
		{Namespace: "ns1", Target: "t1", Poller: "p2"},
		{Namespace: "ns2", Target: "t1", Poller: "p1"},
	}

	for _, k := range keys {
		sched.Add(k, 1000)
	}

	if got := sched.Count(); got != 3 {
		t.Errorf("Count() = %d after adding 3, want 3", got)
	}

	sched.Remove(keys[0])

	if got := sched.Count(); got != 2 {
		t.Errorf("Count() = %d after removing 1, want 2", got)
	}
}

func BenchmarkParsePollerKey(b *testing.B) {
	input := "production/core-router-01/interface-traffic"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParsePollerKey(input)
	}
}

func BenchmarkPollerKeyString(b *testing.B) {
	key := PollerKey{Namespace: "production", Target: "core-router-01", Poller: "interface-traffic"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = key.String()
	}
}

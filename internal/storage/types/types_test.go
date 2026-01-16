package types

import (
	"testing"
	"time"
)

func TestSampleKey(t *testing.T) {
	s := Sample{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "ifInOctets-Gi0-0",
	}

	expected := "prod/router-01/ifInOctets-Gi0-0"
	if s.Key() != expected {
		t.Errorf("expected %s, got %s", expected, s.Key())
	}
}

func TestSampleTimestampTime(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	s := Sample{
		TimestampMs: now.UnixMilli(),
	}

	if !s.TimestampTime().Equal(now) {
		t.Errorf("expected %v, got %v", now, s.TimestampTime())
	}
}

func TestSampleBatch(t *testing.T) {
	batch := NewSampleBatch(10)

	if batch.Len() != 0 {
		t.Errorf("expected empty batch")
	}

	batch.Add(Sample{Namespace: "test", Target: "t1", Poller: "p1"})
	batch.Add(Sample{Namespace: "test", Target: "t2", Poller: "p2"})

	if batch.Len() != 2 {
		t.Errorf("expected 2 samples, got %d", batch.Len())
	}

	batch.Clear()
	if batch.Len() != 0 {
		t.Errorf("expected empty batch after clear")
	}
}

func TestAggregateResultKey(t *testing.T) {
	a := AggregateResult{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "cpu",
	}

	expected := "prod/router-01/cpu"
	if a.Key() != expected {
		t.Errorf("expected %s, got %s", expected, a.Key())
	}
}

func TestAggregateResultPercentiles(t *testing.T) {
	a := AggregateResult{}

	if a.HasPercentiles() {
		t.Error("expected no percentiles")
	}

	a.SetPercentiles(50.0, 90.0, 95.0, 99.0)

	if !a.HasPercentiles() {
		t.Error("expected percentiles")
	}

	if *a.P50 != 50.0 {
		t.Errorf("expected P50=50.0, got %v", *a.P50)
	}
	if *a.P95 != 95.0 {
		t.Errorf("expected P95=95.0, got %v", *a.P95)
	}
}

func TestTierString(t *testing.T) {
	tests := []struct {
		tier     Tier
		expected string
	}{
		{TierRaw, "raw"},
		{Tier5Min, "5min"},
		{TierHourly, "hourly"},
		{TierDaily, "daily"},
		{TierWeekly, "weekly"},
	}

	for _, tt := range tests {
		if tt.tier.String() != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.tier.String())
		}
	}
}

func TestTierDuration(t *testing.T) {
	tests := []struct {
		tier     Tier
		expected time.Duration
	}{
		{TierRaw, 25 * time.Second},
		{Tier5Min, 5 * time.Minute},
		{TierHourly, time.Hour},
		{TierDaily, 24 * time.Hour},
		{TierWeekly, 7 * 24 * time.Hour},
	}

	for _, tt := range tests {
		if tt.tier.Duration() != tt.expected {
			t.Errorf("tier %s: expected %v, got %v", tt.tier, tt.expected, tt.tier.Duration())
		}
	}
}

func TestTierNext(t *testing.T) {
	tests := []struct {
		tier     Tier
		expected Tier
	}{
		{TierRaw, Tier5Min},
		{Tier5Min, TierHourly},
		{TierHourly, TierDaily},
		{TierDaily, TierWeekly},
		{TierWeekly, TierWeekly}, // No higher
	}

	for _, tt := range tests {
		if tt.tier.Next() != tt.expected {
			t.Errorf("tier %s: expected next %s, got %s", tt.tier, tt.expected, tt.tier.Next())
		}
	}
}

func TestTierTruncateToBucket(t *testing.T) {
	// Test 5-minute truncation
	ts := time.Date(2026, 1, 15, 10, 37, 45, 0, time.UTC)

	fiveMin := Tier5Min.TruncateToBucket(ts)
	expected := time.Date(2026, 1, 15, 10, 35, 0, 0, time.UTC)
	if !fiveMin.Equal(expected) {
		t.Errorf("5min: expected %v, got %v", expected, fiveMin)
	}

	// Test hourly truncation
	hourly := TierHourly.TruncateToBucket(ts)
	expected = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	if !hourly.Equal(expected) {
		t.Errorf("hourly: expected %v, got %v", expected, hourly)
	}

	// Test daily truncation
	daily := TierDaily.TruncateToBucket(ts)
	expected = time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !daily.Equal(expected) {
		t.Errorf("daily: expected %v, got %v", expected, daily)
	}

	// Test weekly truncation (should be Monday)
	// 2026-01-15 is a Thursday, so Monday is 2026-01-12
	weekly := TierWeekly.TruncateToBucket(ts)
	expected = time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	if !weekly.Equal(expected) {
		t.Errorf("weekly: expected %v, got %v", expected, weekly)
	}
}

func TestSelectTierForRange(t *testing.T) {
	now := time.Now()

	tests := []struct {
		duration time.Duration
		expected Tier
	}{
		{1 * time.Hour, TierRaw},
		{24 * time.Hour, TierRaw},
		{48 * time.Hour, TierRaw},
		{49 * time.Hour, Tier5Min},
		{7 * 24 * time.Hour, Tier5Min},
		{30 * 24 * time.Hour, Tier5Min},
		{31 * 24 * time.Hour, TierHourly},
		{90 * 24 * time.Hour, TierHourly},
		{91 * 24 * time.Hour, TierDaily},
		{365 * 24 * time.Hour, TierDaily},
		{2 * 365 * 24 * time.Hour, TierDaily},
		{3 * 365 * 24 * time.Hour, TierWeekly},
	}

	for _, tt := range tests {
		start := now.Add(-tt.duration)
		result := SelectTierForRange(start, now)
		if result != tt.expected {
			t.Errorf("duration %v: expected %s, got %s", tt.duration, tt.expected, result)
		}
	}
}

func TestParseTier(t *testing.T) {
	tests := []struct {
		input    string
		expected Tier
		hasError bool
	}{
		{"raw", TierRaw, false},
		{"5min", Tier5Min, false},
		{"hourly", TierHourly, false},
		{"daily", TierDaily, false},
		{"weekly", TierWeekly, false},
		{"invalid", TierRaw, true},
	}

	for _, tt := range tests {
		result, err := ParseTier(tt.input)
		if tt.hasError && err == nil {
			t.Errorf("expected error for input %s", tt.input)
		}
		if !tt.hasError && err != nil {
			t.Errorf("unexpected error for input %s: %v", tt.input, err)
		}
		if !tt.hasError && result != tt.expected {
			t.Errorf("input %s: expected %s, got %s", tt.input, tt.expected, result)
		}
	}
}

func TestAllTiers(t *testing.T) {
	tiers := AllTiers()
	if len(tiers) != 5 {
		t.Errorf("expected 5 tiers, got %d", len(tiers))
	}

	expected := []Tier{TierRaw, Tier5Min, TierHourly, TierDaily, TierWeekly}
	for i, tier := range tiers {
		if tier != expected[i] {
			t.Errorf("index %d: expected %s, got %s", i, expected[i], tier)
		}
	}
}

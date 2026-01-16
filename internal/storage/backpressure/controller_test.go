package backpressure

import (
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestLevel_String(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelNormal, "normal"},
		{LevelWarning, "warning"},
		{LevelCritical, "critical"},
		{LevelEmergency, "emergency"},
	}

	for _, tt := range tests {
		if tt.level.String() != tt.expected {
			t.Errorf("level %d: expected %s, got %s", tt.level, tt.expected, tt.level.String())
		}
	}
}

func TestController_New(t *testing.T) {
	buf := buffer.New(1000)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = true

	c := New(cfg, buf)

	if c == nil {
		t.Fatal("controller is nil")
	}

	if c.CurrentLevel() != LevelNormal {
		t.Errorf("expected initial level normal, got %s", c.CurrentLevel())
	}
}

func TestController_Check(t *testing.T) {
	buf := buffer.New(100)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.Thresholds.Warning = 0.50
	cfg.Backpressure.Thresholds.Critical = 0.80
	cfg.Backpressure.Thresholds.Emergency = 0.95
	cfg.Backpressure.Recovery.Cooldown = 0 // Disable cooldown for testing

	c := New(cfg, buf)

	// Initially normal
	level := c.Check()
	if level != LevelNormal {
		t.Errorf("expected normal, got %s", level)
	}

	// Fill to 50% - should trigger warning
	now := time.Now().UnixMilli()
	for i := 0; i < 50; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	level = c.Check()
	if level != LevelWarning {
		t.Errorf("expected warning at 50%%, got %s (usage: %.2f)", level, buf.UsageRatio())
	}

	// Fill to 80% - should trigger critical
	for i := 50; i < 80; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	level = c.Check()
	if level != LevelCritical {
		t.Errorf("expected critical at 80%%, got %s (usage: %.2f)", level, buf.UsageRatio())
	}

	// Fill to 95% - should trigger emergency
	for i := 80; i < 95; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	level = c.Check()
	if level != LevelEmergency {
		t.Errorf("expected emergency at 95%%, got %s (usage: %.2f)", level, buf.UsageRatio())
	}
}

func TestController_Hysteresis(t *testing.T) {
	buf := buffer.New(100)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.Thresholds.Warning = 0.50
	cfg.Backpressure.Thresholds.Critical = 0.80
	cfg.Backpressure.Thresholds.Emergency = 0.95
	cfg.Backpressure.Recovery.Hysteresis = 0.10
	cfg.Backpressure.Recovery.Cooldown = 0

	c := New(cfg, buf)

	// Fill to 55% - trigger warning
	now := time.Now().UnixMilli()
	for i := 0; i < 55; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	level := c.Check()
	if level != LevelWarning {
		t.Errorf("expected warning at 55%%, got %s", level)
	}

	// Drop to 45% - should stay in warning due to hysteresis (threshold - hysteresis = 40%)
	buf.PopN(10) // Now at 45%

	level = c.Check()
	if level != LevelWarning {
		t.Errorf("expected warning to persist at 45%% (hysteresis), got %s", level)
	}

	// Drop below hysteresis threshold (40%)
	buf.PopN(10) // Now at 35%

	level = c.Check()
	if level != LevelNormal {
		t.Errorf("expected normal at 35%%, got %s", level)
	}
}

func TestController_ThrottleFactor(t *testing.T) {
	buf := buffer.New(1000)
	cfg := config.DefaultConfig()
	c := New(cfg, buf)

	tests := []struct {
		level    Level
		expected float64
	}{
		{LevelNormal, 1.0},
		{LevelWarning, 0.9},
		{LevelCritical, 0.5},
		{LevelEmergency, 0.1},
	}

	for _, tt := range tests {
		c.level.Store(int32(tt.level))
		factor := c.ThrottleFactor()
		if factor != tt.expected {
			t.Errorf("level %s: expected factor %f, got %f", tt.level, tt.expected, factor)
		}
	}
}

func TestController_ShouldDrop(t *testing.T) {
	buf := buffer.New(1000)
	cfg := config.DefaultConfig()
	c := New(cfg, buf)

	// Normal - should not drop
	c.level.Store(int32(LevelNormal))
	if c.ShouldDrop() {
		t.Error("should not drop at normal level")
	}

	// Emergency - should drop
	c.level.Store(int32(LevelEmergency))
	if !c.ShouldDrop() {
		t.Error("should drop at emergency level")
	}
}

func TestController_ShouldThrottle(t *testing.T) {
	buf := buffer.New(1000)
	cfg := config.DefaultConfig()
	c := New(cfg, buf)

	tests := []struct {
		level    Level
		expected bool
	}{
		{LevelNormal, false},
		{LevelWarning, false},
		{LevelCritical, true},
		{LevelEmergency, true},
	}

	for _, tt := range tests {
		c.level.Store(int32(tt.level))
		if c.ShouldThrottle() != tt.expected {
			t.Errorf("level %s: expected throttle=%v", tt.level, tt.expected)
		}
	}
}

func TestController_ShouldPauseCompaction(t *testing.T) {
	buf := buffer.New(1000)
	cfg := config.DefaultConfig()
	c := New(cfg, buf)

	tests := []struct {
		level    Level
		expected bool
	}{
		{LevelNormal, false},
		{LevelWarning, true},
		{LevelCritical, true},
		{LevelEmergency, true},
	}

	for _, tt := range tests {
		c.level.Store(int32(tt.level))
		if c.ShouldPauseCompaction() != tt.expected {
			t.Errorf("level %s: expected pause=%v", tt.level, tt.expected)
		}
	}
}

func TestController_OnLevelChange(t *testing.T) {
	buf := buffer.New(100)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.Thresholds.Warning = 0.50
	cfg.Backpressure.Recovery.Cooldown = 0

	c := New(cfg, buf)

	var callbackCalled bool
	var oldLevel, newLevel Level

	c.SetOnLevelChange(func(old, new Level) {
		callbackCalled = true
		oldLevel = old
		newLevel = new
	})

	// Trigger level change
	now := time.Now().UnixMilli()
	for i := 0; i < 55; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	c.Check()

	if !callbackCalled {
		t.Error("callback should have been called")
	}

	if oldLevel != LevelNormal {
		t.Errorf("expected old level normal, got %s", oldLevel)
	}

	if newLevel != LevelWarning {
		t.Errorf("expected new level warning, got %s", newLevel)
	}
}

func TestController_Stats(t *testing.T) {
	buf := buffer.New(100)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.Thresholds.Warning = 0.50
	cfg.Backpressure.Recovery.Cooldown = 0

	c := New(cfg, buf)

	// Trigger level change
	now := time.Now().UnixMilli()
	for i := 0; i < 55; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	c.Check()

	// Record some drops
	c.RecordDrop()
	c.RecordDrop()

	stats := c.Stats()

	if stats.CurrentLevel != LevelWarning {
		t.Errorf("expected warning level, got %s", stats.CurrentLevel)
	}

	if stats.LevelChanges != 1 {
		t.Errorf("expected 1 level change, got %d", stats.LevelChanges)
	}

	if stats.WarningCount != 1 {
		t.Errorf("expected 1 warning count, got %d", stats.WarningCount)
	}

	if stats.SamplesDropped != 2 {
		t.Errorf("expected 2 samples dropped, got %d", stats.SamplesDropped)
	}
}

func TestController_Disabled(t *testing.T) {
	buf := buffer.New(100)

	cfg := config.DefaultConfig()
	cfg.Backpressure.Enabled = false

	c := New(cfg, buf)

	// Fill buffer completely
	now := time.Now().UnixMilli()
	for i := 0; i < 100; i++ {
		buf.Push(types.Sample{TimestampMs: now + int64(i), Valid: true})
	}

	// Should always return normal when disabled
	level := c.Check()
	if level != LevelNormal {
		t.Errorf("expected normal when disabled, got %s", level)
	}

	if !c.IsEnabled() == cfg.Backpressure.Enabled {
		t.Error("IsEnabled mismatch")
	}
}

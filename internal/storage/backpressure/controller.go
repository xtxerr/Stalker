package backpressure

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/config"
)

// Level represents the current backpressure level.
type Level int

const (
	// LevelNormal - system operating normally.
	LevelNormal Level = iota

	// LevelWarning - elevated load, pause non-critical operations.
	LevelWarning

	// LevelCritical - high load, throttle incoming requests.
	LevelCritical

	// LevelEmergency - overload, drop samples to prevent system failure.
	LevelEmergency
)

// String returns the string representation of the level.
func (l Level) String() string {
	switch l {
	case LevelNormal:
		return "normal"
	case LevelWarning:
		return "warning"
	case LevelCritical:
		return "critical"
	case LevelEmergency:
		return "emergency"
	default:
		return "unknown"
	}
}

// Controller manages backpressure based on buffer utilization.
type Controller struct {
	mu sync.RWMutex

	config *config.Config
	buffer *buffer.RingBuffer

	// Current state
	level     atomic.Int32
	lastCheck time.Time
	lastLevel Level

	// Statistics
	stats Stats

	// Level change callback
	onLevelChange func(old, new Level)
}

// Stats holds backpressure statistics.
type Stats struct {
	LevelChanges     int64
	WarningCount     int64
	CriticalCount    int64
	EmergencyCount   int64
	SamplesDropped   int64
	ThrottleSeconds  float64
}

// New creates a new backpressure controller.
func New(cfg *config.Config, buf *buffer.RingBuffer) *Controller {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	c := &Controller{
		config:    cfg,
		buffer:    buf,
		lastCheck: time.Now(),
	}

	return c
}

// SetOnLevelChange sets the callback for level changes.
func (c *Controller) SetOnLevelChange(fn func(old, new Level)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onLevelChange = fn
}

// Check evaluates current conditions and updates the level.
// This should be called periodically.
func (c *Controller) Check() Level {
	if !c.config.Backpressure.Enabled {
		return LevelNormal
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Respect cooldown
	if now.Sub(c.lastCheck) < c.config.Backpressure.Recovery.Cooldown {
		return Level(c.level.Load())
	}

	c.lastCheck = now

	// Get buffer utilization
	usage := c.buffer.UsageRatio()

	// Determine new level with hysteresis
	newLevel := c.determineLevel(usage)

	// Update level if changed
	if newLevel != c.lastLevel {
		c.setLevel(newLevel)
	}

	return newLevel
}

// determineLevel determines the backpressure level based on usage.
func (c *Controller) determineLevel(usage float64) Level {
	thresholds := c.config.Backpressure.Thresholds
	hysteresis := c.config.Backpressure.Recovery.Hysteresis
	currentLevel := c.lastLevel

	// Going up (increasing pressure)
	if usage >= thresholds.Emergency {
		return LevelEmergency
	}
	if usage >= thresholds.Critical {
		return LevelCritical
	}
	if usage >= thresholds.Warning {
		return LevelWarning
	}

	// Going down (decreasing pressure) - apply hysteresis
	switch currentLevel {
	case LevelEmergency:
		if usage < thresholds.Emergency-hysteresis {
			return LevelCritical
		}
		return LevelEmergency
	case LevelCritical:
		if usage < thresholds.Critical-hysteresis {
			return LevelWarning
		}
		return LevelCritical
	case LevelWarning:
		if usage < thresholds.Warning-hysteresis {
			return LevelNormal
		}
		return LevelWarning
	default:
		return LevelNormal
	}
}

// setLevel updates the current level and fires callback.
func (c *Controller) setLevel(newLevel Level) {
	oldLevel := c.lastLevel
	c.lastLevel = newLevel
	c.level.Store(int32(newLevel))
	c.stats.LevelChanges++

	// Update level-specific counters
	switch newLevel {
	case LevelWarning:
		c.stats.WarningCount++
	case LevelCritical:
		c.stats.CriticalCount++
	case LevelEmergency:
		c.stats.EmergencyCount++
	}

	// Fire callback
	if c.onLevelChange != nil {
		c.onLevelChange(oldLevel, newLevel)
	}
}

// CurrentLevel returns the current backpressure level.
func (c *Controller) CurrentLevel() Level {
	return Level(c.level.Load())
}

// ShouldDrop returns true if samples should be dropped.
func (c *Controller) ShouldDrop() bool {
	return c.CurrentLevel() == LevelEmergency
}

// ShouldThrottle returns true if requests should be throttled.
func (c *Controller) ShouldThrottle() bool {
	level := c.CurrentLevel()
	return level >= LevelCritical
}

// ShouldPauseCompaction returns true if compaction should be paused.
func (c *Controller) ShouldPauseCompaction() bool {
	level := c.CurrentLevel()
	return level >= LevelWarning
}

// ThrottleFactor returns the throttle factor (0.0 to 1.0).
// 1.0 = no throttling, 0.0 = full throttle (reject all).
func (c *Controller) ThrottleFactor() float64 {
	level := c.CurrentLevel()
	switch level {
	case LevelNormal:
		return 1.0
	case LevelWarning:
		return 0.9
	case LevelCritical:
		return 0.5
	case LevelEmergency:
		return 0.1
	default:
		return 1.0
	}
}

// ThrottleDelay returns the recommended delay between operations.
func (c *Controller) ThrottleDelay() time.Duration {
	factor := c.ThrottleFactor()
	if factor >= 1.0 {
		return 0
	}

	// Max delay of 100ms at emergency level
	maxDelay := 100 * time.Millisecond
	delay := time.Duration(float64(maxDelay) * (1.0 - factor))

	c.mu.Lock()
	c.stats.ThrottleSeconds += delay.Seconds()
	c.mu.Unlock()

	return delay
}

// RecordDrop records that a sample was dropped.
func (c *Controller) RecordDrop() {
	c.mu.Lock()
	c.stats.SamplesDropped++
	c.mu.Unlock()
}

// Stats returns current statistics.
func (c *Controller) Stats() ControllerStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ControllerStats{
		CurrentLevel:    c.CurrentLevel(),
		LevelChanges:    c.stats.LevelChanges,
		WarningCount:    c.stats.WarningCount,
		CriticalCount:   c.stats.CriticalCount,
		EmergencyCount:  c.stats.EmergencyCount,
		SamplesDropped:  c.stats.SamplesDropped,
		ThrottleSeconds: c.stats.ThrottleSeconds,
		BufferUsage:     c.buffer.UsageRatio(),
	}
}

// ControllerStats holds controller statistics.
type ControllerStats struct {
	CurrentLevel    Level
	LevelChanges    int64
	WarningCount    int64
	CriticalCount   int64
	EmergencyCount  int64
	SamplesDropped  int64
	ThrottleSeconds float64
	BufferUsage     float64
}

// IsEnabled returns whether backpressure is enabled.
func (c *Controller) IsEnabled() bool {
	return c.config.Backpressure.Enabled
}

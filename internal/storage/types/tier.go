package types

import (
	"fmt"
	"time"
)

// Tier represents a storage tier with specific resolution and retention.
type Tier int

const (
	// TierRaw stores raw samples at polling resolution (e.g., 25s).
	// Retention: 48 hours
	TierRaw Tier = iota

	// Tier5Min stores 5-minute aggregates.
	// Retention: 30 days
	Tier5Min

	// TierHourly stores hourly aggregates.
	// Retention: 90 days
	TierHourly

	// TierDaily stores daily aggregates.
	// Retention: 2 years
	TierDaily

	// TierWeekly stores weekly aggregates.
	// Retention: 10 years
	TierWeekly
)

// String returns the string representation of the tier.
func (t Tier) String() string {
	switch t {
	case TierRaw:
		return "raw"
	case Tier5Min:
		return "5min"
	case TierHourly:
		return "hourly"
	case TierDaily:
		return "daily"
	case TierWeekly:
		return "weekly"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// Duration returns the bucket duration for this tier.
func (t Tier) Duration() time.Duration {
	switch t {
	case TierRaw:
		return 25 * time.Second // Polling interval
	case Tier5Min:
		return 5 * time.Minute
	case TierHourly:
		return time.Hour
	case TierDaily:
		return 24 * time.Hour
	case TierWeekly:
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}

// DefaultRetention returns the default retention duration for this tier.
func (t Tier) DefaultRetention() time.Duration {
	switch t {
	case TierRaw:
		return 48 * time.Hour
	case Tier5Min:
		return 30 * 24 * time.Hour // 30 days
	case TierHourly:
		return 90 * 24 * time.Hour // 90 days
	case TierDaily:
		return 2 * 365 * 24 * time.Hour // 2 years
	case TierWeekly:
		return 10 * 365 * 24 * time.Hour // 10 years
	default:
		return 0
	}
}

// BucketsPerDay returns the number of buckets in a day for this tier.
func (t Tier) BucketsPerDay() int {
	switch t {
	case TierRaw:
		return 3456 // 86400 / 25
	case Tier5Min:
		return 288 // 24 * 12
	case TierHourly:
		return 24
	case TierDaily:
		return 1
	case TierWeekly:
		return 0 // Less than 1 per day
	default:
		return 0
	}
}

// Next returns the next tier for compaction.
// Returns the same tier if it's the highest tier.
func (t Tier) Next() Tier {
	switch t {
	case TierRaw:
		return Tier5Min
	case Tier5Min:
		return TierHourly
	case TierHourly:
		return TierDaily
	case TierDaily:
		return TierWeekly
	case TierWeekly:
		return TierWeekly // No higher tier
	default:
		return t
	}
}

// Previous returns the previous tier.
// Returns the same tier if it's the lowest tier.
func (t Tier) Previous() Tier {
	switch t {
	case TierRaw:
		return TierRaw // No lower tier
	case Tier5Min:
		return TierRaw
	case TierHourly:
		return Tier5Min
	case TierDaily:
		return TierHourly
	case TierWeekly:
		return TierDaily
	default:
		return t
	}
}

// IsHighest returns true if this is the highest tier.
func (t Tier) IsHighest() bool {
	return t == TierWeekly
}

// IsLowest returns true if this is the lowest tier.
func (t Tier) IsLowest() bool {
	return t == TierRaw
}

// TruncateToBucket truncates a timestamp to the start of its bucket.
func (t Tier) TruncateToBucket(ts time.Time) time.Time {
	switch t {
	case TierRaw:
		// Truncate to 25-second intervals
		seconds := ts.Unix()
		bucket := (seconds / 25) * 25
		return time.Unix(bucket, 0).UTC()
	case Tier5Min:
		return ts.Truncate(5 * time.Minute).UTC()
	case TierHourly:
		return ts.Truncate(time.Hour).UTC()
	case TierDaily:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
	case TierWeekly:
		// Truncate to Monday 00:00:00 UTC
		weekday := int(ts.Weekday())
		if weekday == 0 {
			weekday = 7 // Sunday = 7
		}
		monday := ts.AddDate(0, 0, -(weekday - 1))
		return time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, time.UTC)
	default:
		return ts
	}
}

// ParseTier parses a string into a Tier.
func ParseTier(s string) (Tier, error) {
	switch s {
	case "raw":
		return TierRaw, nil
	case "5min":
		return Tier5Min, nil
	case "hourly":
		return TierHourly, nil
	case "daily":
		return TierDaily, nil
	case "weekly":
		return TierWeekly, nil
	default:
		return TierRaw, fmt.Errorf("unknown tier: %s", s)
	}
}

// AllTiers returns all available tiers in order.
func AllTiers() []Tier {
	return []Tier{TierRaw, Tier5Min, TierHourly, TierDaily, TierWeekly}
}

// SelectTierForRange returns the appropriate tier for a given time range.
// This implements automatic tier selection based on query duration.
func SelectTierForRange(start, end time.Time) Tier {
	duration := end.Sub(start)

	switch {
	case duration <= 48*time.Hour:
		return TierRaw
	case duration <= 30*24*time.Hour:
		return Tier5Min
	case duration <= 90*24*time.Hour:
		return TierHourly
	case duration <= 2*365*24*time.Hour:
		return TierDaily
	default:
		return TierWeekly
	}
}

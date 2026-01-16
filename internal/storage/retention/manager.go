package retention

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// Manager handles automatic cleanup of expired data.
type Manager struct {
	mu     sync.RWMutex
	config *config.Config
	stats  Stats
}

// Stats holds retention statistics.
type Stats struct {
	LastRunTime   time.Time
	FilesDeleted  int64
	BytesFreed    int64
	FilesSkipped  int64
	Errors        int64
}

// CleanupResult holds the result of a cleanup operation.
type CleanupResult struct {
	Tier         types.Tier
	FilesDeleted int
	BytesFreed   int64
	FilesSkipped int
	Errors       []error
}

// New creates a new retention manager.
func New(cfg *config.Config) *Manager {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	return &Manager{
		config: cfg,
	}
}

// RunCleanup performs cleanup on all tiers.
func (m *Manager) RunCleanup() []CleanupResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats.LastRunTime = time.Now()

	var results []CleanupResult

	// Clean each tier
	for _, tier := range types.AllTiers() {
		result := m.cleanupTier(tier, false)
		results = append(results, result)

		m.stats.FilesDeleted += int64(result.FilesDeleted)
		m.stats.BytesFreed += result.BytesFreed
		m.stats.FilesSkipped += int64(result.FilesSkipped)
		m.stats.Errors += int64(len(result.Errors))
	}

	return results
}

// DryRun simulates cleanup without deleting files.
func (m *Manager) DryRun() []CleanupResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	var results []CleanupResult

	for _, tier := range types.AllTiers() {
		result := m.cleanupTier(tier, true)
		results = append(results, result)
	}

	return results
}

// CleanupTier cleans a specific tier.
func (m *Manager) CleanupTier(tier types.Tier) CleanupResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := m.cleanupTier(tier, false)

	m.stats.FilesDeleted += int64(result.FilesDeleted)
	m.stats.BytesFreed += result.BytesFreed
	m.stats.FilesSkipped += int64(result.FilesSkipped)
	m.stats.Errors += int64(len(result.Errors))

	return result
}

// cleanupTier performs cleanup for a single tier.
func (m *Manager) cleanupTier(tier types.Tier, dryRun bool) CleanupResult {
	result := CleanupResult{Tier: tier}

	tierDir := m.config.TierDir(tier.String())
	retention := m.getRetention(tier)
	cutoff := time.Now().Add(-retention)

	// List files
	files, err := m.listFiles(tierDir)
	if err != nil {
		if !os.IsNotExist(err) {
			result.Errors = append(result.Errors, fmt.Errorf("list files: %w", err))
		}
		return result
	}

	for _, file := range files {
		// Parse file time from name
		fileTime, err := m.parseFileTime(file.name, tier)
		if err != nil {
			result.FilesSkipped++
			continue
		}

		// Check if file is expired
		if fileTime.After(cutoff) {
			result.FilesSkipped++
			continue
		}

		// Delete file
		if !dryRun {
			if err := os.Remove(file.path); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("delete %s: %w", file.path, err))
				continue
			}
		}

		result.FilesDeleted++
		result.BytesFreed += file.size
	}

	return result
}

// fileInfo holds information about a file.
type fileInfo struct {
	name string
	path string
	size int64
}

// listFiles lists all Parquet files in a directory.
func (m *Manager) listFiles(dir string) ([]fileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []fileInfo

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != ".parquet" {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, fileInfo{
			name: name,
			path: filepath.Join(dir, name),
			size: info.Size(),
		})
	}

	// Sort by name (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].name < files[j].name
	})

	return files, nil
}

// getRetention returns the retention duration for a tier.
func (m *Manager) getRetention(tier types.Tier) time.Duration {
	switch tier {
	case types.TierRaw:
		return m.config.Retention.Raw
	case types.Tier5Min:
		return m.config.Retention.FiveMin
	case types.TierHourly:
		return m.config.Retention.Hourly
	case types.TierDaily:
		return m.config.Retention.Daily
	case types.TierWeekly:
		return m.config.Retention.Weekly
	default:
		return tier.DefaultRetention()
	}
}

// parseFileTime extracts the timestamp from a filename.
func (m *Manager) parseFileTime(name string, tier types.Tier) (time.Time, error) {
	// Remove extension
	base := name[:len(name)-len(filepath.Ext(name))]

	// Parse based on tier-specific format
	var layout string
	switch tier {
	case types.TierRaw, types.Tier5Min:
		// Format: 2006-01-02_15-04
		layout = "2006-01-02_15-04"
	case types.TierHourly:
		// Format: 2006-01-02
		layout = "2006-01-02"
	case types.TierDaily:
		// Format: 2006-01
		layout = "2006-01"
	case types.TierWeekly:
		// Format: 2006
		layout = "2006"
	default:
		return time.Time{}, fmt.Errorf("unknown tier: %s", tier)
	}

	return time.Parse(layout, base)
}

// Stats returns current statistics.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ManagerStats{
		LastRunTime:  m.stats.LastRunTime,
		FilesDeleted: m.stats.FilesDeleted,
		BytesFreed:   m.stats.BytesFreed,
		FilesSkipped: m.stats.FilesSkipped,
		Errors:       m.stats.Errors,
	}
}

// ManagerStats holds manager statistics.
type ManagerStats struct {
	LastRunTime  time.Time
	FilesDeleted int64
	BytesFreed   int64
	FilesSkipped int64
	Errors       int64
}

// GetDiskUsage returns disk usage for each tier.
func (m *Manager) GetDiskUsage() map[types.Tier]DiskUsage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	usage := make(map[types.Tier]DiskUsage)

	for _, tier := range types.AllTiers() {
		tierDir := m.config.TierDir(tier.String())
		files, err := m.listFiles(tierDir)
		if err != nil {
			continue
		}

		var totalSize int64
		for _, f := range files {
			totalSize += f.size
		}

		usage[tier] = DiskUsage{
			FileCount: len(files),
			TotalSize: totalSize,
		}
	}

	return usage
}

// DiskUsage holds disk usage information.
type DiskUsage struct {
	FileCount int
	TotalSize int64
}

// FormatDiskUsage returns a formatted string of disk usage.
func (m *Manager) FormatDiskUsage() string {
	usage := m.GetDiskUsage()

	var result string
	var totalSize int64
	var totalFiles int

	for _, tier := range types.AllTiers() {
		u := usage[tier]
		totalSize += u.TotalSize
		totalFiles += u.FileCount

		result += fmt.Sprintf("  %s: %d files, %s\n",
			tier.String(),
			u.FileCount,
			formatBytes(u.TotalSize),
		)
	}

	result = fmt.Sprintf("Disk Usage:\n%s  Total: %d files, %s\n",
		result, totalFiles, formatBytes(totalSize))

	return result
}

// formatBytes formats bytes as human-readable string.
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

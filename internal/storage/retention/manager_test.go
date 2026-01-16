package retention

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestManager_New(t *testing.T) {
	cfg := config.DefaultConfig()
	m := New(cfg)

	if m == nil {
		t.Fatal("manager is nil")
	}
}

func TestManager_ParseFileTime(t *testing.T) {
	cfg := config.DefaultConfig()
	m := New(cfg)

	tests := []struct {
		name     string
		filename string
		tier     types.Tier
		expected time.Time
		hasError bool
	}{
		{
			name:     "5min format",
			filename: "2026-01-15_10-30.parquet",
			tier:     types.Tier5Min,
			expected: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "hourly format",
			filename: "2026-01-15.parquet",
			tier:     types.TierHourly,
			expected: time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "daily format",
			filename: "2026-01.parquet",
			tier:     types.TierDaily,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekly format",
			filename: "2026.parquet",
			tier:     types.TierWeekly,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "invalid format",
			filename: "invalid.parquet",
			tier:     types.Tier5Min,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.parseFileTime(tt.filename, tt.tier)

			if tt.hasError {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Equal(tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestManager_CleanupTier(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Retention.FiveMin = 24 * time.Hour // 1 day retention

	m := New(cfg)

	// Create 5min directory
	tierDir := filepath.Join(tmpDir, "5min")
	if err := os.MkdirAll(tierDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Create test files
	now := time.Now()
	files := []struct {
		name   string
		age    time.Duration
		delete bool
	}{
		{"2026-01-15_10-30.parquet", 2 * 24 * time.Hour, true},  // 2 days old - delete
		{"2026-01-15_11-30.parquet", 2 * 24 * time.Hour, true},  // 2 days old - delete
		{now.Format("2006-01-02_15-04") + ".parquet", 0, false}, // Current - keep
	}

	for _, f := range files {
		path := filepath.Join(tierDir, f.name)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}

	// Run cleanup
	result := m.CleanupTier(types.Tier5Min)

	if result.FilesDeleted != 2 {
		t.Errorf("expected 2 files deleted, got %d", result.FilesDeleted)
	}

	if result.FilesSkipped != 1 {
		t.Errorf("expected 1 file skipped, got %d", result.FilesSkipped)
	}

	// Verify files
	remaining, _ := os.ReadDir(tierDir)
	if len(remaining) != 1 {
		t.Errorf("expected 1 file remaining, got %d", len(remaining))
	}
}

func TestManager_DryRun(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Retention.FiveMin = 24 * time.Hour

	m := New(cfg)

	// Create 5min directory and old file
	tierDir := filepath.Join(tmpDir, "5min")
	os.MkdirAll(tierDir, 0755)

	oldFile := filepath.Join(tierDir, "2020-01-01_10-30.parquet")
	os.WriteFile(oldFile, []byte("test"), 0644)

	// Dry run
	results := m.DryRun()

	// File should be marked for deletion
	var fiveMinResult *CleanupResult
	for _, r := range results {
		if r.Tier == types.Tier5Min {
			fiveMinResult = &r
			break
		}
	}

	if fiveMinResult == nil {
		t.Fatal("5min result not found")
	}

	if fiveMinResult.FilesDeleted != 1 {
		t.Errorf("expected 1 file would be deleted, got %d", fiveMinResult.FilesDeleted)
	}

	// File should still exist (dry run)
	if _, err := os.Stat(oldFile); os.IsNotExist(err) {
		t.Error("file should still exist after dry run")
	}
}

func TestManager_GetDiskUsage(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	m := New(cfg)

	// Create some files
	tierDir := filepath.Join(tmpDir, "5min")
	os.MkdirAll(tierDir, 0755)

	for i := 0; i < 3; i++ {
		path := filepath.Join(tierDir, time.Now().Format("2006-01-02_15-04")+"-"+string(rune('a'+i))+".parquet")
		os.WriteFile(path, []byte("test data content"), 0644)
	}

	usage := m.GetDiskUsage()

	fiveMinUsage := usage[types.Tier5Min]
	if fiveMinUsage.FileCount != 3 {
		t.Errorf("expected 3 files, got %d", fiveMinUsage.FileCount)
	}

	if fiveMinUsage.TotalSize <= 0 {
		t.Error("expected positive total size")
	}
}

func TestManager_FormatDiskUsage(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	m := New(cfg)

	output := m.FormatDiskUsage()

	if len(output) == 0 {
		t.Error("expected non-empty output")
	}

	// Should contain tier names
	for _, tier := range types.AllTiers() {
		if !containsString(output, tier.String()) {
			t.Errorf("output should contain tier %s", tier)
		}
	}
}

func TestManager_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Retention.FiveMin = 24 * time.Hour

	m := New(cfg)

	// Create old file
	tierDir := filepath.Join(tmpDir, "5min")
	os.MkdirAll(tierDir, 0755)
	os.WriteFile(filepath.Join(tierDir, "2020-01-01_10-30.parquet"), []byte("test"), 0644)

	// Initial stats
	stats := m.Stats()
	if stats.FilesDeleted != 0 {
		t.Errorf("expected 0 files deleted initially, got %d", stats.FilesDeleted)
	}

	// Run cleanup
	m.RunCleanup()

	// Updated stats
	stats = m.Stats()
	if stats.FilesDeleted != 1 {
		t.Errorf("expected 1 file deleted, got %d", stats.FilesDeleted)
	}

	if stats.LastRunTime.IsZero() {
		t.Error("last run time should be set")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.bytes)
		if result != tt.expected {
			t.Errorf("formatBytes(%d): expected %s, got %s", tt.bytes, tt.expected, result)
		}
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsString(s[1:], substr) || s[:len(substr)] == substr)
}

// Package loader - Configuration Types
//
// LOCATION: internal/loader/types.go
//
// Defines the YAML configuration structure for stalkerd.
//
// ARCHITECTURE:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                         config.yaml                                 │
//   ├─────────────────────────────────────────────────────────────────────┤
//   │                                                                     │
//   │  server:      Network, TLS, shutdown behavior                       │
//   │  auth:        Tokens, rate limiting                                 │
//   │  session:     Client connection management                          │
//   │  poller:      Scheduler worker pool                                 │
//   │                                                                     │
//   │  ┌─────────────────────┐    ┌─────────────────────────────────┐    │
//   │  │     metastore:      │    │        samplestore:             │    │
//   │  │     (DuckDB)        │    │     (Parquet + WAL)             │    │
//   │  ├─────────────────────┤    ├─────────────────────────────────┤    │
//   │  │ • Namespaces        │    │ • Raw samples (high volume)     │    │
//   │  │ • Targets           │    │ • Aggregated data (5m/h/d/w)    │    │
//   │  │ • Pollers (config)  │    │ • DDSketch percentiles          │    │
//   │  │ • Secrets           │    │ • Long-term retention           │    │
//   │  │ • Tree structure    │    │                                 │    │
//   │  │ • Poller state      │    │ Write path:                     │    │
//   │  │ • Poller stats      │    │   Ingest → WAL → Buffer →       │    │
//   │  │                     │    │   Parquet → Compaction          │    │
//   │  │ Access: OLTP        │    │                                 │    │
//   │  │ (many small txns)   │    │ Access: OLAP                    │    │
//   │  └─────────────────────┘    │ (bulk writes, range queries)    │    │
//   │                             └─────────────────────────────────┘    │
//   │                                                                     │
//   │  snmp:        Protocol defaults                                     │
//   │  sync:        YAML↔DB reconciliation                                │
//   │  tree:        Virtual filesystem                                    │
//   │  namespaces:  Multi-tenant configuration                            │
//   │                                                                     │
//   └─────────────────────────────────────────────────────────────────────┘

package loader

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Root Configuration
// =============================================================================

// Config is the root configuration structure for stalkerd.
type Config struct {
	// -------------------------------------------------------------------------
	// Runtime Settings (server process configuration)
	// -------------------------------------------------------------------------

	// Listen is the gRPC server listen address.
	// Format: "host:port" or ":port"
	// Default: "0.0.0.0:9161"
	Listen string `yaml:"listen"`

	// TLS configures transport layer security.
	TLS TLSConfig `yaml:"tls"`

	// Auth configures authentication tokens and rate limiting.
	Auth AuthConfig `yaml:"auth"`

	// Session configures client session management.
	Session SessionConfig `yaml:"session"`

	// Poller configures the scheduler and worker pool.
	Poller PollerConfig `yaml:"poller"`

	// Shutdown configures graceful shutdown behavior.
	Shutdown ShutdownConfig `yaml:"shutdown"`

	// -------------------------------------------------------------------------
	// Storage Systems
	// -------------------------------------------------------------------------

	// Metastore is the configuration/state database (DuckDB).
	//
	// Stores: namespaces, targets, pollers, secrets, tree, poller state/stats.
	// Access pattern: OLTP (many small transactions, low latency).
	// Technology: DuckDB (embedded SQL database).
	Metastore MetastoreConfig `yaml:"metastore"`

	// Samplestore is the time-series sample database (Parquet + WAL).
	//
	// Stores: raw samples, aggregated data (5min/hourly/daily/weekly).
	// Access pattern: OLAP (bulk writes, range queries, analytics).
	// Technology: Write-Ahead Log + Parquet files + DuckDB for queries.
	Samplestore SamplestoreConfig `yaml:"samplestore"`

	// -------------------------------------------------------------------------
	// Protocol Defaults
	// -------------------------------------------------------------------------

	// SNMP configures server-wide SNMP defaults.
	// Can be overridden at: namespace → target → poller level.
	SNMP SNMPDefaults `yaml:"snmp"`

	// -------------------------------------------------------------------------
	// Declarative Configuration
	// -------------------------------------------------------------------------

	// Sync configures YAML-to-database synchronization.
	Sync *SyncConfig `yaml:"sync"`

	// Tree configures the virtual filesystem organization.
	Tree *TreeConfig `yaml:"tree"`

	// Namespaces defines multi-tenant namespace configuration.
	Namespaces map[string]*NamespaceConfig `yaml:"namespaces"`

	// Secrets defines global secrets (referenced as "secret:name").
	Secrets map[string]*SecretConfig `yaml:"secrets"`

	// Include lists additional config files to load.
	// Supports glob patterns. Relative to this file's directory.
	Include []string `yaml:"include"`

	// -------------------------------------------------------------------------
	// Deprecated (kept for backwards compatibility)
	// -------------------------------------------------------------------------

	// Storage is deprecated. Use Metastore instead.
	// If set, values are copied to Metastore.
	Storage *StorageConfigLegacy `yaml:"storage,omitempty"`
}

// =============================================================================
// Server Configuration
// =============================================================================

// TLSConfig configures transport layer security.
type TLSConfig struct {
	// CertFile is the path to the PEM-encoded TLS certificate.
	// Leave empty to disable TLS (not recommended for production).
	CertFile string `yaml:"cert_file"`

	// KeyFile is the path to the PEM-encoded TLS private key.
	KeyFile string `yaml:"key_file"`
}

// AuthConfig configures authentication.
type AuthConfig struct {
	// RateLimitPerMinute is the max failed auth attempts per IP per minute.
	// After this limit, the IP is temporarily blocked.
	// Default: 5
	RateLimitPerMinute int `yaml:"rate_limit_per_minute"`

	// Tokens is the list of authentication tokens.
	// At least one token is required.
	Tokens []TokenConfig `yaml:"tokens"`
}

// TokenConfig defines an authentication token.
type TokenConfig struct {
	// ID is a unique identifier for logging/auditing (not secret).
	ID string `yaml:"id"`

	// Token is the secret token value.
	// Use environment variables: "${STALKER_TOKEN}"
	Token string `yaml:"token"`

	// Namespaces restricts this token to specific namespaces.
	// Empty = access to all namespaces (admin).
	Namespaces []string `yaml:"namespaces"`
}

// SessionConfig configures client session management.
type SessionConfig struct {
	// AuthTimeoutSec is the max time for authentication after connect.
	// Range: 5-300, Default: 30
	AuthTimeoutSec int `yaml:"auth_timeout_sec"`

	// ReconnectWindowSec is reserved for future session resumption.
	// Default: 600
	ReconnectWindowSec int `yaml:"reconnect_window_sec"`

	// CleanupIntervalSec is how often closed sessions are cleaned up.
	// Range: 10-300, Default: 60
	CleanupIntervalSec int `yaml:"cleanup_interval_sec"`

	// SendBufferSize is the per-session message queue capacity.
	// Range: 100-10000, Default: 1000
	SendBufferSize int `yaml:"send_buffer_size"`

	// SendTimeoutMs is the timeout for queuing a message.
	// Range: 10-10000, Default: 100
	SendTimeoutMs int `yaml:"send_timeout_ms"`
}

// PollerConfig configures the scheduler and worker pool.
type PollerConfig struct {
	// Workers is the number of concurrent poll workers.
	// Range: 1-1000, Default: 100
	Workers int `yaml:"workers"`

	// QueueSize is the job queue capacity.
	// Range: 100-100000, Default: 10000
	QueueSize int `yaml:"queue_size"`
}

// ShutdownConfig configures graceful shutdown.
type ShutdownConfig struct {
	// DrainTimeoutSec is how long to wait for in-flight polls.
	// Range: 5-300, Default: 30
	DrainTimeoutSec int `yaml:"drain_timeout_sec"`
}

// =============================================================================
// Metastore Configuration (DuckDB - Config/State/Stats)
// =============================================================================

// MetastoreConfig configures the metadata database.
//
// The metastore holds all configuration and runtime state:
//   - Namespace/Target/Poller definitions
//   - Encrypted secrets
//   - Tree structure (virtual filesystem)
//   - Poller operational state (up/down, health, last error)
//   - Poller statistics (poll counts, timing)
//
// Technology: DuckDB (embedded OLTP database)
// Access pattern: Many small reads/writes, low latency
type MetastoreConfig struct {
	// Path is the database file path.
	// Special value ":memory:" for in-memory (testing only).
	// Default: "stalker.db"
	Path string `yaml:"path"`

	// SecretKeyPath is the 32-byte AES-256 key for secret encryption.
	// Generate with: openssl rand -out secret.key 32
	// Leave empty to disable encryption (not recommended).
	SecretKeyPath string `yaml:"secret_key_path"`

	// -------------------------------------------------------------------------
	// State/Stats Persistence
	// -------------------------------------------------------------------------
	// Poller state and statistics are cached in memory and periodically
	// flushed to the database to reduce I/O.

	// StateFlushInterval is how often poller state is persisted.
	// State: oper_state, health_state, last_error, timestamps.
	// Default: 5s
	StateFlushInterval Duration `yaml:"state_flush_interval"`

	// StatsFlushInterval is how often poller statistics are persisted.
	// Stats: poll_count, success_count, timing percentiles.
	// Default: 10s
	StatsFlushInterval Duration `yaml:"stats_flush_interval"`

	// -------------------------------------------------------------------------
	// Connection Pool
	// -------------------------------------------------------------------------

	// MaxOpenConns is the max open database connections.
	// Default: 25
	MaxOpenConns int `yaml:"max_open_conns"`

	// MaxIdleConns is the max idle connections in the pool.
	// Default: 5
	MaxIdleConns int `yaml:"max_idle_conns"`

	// ConnMaxLifetime is the max lifetime of a connection.
	// Default: 5m
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime"`

	// QueryTimeout is the default query timeout.
	// Default: 30s
	QueryTimeout Duration `yaml:"query_timeout"`
}

// =============================================================================
// Samplestore Configuration (Parquet + WAL - Time-Series Data)
// =============================================================================

// SamplestoreConfig configures the time-series sample database.
//
// The samplestore holds all collected metric samples:
//   - Raw samples (original polling resolution)
//   - Aggregated data (5min, hourly, daily, weekly)
//   - DDSketch percentiles (P50, P90, P95, P99)
//
// Architecture:
//   Ingest → WAL (crash safety) → Buffer (in-memory) → Parquet (columnar files)
//           → Compaction (aggregation) → Retention (cleanup)
//
// Technology: Write-Ahead Log + Parquet + DuckDB for queries
// Access pattern: Bulk writes, range queries, analytics (OLAP)
type SamplestoreConfig struct {
	// Enabled enables the samplestore.
	// If false, samples are only kept in the ring buffer (metastore).
	// Default: true
	Enabled bool `yaml:"enabled"`

	// DataDir is the root directory for all storage files.
	// Subdirectories: wal/, raw/, 5min/, hourly/, daily/, weekly/
	// Default: "/var/lib/stalker/samples"
	DataDir string `yaml:"data_dir"`

	// -------------------------------------------------------------------------
	// Capacity Planning
	// -------------------------------------------------------------------------

	Scale ScaleConfig `yaml:"scale"`

	// -------------------------------------------------------------------------
	// Features
	// -------------------------------------------------------------------------

	Features FeaturesConfig `yaml:"features"`

	// -------------------------------------------------------------------------
	// Write-Ahead Log (WAL)
	// -------------------------------------------------------------------------

	WAL WALConfig `yaml:"wal"`

	// -------------------------------------------------------------------------
	// Parquet Storage
	// -------------------------------------------------------------------------

	Flush FlushConfig `yaml:"flush"`

	// -------------------------------------------------------------------------
	// Aggregation & Compaction
	// -------------------------------------------------------------------------

	Compaction CompactionConfig `yaml:"compaction"`

	// -------------------------------------------------------------------------
	// Retention
	// -------------------------------------------------------------------------

	Retention RetentionConfig `yaml:"retention"`

	// -------------------------------------------------------------------------
	// Backpressure
	// -------------------------------------------------------------------------

	Backpressure BackpressureConfig `yaml:"backpressure"`

	// -------------------------------------------------------------------------
	// Query Engine
	// -------------------------------------------------------------------------

	Query QueryConfig `yaml:"query"`
}

// ScaleConfig defines expected load for capacity planning.
type ScaleConfig struct {
	// PollerCount is the expected number of pollers (metric series).
	// Used for buffer sizing and capacity planning.
	// Default: 100000
	PollerCount int `yaml:"poller_count"`

	// PollIntervalSec is the expected polling interval in seconds.
	// Used to calculate expected samples/second.
	// Default: 60
	PollIntervalSec int `yaml:"poll_interval_sec"`
}

// FeaturesConfig configures optional features.
type FeaturesConfig struct {
	// Buffer configures the in-memory sample buffer.
	Buffer BufferConfig `yaml:"buffer"`

	// Percentiles configures DDSketch percentile calculation.
	Percentiles PercentilesConfig `yaml:"percentiles"`

	// Compression configures Parquet compression.
	Compression CompressionConfig `yaml:"compression"`
}

// BufferConfig configures the in-memory sample buffer.
type BufferConfig struct {
	// Enabled enables the in-memory buffer.
	// Default: true
	Enabled bool `yaml:"enabled"`

	// Duration is the max age of samples in the buffer.
	// Samples older than this are evicted.
	// Default: 5m
	Duration Duration `yaml:"duration"`

	// MemoryLimit is an alternative limit based on memory usage.
	// If set, Duration is ignored.
	// Default: "" (use Duration)
	MemoryLimit string `yaml:"memory_limit"`
}

// PercentilesConfig configures DDSketch percentile calculation.
type PercentilesConfig struct {
	// Enabled enables percentile calculation.
	// Default: true
	Enabled bool `yaml:"enabled"`

	// Accuracy is the relative accuracy (0.01 = 1% error).
	// Lower = more accurate, more memory.
	// Range: 0.001-0.1, Default: 0.01
	Accuracy float64 `yaml:"accuracy"`
}

// CompressionConfig configures Parquet compression.
type CompressionConfig struct {
	// Algorithm is the compression algorithm.
	//   snappy - Fast compression/decompression, moderate ratio
	//   zstd   - Best ratio, good speed (recommended)
	//   lz4    - Fastest, lowest ratio
	//   none   - No compression
	// Default: zstd
	Algorithm string `yaml:"algorithm"`

	// Level is the compression level (for zstd: 1-22).
	// Default: 3
	Level int `yaml:"level"`
}

// WALConfig configures the Write-Ahead Log.
type WALConfig struct {
	// Dir is the WAL directory. Defaults to {DataDir}/wal.
	Dir string `yaml:"dir"`

	// SyncMode determines durability vs. performance.
	//   async  - Buffer writes, sync periodically (fastest, risk of data loss)
	//   sync   - Sync after each batch (balanced)
	//   fsync  - fsync after each write (safest, slowest)
	// Default: async
	SyncMode string `yaml:"sync_mode"`

	// SyncInterval is the sync interval for async mode.
	// Default: 1s
	SyncInterval Duration `yaml:"sync_interval"`

	// MaxSegmentSize is the max segment size before rotation.
	// Default: 100MB
	MaxSegmentSize ByteSize `yaml:"max_segment_size"`
}

// FlushConfig configures buffer-to-Parquet flushing.
type FlushConfig struct {
	// Interval is how often to flush buffer to Parquet.
	// Default: 10m
	Interval Duration `yaml:"interval"`

	// MaxBufferSize triggers a flush when reached.
	// Default: 100MB
	MaxBufferSize ByteSize `yaml:"max_buffer_size"`
}

// CompactionConfig configures the compaction engine.
type CompactionConfig struct {
	// Workers is the number of parallel compaction workers.
	// Default: 4
	Workers int `yaml:"workers"`

	// Schedule configures when compaction jobs run (cron format).
	Schedule CompactionScheduleConfig `yaml:"schedule"`
}

// CompactionScheduleConfig configures compaction job schedules.
type CompactionScheduleConfig struct {
	// RawTo5Min aggregates raw → 5-minute.
	// Default: "0 * * * *" (every hour)
	RawTo5Min string `yaml:"raw_to_5min"`

	// FiveMinToHourly aggregates 5-minute → hourly.
	// Default: "0 2 * * *" (daily at 2am)
	FiveMinToHourly string `yaml:"5min_to_hourly"`

	// HourlyToDaily aggregates hourly → daily.
	// Default: "0 3 * * 0" (weekly on Sunday at 3am)
	HourlyToDaily string `yaml:"hourly_to_daily"`

	// DailyToWeekly aggregates daily → weekly.
	// Default: "0 4 1 * *" (monthly on 1st at 4am)
	DailyToWeekly string `yaml:"daily_to_weekly"`
}

// RetentionConfig configures data retention per tier.
type RetentionConfig struct {
	// Raw is retention for raw samples.
	// Default: 48h
	Raw Duration `yaml:"raw"`

	// FiveMin is retention for 5-minute aggregates.
	// Default: 720h (30 days)
	FiveMin Duration `yaml:"5min"`

	// Hourly is retention for hourly aggregates.
	// Default: 2160h (90 days)
	Hourly Duration `yaml:"hourly"`

	// Daily is retention for daily aggregates.
	// Default: 17520h (2 years)
	Daily Duration `yaml:"daily"`

	// Weekly is retention for weekly aggregates.
	// Default: 87600h (10 years)
	Weekly Duration `yaml:"weekly"`
}

// BackpressureConfig configures load shedding.
type BackpressureConfig struct {
	// Enabled enables backpressure handling.
	// Default: true
	Enabled bool `yaml:"enabled"`

	// Thresholds for level changes.
	Thresholds BackpressureThresholds `yaml:"thresholds"`

	// Recovery settings.
	Recovery BackpressureRecovery `yaml:"recovery"`
}

// BackpressureThresholds defines level thresholds.
type BackpressureThresholds struct {
	// Warning triggers warning level (pause compaction).
	// Range: 0.0-1.0, Default: 0.50
	Warning float64 `yaml:"warning"`

	// Critical triggers critical level (throttle pollers).
	// Range: 0.0-1.0, Default: 0.80
	Critical float64 `yaml:"critical"`

	// Emergency triggers emergency level (drop samples).
	// Range: 0.0-1.0, Default: 0.95
	Emergency float64 `yaml:"emergency"`
}

// BackpressureRecovery configures recovery behavior.
type BackpressureRecovery struct {
	// Hysteresis prevents flapping between levels.
	// Level drops when usage falls below (threshold - hysteresis).
	// Range: 0.0-0.5, Default: 0.10
	Hysteresis float64 `yaml:"hysteresis"`

	// Cooldown is the minimum time between level changes.
	// Default: 30s
	Cooldown Duration `yaml:"cooldown"`
}

// QueryConfig configures the DuckDB query engine.
type QueryConfig struct {
	// MemoryLimit is the DuckDB memory limit.
	// Default: 2GB
	MemoryLimit string `yaml:"memory_limit"`

	// Timeout is the query timeout.
	// Default: 30s
	Timeout Duration `yaml:"timeout"`

	// MaxRows is the maximum rows returned per query.
	// Default: 1000000
	MaxRows int `yaml:"max_rows"`
}

// =============================================================================
// SNMP Defaults
// =============================================================================

// SNMPDefaults holds server-wide SNMP defaults.
type SNMPDefaults struct {
	// TimeoutMs is the SNMP request timeout in milliseconds.
	// Range: 500-30000, Default: 5000
	TimeoutMs uint32 `yaml:"timeout_ms"`

	// Retries is the number of retry attempts.
	// Range: 0-5, Default: 2
	Retries uint32 `yaml:"retries"`

	// IntervalMs is the default polling interval in milliseconds.
	// Range: 1000-3600000, Default: 60000
	IntervalMs uint32 `yaml:"interval_ms"`

	// BufferSize is the sample buffer size per poller.
	// Range: 60-86400, Default: 3600
	BufferSize uint32 `yaml:"buffer_size"`
}

// =============================================================================
// Namespace Configuration
// =============================================================================

// NamespaceConfig defines a namespace.
type NamespaceConfig struct {
	Description               string                   `yaml:"description"`
	Defaults                  *PollerDefaultsConfig    `yaml:"defaults"`
	SessionCleanupIntervalSec *int                     `yaml:"session_cleanup_interval_sec"`
	Targets                   map[string]*TargetConfig `yaml:"targets"`
}

// TargetConfig defines a target.
type TargetConfig struct {
	Description string                       `yaml:"description"`
	Host        string                       `yaml:"host"`
	Port        uint16                       `yaml:"port"`
	Labels      map[string]string            `yaml:"labels"`
	Defaults    *PollerDefaultsConfig        `yaml:"defaults"`
	Pollers     map[string]*PollerYAMLConfig `yaml:"pollers"`
}

// PollerYAMLConfig defines a poller.
type PollerYAMLConfig struct {
	Description    string                 `yaml:"description"`
	Protocol       string                 `yaml:"protocol"`
	ProtocolConfig map[string]interface{} `yaml:"protocol_config"`
	PollingConfig  *PollingConfigYAML     `yaml:"polling"`
	AdminState     string                 `yaml:"admin_state"`
}

// PollingConfigYAML holds polling settings.
type PollingConfigYAML struct {
	IntervalMs *uint32 `yaml:"interval_ms"`
	TimeoutMs  *uint32 `yaml:"timeout_ms"`
	Retries    *uint32 `yaml:"retries"`
	BufferSize *uint32 `yaml:"buffer_size"`
}

// PollerDefaultsConfig holds inherited poller settings.
type PollerDefaultsConfig struct {
	Community     string  `yaml:"community"`
	SecurityName  string  `yaml:"security_name"`
	SecurityLevel string  `yaml:"security_level"`
	AuthProtocol  string  `yaml:"auth_protocol"`
	AuthPassword  string  `yaml:"auth_password"`
	PrivProtocol  string  `yaml:"priv_protocol"`
	PrivPassword  string  `yaml:"priv_password"`
	IntervalMs    *uint32 `yaml:"interval_ms"`
	TimeoutMs     *uint32 `yaml:"timeout_ms"`
	Retries       *uint32 `yaml:"retries"`
	BufferSize    *uint32 `yaml:"buffer_size"`
}

// SecretConfig defines an encrypted secret.
type SecretConfig struct {
	Type  string `yaml:"type"`
	Value string `yaml:"value"`
}

// =============================================================================
// Sync and Tree Configuration - main types defined in sync_integration.go
// =============================================================================

// TreeViewConfig defines a label-based view.
type TreeViewConfig struct {
	Path        string `yaml:"path"`
	Match       string `yaml:"match"`
	When        string `yaml:"when"`
	Description string `yaml:"description"`
}

// TreeSmartConfig defines a query-based smart folder.
type TreeSmartConfig struct {
	Description string   `yaml:"description"`
	Targets     string   `yaml:"targets"`
	Pollers     string   `yaml:"pollers"`
	Cache       Duration `yaml:"cache"`
}

// =============================================================================
// Legacy Configuration (backwards compatibility)
// =============================================================================

// StorageConfigLegacy is the old storage config format.
// Deprecated: Use MetastoreConfig instead.
type StorageConfigLegacy struct {
	DBPath                string `yaml:"db_path"`
	SecretKeyPath         string `yaml:"secret_key_path"`
	SampleBatchSize       int    `yaml:"sample_batch_size"`
	SampleFlushTimeoutSec int    `yaml:"sample_flush_timeout_sec"`
	StateFlushIntervalSec int    `yaml:"state_flush_interval_sec"`
	StatsFlushIntervalSec int    `yaml:"stats_flush_interval_sec"`
	MaxOverflowSize       int    `yaml:"max_overflow_size"`
	MaxRetries            int    `yaml:"max_retries"`
}

// =============================================================================
// Defaults
// =============================================================================

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Listen: "0.0.0.0:9161",

		Auth: AuthConfig{
			RateLimitPerMinute: 5,
		},

		Session: SessionConfig{
			AuthTimeoutSec:     30,
			ReconnectWindowSec: 600,
			CleanupIntervalSec: 60,
			SendBufferSize:     1000,
			SendTimeoutMs:      100,
		},

		Poller: PollerConfig{
			Workers:   100,
			QueueSize: 10000,
		},

		Shutdown: ShutdownConfig{
			DrainTimeoutSec: 30,
		},

		Metastore: MetastoreConfig{
			Path:               "stalker.db",
			StateFlushInterval: Duration(5 * time.Second),
			StatsFlushInterval: Duration(10 * time.Second),
			MaxOpenConns:       25,
			MaxIdleConns:       5,
			ConnMaxLifetime:    Duration(5 * time.Minute),
			QueryTimeout:       Duration(30 * time.Second),
		},

		Samplestore: SamplestoreConfig{
			Enabled: true,
			DataDir: "/var/lib/stalker/samples",

			Scale: ScaleConfig{
				PollerCount:     100000,
				PollIntervalSec: 60,
			},

			Features: FeaturesConfig{
				Buffer: BufferConfig{
					Enabled:  true,
					Duration: Duration(5 * time.Minute),
				},
				Percentiles: PercentilesConfig{
					Enabled:  true,
					Accuracy: 0.01,
				},
				Compression: CompressionConfig{
					Algorithm: "zstd",
					Level:     3,
				},
			},

			WAL: WALConfig{
				SyncMode:       "async",
				SyncInterval:   Duration(time.Second),
				MaxSegmentSize: 100 * 1024 * 1024, // 100MB
			},

			Flush: FlushConfig{
				Interval:      Duration(10 * time.Minute),
				MaxBufferSize: 100 * 1024 * 1024, // 100MB
			},

			Compaction: CompactionConfig{
				Workers: 4,
				Schedule: CompactionScheduleConfig{
					RawTo5Min:       "0 * * * *",
					FiveMinToHourly: "0 2 * * *",
					HourlyToDaily:   "0 3 * * 0",
					DailyToWeekly:   "0 4 1 * *",
				},
			},

			Retention: RetentionConfig{
				Raw:     Duration(48 * time.Hour),
				FiveMin: Duration(30 * 24 * time.Hour),
				Hourly:  Duration(90 * 24 * time.Hour),
				Daily:   Duration(2 * 365 * 24 * time.Hour),
				Weekly:  Duration(10 * 365 * 24 * time.Hour),
			},

			Backpressure: BackpressureConfig{
				Enabled: true,
				Thresholds: BackpressureThresholds{
					Warning:   0.50,
					Critical:  0.80,
					Emergency: 0.95,
				},
				Recovery: BackpressureRecovery{
					Hysteresis: 0.10,
					Cooldown:   Duration(30 * time.Second),
				},
			},

			Query: QueryConfig{
				MemoryLimit: "2GB",
				Timeout:     Duration(30 * time.Second),
				MaxRows:     1000000,
			},
		},

		SNMP: SNMPDefaults{
			TimeoutMs:  5000,
			Retries:    2,
			IntervalMs: 60000,
			BufferSize: 3600,
		},
	}
}

// ApplyLegacyFields migrates deprecated fields to new locations.
func (c *Config) ApplyLegacyFields() {
	if c.Storage != nil {
		// Migrate storage.db_path → metastore.path
		if c.Storage.DBPath != "" && c.Metastore.Path == "stalker.db" {
			c.Metastore.Path = c.Storage.DBPath
		}
		// Migrate storage.secret_key_path → metastore.secret_key_path
		if c.Storage.SecretKeyPath != "" && c.Metastore.SecretKeyPath == "" {
			c.Metastore.SecretKeyPath = c.Storage.SecretKeyPath
		}
		// Migrate flush intervals
		if c.Storage.StateFlushIntervalSec > 0 {
			c.Metastore.StateFlushInterval = Duration(time.Duration(c.Storage.StateFlushIntervalSec) * time.Second)
		}
		if c.Storage.StatsFlushIntervalSec > 0 {
			c.Metastore.StatsFlushInterval = Duration(time.Duration(c.Storage.StatsFlushIntervalSec) * time.Second)
		}
	}
}

// =============================================================================
// Custom Types
// =============================================================================

// Duration is a time.Duration that can be unmarshaled from YAML.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		// Try as int (seconds)
		var i int
		if err := unmarshal(&i); err != nil {
			return err
		}
		*d = Duration(time.Duration(i) * time.Second)
		return nil
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// Duration returns the time.Duration value.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// ByteSize is a size in bytes that can be unmarshaled from YAML.
// Supports: "100MB", "1GB", "500KB", or plain bytes.
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		// Try as int64
		var i int64
		if err := unmarshal(&i); err != nil {
			return err
		}
		*b = ByteSize(i)
		return nil
	}
	size, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(size)
	return nil
}

// parseByteSize parses a size string like "100MB" or "1GB".
func parseByteSize(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	units := map[string]int64{
		"B":  1,
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"TB": 1024 * 1024 * 1024 * 1024,
	}

	for suffix, multiplier := range units {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSuffix(s, suffix)
			numStr = strings.TrimSpace(numStr)
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse byte size %q: %w", s, err)
			}
			return n * multiplier, nil
		}
	}

	// Try as plain number
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte size %q: %w", s, err)
	}
	return n, nil
}

// Bytes returns the size in bytes.
func (b ByteSize) Bytes() int64 {
	return int64(b)
}

// =============================================================================
// Helper Functions
// =============================================================================

// convertPollerDefaults converts YAML defaults to store defaults.
func convertPollerDefaults(cfg *PollerDefaultsConfig) *store.PollerDefaults {
	if cfg == nil {
		return nil
	}

	return &store.PollerDefaults{
		Community:     cfg.Community,
		SecurityName:  cfg.SecurityName,
		SecurityLevel: cfg.SecurityLevel,
		AuthProtocol:  cfg.AuthProtocol,
		AuthPassword:  cfg.AuthPassword,
		PrivProtocol:  cfg.PrivProtocol,
		PrivPassword:  cfg.PrivPassword,
		IntervalMs:    cfg.IntervalMs,
		TimeoutMs:     cfg.TimeoutMs,
		Retries:       cfg.Retries,
		BufferSize:    cfg.BufferSize,
	}
}

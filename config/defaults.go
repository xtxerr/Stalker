// Package config provides configuration defaults and utilities
// for the stalker application.
//
// This package defines all configurable constants with documented defaults.
// Users can override these values via config.yaml or environment variables.
package config

import "time"

// =============================================================================
// Network Defaults
// =============================================================================

const (
	// DefaultListenAddress is the default server listen address.
	// Override via config: server.listen
	DefaultListenAddress = "0.0.0.0:9161"

	// DefaultMaxMessageSize limits protobuf message size to prevent OOM.
	// 16 MiB should be sufficient for any reasonable message.
	// Override via config: server.max_message_size
	DefaultMaxMessageSize = 16 * 1024 * 1024
)

// =============================================================================
// Session Defaults
// =============================================================================

const (
	// DefaultAuthTimeoutSec is the time allowed for authentication after connect.
	// Clients must authenticate within this window or be disconnected.
	// Override via config: session.auth_timeout_sec
	DefaultAuthTimeoutSec = 30

	// DefaultSessionCleanupIntervalSec is how often closed sessions are cleaned up.
	// This is internal housekeeping to free memory from disconnected sessions.
	// Override via config: session.cleanup_interval_sec
	DefaultSessionCleanupIntervalSec = 60

	// DefaultSessionSendBufferSize is the capacity of the per-session send channel.
	// Larger values allow more messages to be queued during slow clients.
	// Range: 100-10000
	// Override via config: session.send_buffer_size
	DefaultSessionSendBufferSize = 1000

	// DefaultSessionSendTimeoutMs is how long to wait when the send buffer is full.
	// After this timeout, the message is dropped.
	// Override via config: session.send_timeout_ms
	DefaultSessionSendTimeoutMs = 100
)

// =============================================================================
// Scheduler Defaults
// =============================================================================

const (
	// DefaultPollerWorkers is the number of concurrent poll workers.
	// Each worker can execute one SNMP poll at a time.
	// Override via config: poller.workers
	DefaultPollerWorkers = 100

	// DefaultPollerQueueSize is the job queue capacity.
	// When full, polls are delayed (backpressure).
	// Override via config: poller.queue_size
	DefaultPollerQueueSize = 10000

	// DefaultSchedulerTickInterval is how often the scheduler checks for due polls.
	// Override via config: poller.tick_interval
	DefaultSchedulerTickInterval = 10 * time.Millisecond
)

// =============================================================================
// Shutdown Defaults
// =============================================================================

const (
	// DefaultDrainTimeoutSec is how long to wait for in-flight polls during shutdown.
	// This follows the Kubernetes convention (terminationGracePeriodSeconds = 30s).
	// After this timeout, remaining jobs are abandoned.
	// Override via config: server.drain_timeout_sec
	DefaultDrainTimeoutSec = 30
)

// =============================================================================
// SNMP Defaults
// =============================================================================

const (
	// DefaultSNMPTimeoutMs is the timeout for a single SNMP request.
	// Override via config: snmp.timeout_ms
	DefaultSNMPTimeoutMs = 5000

	// DefaultSNMPRetries is the number of retry attempts after timeout.
	// Override via config: snmp.retries
	DefaultSNMPRetries = 2

	// DefaultSNMPIntervalMs is the default polling interval.
	// Override via config: snmp.interval_ms
	DefaultSNMPIntervalMs = 60000

	// DefaultSNMPBufferSize is the default sample buffer size.
	// Override via config: snmp.buffer_size
	DefaultSNMPBufferSize = 3600
)

// =============================================================================
// Sync Manager Defaults
// =============================================================================

const (
	// DefaultStateFlushIntervalSec is how often poller state is persisted.
	// Override via config: storage.state_flush_interval_sec
	DefaultStateFlushIntervalSec = 5

	// DefaultStatsFlushIntervalSec is how often poller stats are persisted.
	// Override via config: storage.stats_flush_interval_sec
	DefaultStatsFlushIntervalSec = 10

	// DefaultSampleBatchSize is the number of samples before flush.
	// Override via config: storage.sample_batch_size
	DefaultSampleBatchSize = 1000

	// DefaultSampleFlushTimeoutSec is the max hold time for samples.
	// Override via config: storage.sample_flush_timeout_sec
	DefaultSampleFlushTimeoutSec = 5

	// DefaultMaxRetries is the max retry attempts for database operations.
	// Override via config: storage.max_retries
	DefaultMaxRetries = 3

	// DefaultMaxOverflowSize is the max size of the overflow buffer for samples.
	// Override via config: storage.max_overflow_size
	DefaultMaxOverflowSize = 10000
)

// =============================================================================
// Rate Limiting Defaults
// =============================================================================

const (
	// DefaultAuthRateLimitPerMinute is the max FAILED auth attempts per IP per minute.
	// Only failed authentication attempts are counted. Successful authentications
	// reset the failure counter. After reaching this limit, the IP is temporarily
	// blocked from connecting until the time window expires.
	// Override via config: auth.rate_limit_per_minute
	DefaultAuthRateLimitPerMinute = 5

	// DefaultAuthRateLimitBurst is reserved for future use (token bucket burst capacity).
	// Override via config: auth.rate_limit_burst
	DefaultAuthRateLimitBurst = 10
)

// =============================================================================
// Cache Defaults
// =============================================================================

const (
	// DefaultConfigCacheTTLSec is how long resolved configs are cached.
	// Override via config: cache.config_ttl_sec
	DefaultConfigCacheTTLSec = 30
)

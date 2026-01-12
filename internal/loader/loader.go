// Package loader handles configuration file loading and validation.
package loader

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/errors"
	"github.com/xtxerr/stalker/internal/manager"
	"github.com/xtxerr/stalker/internal/store"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Configuration Structures
// =============================================================================

// Config is the root configuration structure.
type Config struct {
	// Server network settings
	Listen string `yaml:"listen"`

	// TLS configuration
	TLS TLSConfig `yaml:"tls"`

	// Authentication
	Auth AuthConfig `yaml:"auth"`

	// Session management
	Session SessionConfig `yaml:"session"`

	// Scheduler (poller workers)
	Poller PollerConfig `yaml:"poller"`

	// Storage settings
	Storage StorageConfig `yaml:"storage"`

	// SNMP defaults
	SNMP SNMPConfig `yaml:"snmp"`

	// Shutdown behavior
	Shutdown ShutdownConfig `yaml:"shutdown"`

	// Include additional config files
	Include []string `yaml:"include"`

	// Pre-configured namespaces (optional)
	Namespaces map[string]*NamespaceConfig `yaml:"namespaces"`
}

// TLSConfig holds TLS settings.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	Tokens []TokenConfig `yaml:"tokens"`
}

// TokenConfig represents an authentication token.
type TokenConfig struct {
	// ID is the unique identifier for this token (for logging/auditing)
	ID string `yaml:"id"`

	// Token is the actual secret value
	Token string `yaml:"token"`

	// Namespaces restricts access to specific namespaces (empty = all)
	Namespaces []string `yaml:"namespaces"`
}

// SessionConfig holds session management settings.
type SessionConfig struct {
	// AuthTimeoutSec is how long a client has to authenticate after connecting.
	// Default: 30 seconds
	AuthTimeoutSec int `yaml:"auth_timeout_sec"`

	// ReconnectWindowSec is how long a disconnected session remains valid for reconnection.
	// Default: 600 seconds (10 minutes)
	ReconnectWindowSec int `yaml:"reconnect_window_sec"`

	// CleanupIntervalSec is the global default for how often expired sessions are cleaned up.
	// Can be overridden per-namespace in namespace config.
	// Default: 60 seconds
	CleanupIntervalSec int `yaml:"cleanup_interval_sec"`
}

// PollerConfig holds scheduler/worker settings.
type PollerConfig struct {
	// Workers is the number of concurrent poll workers.
	// Each worker executes one SNMP poll at a time.
	// Default: 100
	Workers int `yaml:"workers"`

	// QueueSize is the job queue capacity.
	// When full, new polls are delayed (backpressure).
	// Default: 10000
	QueueSize int `yaml:"queue_size"`
}

// StorageConfig holds database and batching settings.
type StorageConfig struct {
	// DBPath is the path to the DuckDB database file.
	// Default: "stalker.db"
	DBPath string `yaml:"db_path"`

	// SecretKeyPath is the path to the secret encryption key (32 bytes).
	// If not set, secrets cannot be stored.
	SecretKeyPath string `yaml:"secret_key_path"`

	// SampleBatchSize is how many samples to batch before flushing to DB.
	// Default: 1000
	SampleBatchSize int `yaml:"sample_batch_size"`

	// SampleFlushTimeoutSec is the maximum time to hold samples before flushing.
	// Default: 5 seconds
	SampleFlushTimeoutSec int `yaml:"sample_flush_timeout_sec"`

	// StateFlushIntervalSec is how often poller state is persisted.
	// Default: 5 seconds
	StateFlushIntervalSec int `yaml:"state_flush_interval_sec"`

	// StatsFlushIntervalSec is how often poller stats are persisted.
	// Default: 10 seconds
	StatsFlushIntervalSec int `yaml:"stats_flush_interval_sec"`
}

// SNMPConfig holds default SNMP settings.
type SNMPConfig struct {
	// TimeoutMs is the default timeout for SNMP requests in milliseconds.
	// Default: 5000 (5 seconds)
	TimeoutMs uint32 `yaml:"timeout_ms"`

	// Retries is the default number of retry attempts.
	// Default: 2
	Retries uint32 `yaml:"retries"`

	// IntervalMs is the default polling interval in milliseconds.
	// Default: 60000 (1 minute)
	IntervalMs uint32 `yaml:"interval_ms"`

	// BufferSize is the default sample buffer size per poller.
	// Default: 3600 (1 hour at 1 sample/second)
	BufferSize uint32 `yaml:"buffer_size"`
}

// ShutdownConfig holds graceful shutdown settings.
type ShutdownConfig struct {
	// DrainTimeoutSec is how long to wait for in-flight polls during shutdown.
	// After this timeout, remaining jobs are abandoned.
	// Similar to Kubernetes terminationGracePeriodSeconds.
	// Default: 30 seconds
	DrainTimeoutSec int `yaml:"drain_timeout_sec"`
}

// =============================================================================
// Namespace Configuration (for pre-configuration via YAML)
// =============================================================================

// NamespaceConfig represents namespace configuration in YAML.
type NamespaceConfig struct {
	Description string                   `yaml:"description"`
	Defaults    *PollerDefaults          `yaml:"defaults"`
	Targets     map[string]*TargetConfig `yaml:"targets"`

	// Session cleanup interval specific to this namespace.
	// Overrides the global session.cleanup_interval_sec.
	// If not set, uses the global default.
	SessionCleanupIntervalSec *int `yaml:"session_cleanup_interval_sec"`
}

// PollerDefaults holds default settings that can be inherited.
type PollerDefaults struct {
	// SNMP v2c
	Community string `yaml:"community"`

	// SNMPv3
	SecurityName  string `yaml:"security_name"`
	SecurityLevel string `yaml:"security_level"`
	AuthProtocol  string `yaml:"auth_protocol"`
	AuthPassword  string `yaml:"auth_password"`
	PrivProtocol  string `yaml:"priv_protocol"`
	PrivPassword  string `yaml:"priv_password"`

	// Timing
	IntervalMs *uint32 `yaml:"interval_ms"`
	TimeoutMs  *uint32 `yaml:"timeout_ms"`
	Retries    *uint32 `yaml:"retries"`
	BufferSize *uint32 `yaml:"buffer_size"`
}

// TargetConfig represents target configuration in YAML.
type TargetConfig struct {
	Description string            `yaml:"description"`
	Labels      map[string]string `yaml:"labels"`
	Defaults    *PollerDefaults   `yaml:"defaults"`
	Pollers     map[string]*PollerConfigYAML `yaml:"pollers"`
}

// PollerConfigYAML represents poller configuration in YAML.
type PollerConfigYAML struct {
	Description    string          `yaml:"description"`
	Protocol       string          `yaml:"protocol"`
	ProtocolConfig map[string]any  `yaml:"protocol_config"`
	PollingConfig  *PollingConfig  `yaml:"polling_config"`
	AdminState     string          `yaml:"admin_state"`
}

// PollingConfig holds poller-specific polling settings.
type PollingConfig struct {
	IntervalMs *uint32 `yaml:"interval_ms"`
	TimeoutMs  *uint32 `yaml:"timeout_ms"`
	Retries    *uint32 `yaml:"retries"`
	BufferSize *uint32 `yaml:"buffer_size"`
}

// TreeConfig represents tree node configuration in YAML.
type TreeConfig struct {
	Directories []DirConfig  `yaml:"directories"`
	Links       []LinkConfig `yaml:"links"`
}

// DirConfig represents a directory in the tree.
type DirConfig struct {
	Path        string `yaml:"path"`
	Description string `yaml:"description"`
}

// LinkConfig represents a link in the tree.
type LinkConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // target/poller
	Ref  string `yaml:"ref"`  // target name or target/poller
}

// SecretConfig represents a secret.
type SecretConfig struct {
	Type  string `yaml:"type"`
	Value []byte `yaml:"value"`
}

// =============================================================================
// Load
// =============================================================================

// Load loads configuration from a file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Process includes
	baseDir := filepath.Dir(path)
	for _, inc := range cfg.Include {
		incPath := inc
		if !filepath.IsAbs(incPath) {
			incPath = filepath.Join(baseDir, inc)
		}

		// Support glob patterns
		matches, err := filepath.Glob(incPath)
		if err != nil {
			return nil, fmt.Errorf("glob %s: %w", inc, err)
		}

		for _, match := range matches {
			nsCfg, err := loadNamespaceFile(match)
			if err != nil {
				return nil, fmt.Errorf("load %s: %w", match, err)
			}

			// Merge into config
			if cfg.Namespaces == nil {
				cfg.Namespaces = make(map[string]*NamespaceConfig)
			}
			for name, ns := range nsCfg {
				cfg.Namespaces[name] = ns
			}
		}
	}

	// Apply defaults
	cfg.applyDefaults()

	return &cfg, nil
}

// loadNamespaceFile loads namespaces from a file.
func loadNamespaceFile(path string) (map[string]*NamespaceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := os.ExpandEnv(string(data))

	var result map[string]*NamespaceConfig
	if err := yaml.Unmarshal([]byte(expanded), &result); err != nil {
		return nil, err
	}

	return result, nil
}

// applyDefaults applies default values to missing configuration fields.
func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = config.DefaultListenAddress
	}

	// Session defaults
	if c.Session.AuthTimeoutSec == 0 {
		c.Session.AuthTimeoutSec = config.DefaultAuthTimeoutSec
	}
	if c.Session.ReconnectWindowSec == 0 {
		c.Session.ReconnectWindowSec = config.DefaultReconnectWindowSec
	}
	if c.Session.CleanupIntervalSec == 0 {
		c.Session.CleanupIntervalSec = config.DefaultSessionCleanupIntervalSec
	}

	// Poller defaults
	if c.Poller.Workers == 0 {
		c.Poller.Workers = config.DefaultPollerWorkers
	}
	if c.Poller.QueueSize == 0 {
		c.Poller.QueueSize = config.DefaultPollerQueueSize
	}

	// Storage defaults
	if c.Storage.DBPath == "" {
		c.Storage.DBPath = config.DefaultDBPath
	}
	if c.Storage.SampleBatchSize == 0 {
		c.Storage.SampleBatchSize = config.DefaultSampleBatchSize
	}
	if c.Storage.SampleFlushTimeoutSec == 0 {
		c.Storage.SampleFlushTimeoutSec = config.DefaultSampleFlushTimeoutSec
	}
	if c.Storage.StateFlushIntervalSec == 0 {
		c.Storage.StateFlushIntervalSec = config.DefaultStateFlushIntervalSec
	}
	if c.Storage.StatsFlushIntervalSec == 0 {
		c.Storage.StatsFlushIntervalSec = config.DefaultStatsFlushIntervalSec
	}

	// SNMP defaults
	if c.SNMP.TimeoutMs == 0 {
		c.SNMP.TimeoutMs = uint32(config.DefaultSNMPTimeoutMs)
	}
	if c.SNMP.Retries == 0 {
		c.SNMP.Retries = uint32(config.DefaultSNMPRetries)
	}
	if c.SNMP.IntervalMs == 0 {
		c.SNMP.IntervalMs = uint32(config.DefaultSNMPIntervalMs)
	}
	if c.SNMP.BufferSize == 0 {
		c.SNMP.BufferSize = uint32(config.DefaultSNMPBufferSize)
	}

	// Shutdown defaults
	if c.Shutdown.DrainTimeoutSec == 0 {
		c.Shutdown.DrainTimeoutSec = config.DefaultDrainTimeoutSec
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	errs := errors.NewValidationErrors()

	// Validate auth tokens
	if len(c.Auth.Tokens) == 0 {
		errs.AddField("auth.tokens", "at least one token is required")
	}
	for i, t := range c.Auth.Tokens {
		if t.ID == "" {
			errs.AddField(fmt.Sprintf("auth.tokens[%d].id", i), "cannot be empty")
		}
		if t.Token == "" {
			errs.AddField(fmt.Sprintf("auth.tokens[%d].token", i), "cannot be empty")
		}
	}

	// Validate namespaces
	for name, ns := range c.Namespaces {
		if name == "" {
			errs.AddField("namespace.name", "cannot be empty")
			continue
		}

		// Validate targets
		for tName, target := range ns.Targets {
			if tName == "" {
				errs.AddField(fmt.Sprintf("namespace.%s.target.name", name), "cannot be empty")
				continue
			}

			// Validate pollers
			for pName, poller := range target.Pollers {
				if pName == "" {
					errs.AddField(fmt.Sprintf("namespace.%s.target.%s.poller.name", name, tName), "cannot be empty")
				}
				if poller.Protocol == "" {
					errs.AddField(fmt.Sprintf("namespace.%s.target.%s.poller.%s.protocol", name, tName, pName), "cannot be empty")
				}
			}
		}
	}

	return errs.Err()
}

// =============================================================================
// Apply
// =============================================================================

// ApplyResult holds statistics from applying configuration.
type ApplyResult struct {
	NamespacesCreated int
	TargetsCreated    int
	TargetsUpdated    int
	PollersCreated    int
	PollersUpdated    int
	TreeNodesCreated  int
	SecretsCreated    int
	Errors            []string
}

// Apply applies the loaded configuration to the manager.
func Apply(cfg *Config, mgr *manager.Manager) (*ApplyResult, error) {
	result := &ApplyResult{}

	for nsName, nsCfg := range cfg.Namespaces {
		// Create or update namespace
		existing, err := mgr.Namespaces.Get(nsName)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("get namespace %s: %v", nsName, err))
			continue
		}

		if existing == nil {
			// Create namespace
			ns := &store.Namespace{
				Name:        nsName,
				Description: nsCfg.Description,
			}
			if nsCfg.Defaults != nil {
				ns.Config = &store.NamespaceConfig{
					Defaults: convertPollerDefaults(nsCfg.Defaults),
				}
			}
			// Store session cleanup interval if specified
			if nsCfg.SessionCleanupIntervalSec != nil {
				if ns.Config == nil {
					ns.Config = &store.NamespaceConfig{}
				}
				ns.Config.SessionCleanupIntervalSec = nsCfg.SessionCleanupIntervalSec
			}

			if err := mgr.Namespaces.Create(ns); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("create namespace %s: %v", nsName, err))
				continue
			}
			result.NamespacesCreated++
		}

		// Apply targets
		for tName, tCfg := range nsCfg.Targets {
			if err := applyTarget(mgr, nsName, tName, tCfg, result); err != nil {
				result.Errors = append(result.Errors, err.Error())
			}
		}
	}

	if len(result.Errors) > 0 {
		return result, fmt.Errorf("apply had %d errors", len(result.Errors))
	}
	return result, nil
}

func applyTarget(mgr *manager.Manager, namespace, name string, cfg *TargetConfig, result *ApplyResult) error {
	existing, err := mgr.Targets.Get(namespace, name)
	if err != nil {
		return fmt.Errorf("get target %s/%s: %w", namespace, name, err)
	}

	if existing == nil {
		// Create target
		target := &store.Target{
			Namespace:   namespace,
			Name:        name,
			Description: cfg.Description,
			Labels:      cfg.Labels,
		}
		if cfg.Defaults != nil {
			target.Config = &store.TargetConfig{
				Defaults: convertPollerDefaults(cfg.Defaults),
			}
		}
		if err := mgr.Targets.Create(target); err != nil {
			return fmt.Errorf("create target %s/%s: %w", namespace, name, err)
		}
		result.TargetsCreated++
	} else {
		// Update target
		existing.Description = cfg.Description
		existing.Labels = cfg.Labels
		if cfg.Defaults != nil {
			existing.Config = &store.TargetConfig{
				Defaults: convertPollerDefaults(cfg.Defaults),
			}
		}
		if err := mgr.Targets.Update(existing); err != nil {
			return fmt.Errorf("update target %s/%s: %w", namespace, name, err)
		}
		result.TargetsUpdated++
	}

	// Apply pollers
	for pName, pCfg := range cfg.Pollers {
		if err := applyPoller(mgr, namespace, name, pName, pCfg, result); err != nil {
			return err
		}
	}

	return nil
}

func applyPoller(mgr *manager.Manager, namespace, target, name string, cfg *PollerConfigYAML, result *ApplyResult) error {
	existing, err := mgr.Pollers.Get(namespace, target, name)
	if err != nil {
		return fmt.Errorf("get poller %s/%s/%s: %w", namespace, target, name, err)
	}

	// Convert protocol config to JSON
	protocolConfigJSON, err := yaml.Marshal(cfg.ProtocolConfig)
	if err != nil {
		return fmt.Errorf("marshal protocol config: %w", err)
	}

	if existing == nil {
		// Create poller
		poller := &store.Poller{
			Namespace:      namespace,
			Target:         target,
			Name:           name,
			Description:    cfg.Description,
			Protocol:       cfg.Protocol,
			ProtocolConfig: protocolConfigJSON,
			AdminState:     cfg.AdminState,
		}
		if cfg.PollingConfig != nil {
			poller.PollingConfig = &store.PollingConfig{
				IntervalMs: cfg.PollingConfig.IntervalMs,
				TimeoutMs:  cfg.PollingConfig.TimeoutMs,
				Retries:    cfg.PollingConfig.Retries,
				BufferSize: cfg.PollingConfig.BufferSize,
			}
		}
		if err := mgr.Pollers.Create(poller); err != nil {
			return fmt.Errorf("create poller %s/%s/%s: %w", namespace, target, name, err)
		}
		result.PollersCreated++
	} else {
		// Update poller
		existing.Description = cfg.Description
		existing.Protocol = cfg.Protocol
		existing.ProtocolConfig = protocolConfigJSON
		if cfg.AdminState != "" {
			existing.AdminState = cfg.AdminState
		}
		if cfg.PollingConfig != nil {
			existing.PollingConfig = &store.PollingConfig{
				IntervalMs: cfg.PollingConfig.IntervalMs,
				TimeoutMs:  cfg.PollingConfig.TimeoutMs,
				Retries:    cfg.PollingConfig.Retries,
				BufferSize: cfg.PollingConfig.BufferSize,
			}
		}
		if err := mgr.Pollers.Update(existing); err != nil {
			return fmt.Errorf("update poller %s/%s/%s: %w", namespace, target, name, err)
		}
		result.PollersUpdated++
	}

	return nil
}

func convertPollerDefaults(cfg *PollerDefaults) *store.PollerDefaults {
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

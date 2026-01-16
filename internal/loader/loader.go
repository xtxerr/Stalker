// Package loader handles configuration file loading, validation, and application.
//
// LOCATION: internal/loader/loader.go
//
// This package is responsible for:
//   - Loading YAML configuration files
//   - Expanding environment variables
//   - Processing include directives
//   - Applying configuration to the manager
//   - Converting between YAML and internal representations

package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xtxerr/stalker/internal/errors"
	"github.com/xtxerr/stalker/internal/manager"
	storageconfig "github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/store"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Load
// =============================================================================

// Load loads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Expand environment variables
	expanded := os.ExpandEnv(string(data))

	// Start with defaults
	cfg := DefaultConfig()

	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Apply legacy field migration
	cfg.ApplyLegacyFields()

	// Process includes (load additional namespace files)
	baseDir := filepath.Dir(path)
	if err := processIncludes(cfg, baseDir); err != nil {
		return nil, err
	}

	return cfg, nil
}

// processIncludes loads and merges included configuration files.
func processIncludes(cfg *Config, baseDir string) error {
	for _, pattern := range cfg.Include {
		// Resolve relative paths
		if !filepath.IsAbs(pattern) {
			pattern = filepath.Join(baseDir, pattern)
		}

		// Expand glob pattern
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("invalid include pattern %q: %w", pattern, err)
		}

		for _, match := range matches {
			if err := loadInclude(cfg, match); err != nil {
				return fmt.Errorf("load include %q: %w", match, err)
			}
		}
	}

	return nil
}

// loadInclude loads a single include file and merges it into the config.
func loadInclude(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	expanded := os.ExpandEnv(string(data))

	// Parse into a partial config
	var partial Config
	if err := yaml.Unmarshal([]byte(expanded), &partial); err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	// Merge namespaces
	if cfg.Namespaces == nil {
		cfg.Namespaces = make(map[string]*NamespaceConfig)
	}
	for name, ns := range partial.Namespaces {
		cfg.Namespaces[name] = ns
	}

	// Merge secrets
	if cfg.Secrets == nil {
		cfg.Secrets = make(map[string]*SecretConfig)
	}
	for name, secret := range partial.Secrets {
		cfg.Secrets[name] = secret
	}

	return nil
}

// =============================================================================
// Validate
// =============================================================================

// Validate validates the configuration.
func Validate(cfg *Config) error {
	errs := errors.NewValidationErrors()

	// Server validation
	if cfg.Listen == "" {
		errs.AddField("listen", "cannot be empty")
	}

	// Auth validation
	if len(cfg.Auth.Tokens) == 0 {
		errs.AddField("auth.tokens", "at least one token is required")
	}
	for i, t := range cfg.Auth.Tokens {
		if t.ID == "" {
			errs.AddField(fmt.Sprintf("auth.tokens[%d].id", i), "cannot be empty")
		}
		if t.Token == "" {
			errs.AddField(fmt.Sprintf("auth.tokens[%d].token", i), "cannot be empty")
		}
	}

	// Metastore validation
	if cfg.Metastore.Path == "" {
		errs.AddField("metastore.path", "cannot be empty")
	}

	// Samplestore validation (if enabled)
	if cfg.Samplestore.Enabled {
		if cfg.Samplestore.DataDir == "" {
			errs.AddField("samplestore.data_dir", "cannot be empty when enabled")
		}
	}

	// Namespace validation
	for name, ns := range cfg.Namespaces {
		if name == "" {
			errs.AddField("namespace.name", "cannot be empty")
			continue
		}

		for tName, target := range ns.Targets {
			if tName == "" {
				errs.AddField(fmt.Sprintf("namespace.%s.target.name", name), "cannot be empty")
				continue
			}

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
// Conversion: Config → Storage Config (Samplestore)
// =============================================================================

// ToSamplestoreConfig converts the samplestore configuration to the internal storage config.
func ToSamplestoreConfig(cfg *SamplestoreConfig) *storageconfig.Config {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	return &storageconfig.Config{
		DataDir: cfg.DataDir,

		Scale: storageconfig.ScaleConfig{
			PollerCount:     cfg.Scale.PollerCount,
			PollIntervalSec: cfg.Scale.PollIntervalSec,
		},

		Features: storageconfig.FeaturesConfig{
			RawBuffer: storageconfig.RawBufferConfig{
				Enabled:     cfg.Features.Buffer.Enabled,
				Duration:    cfg.Features.Buffer.Duration.Duration(),
				MemoryLimit: cfg.Features.Buffer.MemoryLimit,
			},
			Percentile: storageconfig.PercentileConfig{
				Enabled:  cfg.Features.Percentiles.Enabled,
				Accuracy: cfg.Features.Percentiles.Accuracy,
			},
			Compression: storageconfig.CompressionConfig{
				Algorithm: cfg.Features.Compression.Algorithm,
				Level:     cfg.Features.Compression.Level,
			},
		},

		Retention: storageconfig.RetentionConfig{
			Raw:     cfg.Retention.Raw.Duration(),
			FiveMin: cfg.Retention.FiveMin.Duration(),
			Hourly:  cfg.Retention.Hourly.Duration(),
			Daily:   cfg.Retention.Daily.Duration(),
			Weekly:  cfg.Retention.Weekly.Duration(),
		},

		Ingestion: storageconfig.IngestionConfig{
			WAL: storageconfig.WALConfig{
				Dir:            cfg.WAL.Dir,
				SyncMode:       cfg.WAL.SyncMode,
				SyncInterval:   cfg.WAL.SyncInterval.Duration(),
				MaxSegmentSize: cfg.WAL.MaxSegmentSize.Bytes(),
			},
			Flush: storageconfig.FlushConfig{
				Interval:      cfg.Flush.Interval.Duration(),
				MaxBufferSize: cfg.Flush.MaxBufferSize.Bytes(),
			},
		},

		Compaction: storageconfig.CompactionConfig{
			Workers: cfg.Compaction.Workers,
			Schedule: storageconfig.CompactionSchedule{
				RawTo5Min:       cfg.Compaction.Schedule.RawTo5Min,
				FiveMinToHourly: cfg.Compaction.Schedule.FiveMinToHourly,
				HourlyToDaily:   cfg.Compaction.Schedule.HourlyToDaily,
				DailyToWeekly:   cfg.Compaction.Schedule.DailyToWeekly,
			},
		},

		Backpressure: storageconfig.BackpressureConfig{
			Enabled: cfg.Backpressure.Enabled,
			Thresholds: storageconfig.BackpressureThresholds{
				Warning:   cfg.Backpressure.Thresholds.Warning,
				Critical:  cfg.Backpressure.Thresholds.Critical,
				Emergency: cfg.Backpressure.Thresholds.Emergency,
			},
			Recovery: storageconfig.BackpressureRecovery{
				Hysteresis: cfg.Backpressure.Recovery.Hysteresis,
				Cooldown:   cfg.Backpressure.Recovery.Cooldown.Duration(),
			},
		},

		Query: storageconfig.QueryConfig{
			MemoryLimit: cfg.Query.MemoryLimit,
			Timeout:     cfg.Query.Timeout.Duration(),
			MaxRows:     cfg.Query.MaxRows,
		},
	}
}

// =============================================================================
// Conversion: Config → Store Config (Metastore)
// =============================================================================

// ToMetastoreConfig converts the metastore configuration to the internal store config.
func ToMetastoreConfig(cfg *MetastoreConfig) *store.Config {
	if cfg == nil {
		return nil
	}

	return &store.Config{
		DBPath:          cfg.Path,
		SecretKeyPath:   cfg.SecretKeyPath,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime.Duration(),
		QueryTimeout:    cfg.QueryTimeout.Duration(),
	}
}

// =============================================================================
// Apply
// =============================================================================

// ApplyResult holds statistics from applying configuration.
type ApplyResult struct {
	NamespacesCreated int
	NamespacesUpdated int
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
					Defaults: convertPollerDefaultsToStore(nsCfg.Defaults),
				}
			}
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
		} else {
			result.NamespacesUpdated++
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
				Defaults: convertPollerDefaultsToStore(cfg.Defaults),
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
				Defaults: convertPollerDefaultsToStore(cfg.Defaults),
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

func applyPoller(mgr *manager.Manager, namespace, target, name string, cfg *PollerYAMLConfig, result *ApplyResult) error {
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

func convertPollerDefaultsToStore(cfg *PollerDefaultsConfig) *store.PollerDefaults {
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

// =============================================================================
// Config Watcher
// =============================================================================

// Watcher watches a config file for changes and reloads it.
type Watcher struct {
	path     string
	mgr      *manager.Manager
	callback func(*ApplyResult)
	done     chan struct{}
	modTime  time.Time
}

// NewWatcher creates a new config file watcher.
func NewWatcher(path string, mgr *manager.Manager, callback func(*ApplyResult)) *Watcher {
	return &Watcher{
		path:     path,
		mgr:      mgr,
		callback: callback,
		done:     make(chan struct{}),
	}
}

// Start begins watching the config file.
func (w *Watcher) Start() {
	// Get initial mod time
	if info, err := os.Stat(w.path); err == nil {
		w.modTime = info.ModTime()
	}

	go w.watch()
}

// Stop stops watching.
func (w *Watcher) Stop() {
	close(w.done)
}

func (w *Watcher) watch() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			info, err := os.Stat(w.path)
			if err != nil {
				continue
			}

			if info.ModTime().After(w.modTime) {
				w.modTime = info.ModTime()
				w.reload()
			}
		}
	}
}

func (w *Watcher) reload() {
	cfg, err := Load(w.path)
	if err != nil {
		if w.callback != nil {
			w.callback(&ApplyResult{
				Errors: []string{fmt.Sprintf("reload config: %v", err)},
			})
		}
		return
	}

	result, _ := Apply(cfg, w.mgr)
	if w.callback != nil {
		w.callback(result)
	}
}

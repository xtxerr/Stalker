// Package loader - Configuration Types
//
// LOCATION: internal/loader/types.go
//
// Defines the YAML configuration structure for stalkerd.

package loader

import (
	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Root Configuration
// =============================================================================

// Config is the root configuration structure.
type Config struct {
	// Runtime settings (not synced to DB)
	Listen  string     `yaml:"listen"`
	TLS     *TLSConfig `yaml:"tls"`
	Workers int        `yaml:"workers"`

	// Storage settings
	Storage *StorageConfig `yaml:"storage"`

	// Sync engine configuration
	Sync *SyncConfig `yaml:"sync"`

	// Tree configuration
	Tree *TreeConfig `yaml:"tree"`

	// Namespaces with targets and pollers
	Namespaces map[string]*NamespaceConfig `yaml:"namespaces"`

	// Global defaults
	Defaults *DefaultsConfig `yaml:"defaults"`

	// Secrets
	Secrets map[string]*SecretConfig `yaml:"secrets"`
}

// =============================================================================
// Runtime Configuration (not synced)
// =============================================================================

// TLSConfig holds TLS settings.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// StorageConfig holds database settings.
type StorageConfig struct {
	DBPath string `yaml:"db_path"`
}

// =============================================================================
// Defaults Configuration
// =============================================================================

// DefaultsConfig holds global default settings.
type DefaultsConfig struct {
	SNMP *SNMPDefaults `yaml:"snmp"`
}

// SNMPDefaults holds default SNMP settings.
type SNMPDefaults struct {
	Version       string `yaml:"version"`
	Community     string `yaml:"community"`
	Port          uint16 `yaml:"port"`
	TimeoutMs     uint32 `yaml:"timeout_ms"`
	Retries       uint32 `yaml:"retries"`
	MaxRepetition uint32 `yaml:"max_repetition"`

	// SNMPv3
	SecurityName  string `yaml:"security_name"`
	SecurityLevel string `yaml:"security_level"`
	AuthProtocol  string `yaml:"auth_protocol"`
	AuthPassword  string `yaml:"auth_password"`
	PrivProtocol  string `yaml:"priv_protocol"`
	PrivPassword  string `yaml:"priv_password"`
}

// =============================================================================
// Namespace Configuration
// =============================================================================

// NamespaceConfig holds namespace settings.
type NamespaceConfig struct {
	Description              string                  `yaml:"description"`
	Defaults                 *PollerDefaultsConfig   `yaml:"defaults"`
	SessionCleanupIntervalSec *int                   `yaml:"session_cleanup_interval_sec"`
	Targets                  map[string]*TargetConfig `yaml:"targets"`
}

// =============================================================================
// Target Configuration
// =============================================================================

// TargetConfig holds target settings.
type TargetConfig struct {
	Description string                  `yaml:"description"`
	Host        string                  `yaml:"host"`
	Port        uint16                  `yaml:"port"`
	Labels      map[string]string       `yaml:"labels"`
	Defaults    *PollerDefaultsConfig   `yaml:"defaults"`
	Pollers     map[string]*PollerConfig `yaml:"pollers"`
}

// =============================================================================
// Poller Configuration
// =============================================================================

// PollerConfig holds poller settings.
type PollerConfig struct {
	Description    string                 `yaml:"description"`
	Protocol       string                 `yaml:"protocol"`
	ProtocolConfig map[string]interface{} `yaml:"protocol_config"`
	PollingConfig  *PollingConfigYAML     `yaml:"polling"`
	AdminState     string                 `yaml:"admin_state"`
}

// PollingConfigYAML holds polling settings from YAML.
type PollingConfigYAML struct {
	IntervalMs *uint32 `yaml:"interval_ms"`
	TimeoutMs  *uint32 `yaml:"timeout_ms"`
	Retries    *uint32 `yaml:"retries"`
	BufferSize *uint32 `yaml:"buffer_size"`
}

// PollerDefaultsConfig holds inherited poller settings.
type PollerDefaultsConfig struct {
	// SNMP
	Community     string `yaml:"community"`
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

// =============================================================================
// Secret Configuration
// =============================================================================

// SecretConfig holds secret settings.
type SecretConfig struct {
	Type  string `yaml:"type"`
	Value string `yaml:"value"`
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

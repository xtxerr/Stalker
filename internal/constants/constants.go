// Package constants provides centralized domain-specific constants
// for the entire stalker application.
//
// This file consolidates all magic strings and values that were
// previously scattered across multiple packages.
package constants

// =============================================================================
// Admin State - User-controlled poller state
// =============================================================================

const (
	// AdminStateEnabled indicates the poller should be running
	AdminStateEnabled = "enabled"

	// AdminStateDisabled indicates the poller should not run
	AdminStateDisabled = "disabled"
)

// ValidAdminStates contains all valid admin state values
var ValidAdminStates = []string{AdminStateEnabled, AdminStateDisabled}

// IsValidAdminState checks if a state is valid
func IsValidAdminState(state string) bool {
	for _, s := range ValidAdminStates {
		if s == state {
			return true
		}
	}
	return false
}

// =============================================================================
// Oper State - System-determined operational state
// =============================================================================

const (
	// OperStateStopped indicates poller is not running
	OperStateStopped = "stopped"

	// OperStateRunning indicates poller is actively polling
	OperStateRunning = "running"

	// OperStatePaused indicates poller is temporarily paused
	OperStatePaused = "paused"

	// OperStateError indicates poller encountered a fatal error
	OperStateError = "error"
)

// ValidOperStates contains all valid oper state values
var ValidOperStates = []string{OperStateStopped, OperStateRunning, OperStatePaused, OperStateError}

// =============================================================================
// Health State - Target reachability state
// =============================================================================

const (
	// HealthStateUnknown indicates health has not been determined
	HealthStateUnknown = "unknown"

	// HealthStateUp indicates target is reachable
	HealthStateUp = "up"

	// HealthStateDown indicates target is unreachable
	HealthStateDown = "down"

	// HealthStateDegraded indicates partial failures
	HealthStateDegraded = "degraded"
)

// ValidHealthStates contains all valid health state values
var ValidHealthStates = []string{HealthStateUnknown, HealthStateUp, HealthStateDown, HealthStateDegraded}

// =============================================================================
// Tree Node Types - Virtual filesystem node types
// =============================================================================

const (
	// NodeTypeDirectory represents a directory node
	NodeTypeDirectory = "directory"

	// NodeTypeLink represents a symlink to target or poller
	NodeTypeLink = "link"

	// NodeTypeInfo represents an info/metadata node
	NodeTypeInfo = "info"
)

// =============================================================================
// Link Reference Prefixes
// =============================================================================

const (
	// LinkPrefixTarget is the prefix for target links
	LinkPrefixTarget = "target:"

	// LinkPrefixPoller is the prefix for poller links
	LinkPrefixPoller = "poller:"
)

// =============================================================================
// Protocol Types
// =============================================================================

const (
	// ProtocolSNMP indicates SNMP polling protocol
	ProtocolSNMP = "snmp"

	// ProtocolHTTP indicates HTTP polling protocol
	ProtocolHTTP = "http"

	// ProtocolICMP indicates ICMP ping protocol
	ProtocolICMP = "icmp"
)

// =============================================================================
// Health Thresholds
// =============================================================================

const (
	// ConsecutiveFailuresForDown is the number of failures before marking down
	ConsecutiveFailuresForDown = 3

	// ConsecutiveSuccessesForUp is the number of successes before marking up
	ConsecutiveSuccessesForUp = 2
)

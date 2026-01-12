// Package manager provides business logic and entity management for stalker.
//
// This file defines PollerState for runtime state tracking and StateManager
// for managing multiple poller states.
//
package manager

import (
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Admin and Operational State Constants
// =============================================================================

const (
	// AdminStateEnabled indicates the poller is administratively enabled.
	AdminStateEnabled = "enabled"

	// AdminStateDisabled indicates the poller is administratively disabled.
	AdminStateDisabled = "disabled"
)

const (
	// OperStateStopped indicates the poller is not running.
	OperStateStopped = "stopped"

	// OperStateStarting indicates the poller is starting up.
	OperStateStarting = "starting"

	// OperStateRunning indicates the poller is actively polling.
	OperStateRunning = "running"

	// OperStateStopping indicates the poller is shutting down.
	OperStateStopping = "stopping"
)

const (
	// HealthStateUnknown indicates health is not yet determined.
	HealthStateUnknown = "unknown"

	// HealthStateUp indicates the poller is healthy.
	HealthStateUp = "up"

	// HealthStateDegraded indicates the poller has intermittent failures.
	HealthStateDegraded = "degraded"

	// HealthStateDown indicates the poller is consistently failing.
	HealthStateDown = "down"
)

// =============================================================================
// PollerState
// =============================================================================

// PollerState holds runtime state for a poller.
//
// PollerState tracks the administrative state (enabled/disabled), operational
// state (stopped/running), and health state (up/down/degraded) of a poller.
// It also tracks failure counts and timestamps for diagnostics.
//
// PollerState is safe for concurrent use.
type PollerState struct {
	Namespace string
	Target    string
	Poller    string

	mu                  sync.RWMutex
	adminState          string
	operState           string
	healthState         string
	lastError           string
	consecutiveFailures int
	lastPollAt          *time.Time
	lastSuccessAt       *time.Time
	lastFailureAt       *time.Time

	dirty atomic.Bool
}

// NewPollerState creates a new poller state with default values.
func NewPollerState(namespace, target, poller string) *PollerState {
	return &PollerState{
		Namespace:   namespace,
		Target:      target,
		Poller:      poller,
		adminState:  AdminStateDisabled,
		operState:   OperStateStopped,
		healthState: HealthStateUnknown,
	}
}

// Key returns the unique key for this poller state.
func (s *PollerState) Key() string {
	return s.Namespace + "/" + s.Target + "/" + s.Poller
}

// GetState returns the current admin, operational, and health states.
func (s *PollerState) GetState() (admin, oper, health string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adminState, s.operState, s.healthState
}

// GetAdminState returns the administrative state.
func (s *PollerState) GetAdminState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adminState
}

// GetOperState returns the operational state.
func (s *PollerState) GetOperState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.operState
}

// GetHealthState returns the health state.
func (s *PollerState) GetHealthState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthState
}

// GetLastError returns the last error message.
func (s *PollerState) GetLastError() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

// GetConsecutiveFailures returns the count of consecutive failures.
func (s *PollerState) GetConsecutiveFailures() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.consecutiveFailures
}

// GetTimestamps returns the last poll, success, and failure timestamps.
func (s *PollerState) GetTimestamps() (lastPoll, lastSuccess, lastFailure *time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastPollAt, s.lastSuccessAt, s.lastFailureAt
}

// SetAdminState sets the administrative state.
func (s *PollerState) SetAdminState(state string) {
	s.mu.Lock()
	if s.adminState != state {
		s.adminState = state
		s.dirty.Store(true)
	}
	s.mu.Unlock()
}

// SetOperState sets the operational state.
func (s *PollerState) SetOperState(state string) {
	s.mu.Lock()
	if s.operState != state {
		s.operState = state
		s.dirty.Store(true)
	}
	s.mu.Unlock()
}

// SetHealthState sets the health state.
func (s *PollerState) SetHealthState(state string) {
	s.mu.Lock()
	if s.healthState != state {
		s.healthState = state
		s.dirty.Store(true)
	}
	s.mu.Unlock()
}

// CanRun returns true if the poller can be started.
func (s *PollerState) CanRun() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adminState == AdminStateEnabled && s.operState == OperStateStopped
}

// IsRunning returns true if the poller is currently running.
func (s *PollerState) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.operState == OperStateRunning
}

// Enable sets the admin state to enabled.
func (s *PollerState) Enable() {
	s.SetAdminState(AdminStateEnabled)
}

// Disable sets the admin state to disabled.
func (s *PollerState) Disable() {
	s.SetAdminState(AdminStateDisabled)
}

// Start sets the operational state to running.
func (s *PollerState) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.adminState != AdminStateEnabled {
		return ErrPollerDisabled
	}
	if s.operState == OperStateRunning {
		return ErrPollerRunning
	}

	s.operState = OperStateRunning
	s.dirty.Store(true)
	return nil
}

// Stop sets the operational state to stopped.
func (s *PollerState) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.operState != OperStateStopped {
		s.operState = OperStateStopped
		s.dirty.Store(true)
	}
}

// RecordSuccess records a successful poll.
func (s *PollerState) RecordSuccess() {
	now := time.Now()
	s.mu.Lock()
	s.lastPollAt = &now
	s.lastSuccessAt = &now
	s.consecutiveFailures = 0
	s.lastError = ""
	if s.healthState != HealthStateUp {
		s.healthState = HealthStateUp
	}
	s.dirty.Store(true)
	s.mu.Unlock()
}

// RecordFailure records a failed poll.
func (s *PollerState) RecordFailure(errMsg string) {
	now := time.Now()
	s.mu.Lock()
	s.lastPollAt = &now
	s.lastFailureAt = &now
	s.consecutiveFailures++
	s.lastError = errMsg

	// Update health based on consecutive failures
	if s.consecutiveFailures >= 3 {
		s.healthState = HealthStateDown
	} else {
		s.healthState = HealthStateDegraded
	}
	s.dirty.Store(true)
	s.mu.Unlock()
}

// RecordPollResult records a poll result (convenience method).
func (s *PollerState) RecordPollResult(success bool, errMsg string) {
	if success {
		s.RecordSuccess()
	} else {
		s.RecordFailure(errMsg)
	}
}

// IsDirty returns true if state has changed since last flush.
func (s *PollerState) IsDirty() bool {
	return s.dirty.Load()
}

// ClearDirty clears the dirty flag.
func (s *PollerState) ClearDirty() {
	s.dirty.Store(false)
}

// =============================================================================
// StateManager
// =============================================================================

// StateManager manages poller states in memory.
//
// StateManager is safe for concurrent use.
type StateManager struct {
	mu     sync.RWMutex
	states map[string]*PollerState // key: namespace/target/poller
}

// NewStateManager creates a new state manager.
func NewStateManager() *StateManager {
	return &StateManager{
		states: make(map[string]*PollerState),
	}
}

// stateKey returns the map key for a state.
func stateKey(namespace, target, poller string) string {
	return namespace + "/" + target + "/" + poller
}

// Get returns the state for a poller, creating it if needed.
func (m *StateManager) Get(namespace, target, poller string) *PollerState {
	key := stateKey(namespace, target, poller)

	// Fast path: read lock
	m.mu.RLock()
	state, ok := m.states[key]
	m.mu.RUnlock()

	if ok {
		return state
	}

	// Slow path: write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if state, ok := m.states[key]; ok {
		return state
	}

	state = NewPollerState(namespace, target, poller)
	m.states[key] = state
	return state
}

// GetIfExists returns the state for a poller if it exists.
func (m *StateManager) GetIfExists(namespace, target, poller string) *PollerState {
	key := stateKey(namespace, target, poller)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.states[key]
}

// Remove removes a state.
func (m *StateManager) Remove(namespace, target, poller string) {
	key := stateKey(namespace, target, poller)

	m.mu.Lock()
	delete(m.states, key)
	m.mu.Unlock()
}

// RemoveForTarget removes all states for a target.
func (m *StateManager) RemoveForTarget(namespace, target string) {
	prefix := namespace + "/" + target + "/"

	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.states {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			delete(m.states, key)
		}
	}
}

// GetDirty returns all dirty states.
//
// The returned slice contains references to the actual state objects.
// Callers should not modify the states without proper synchronization.
func (m *StateManager) GetDirty() []*PollerState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Pre-allocate with estimated capacity (10% dirty)
	estimatedSize := len(m.states) / 10
	if estimatedSize < 10 {
		estimatedSize = 10
	}

	dirty := make([]*PollerState, 0, estimatedSize)
	for _, state := range m.states {
		if state.IsDirty() {
			dirty = append(dirty, state)
		}
	}
	return dirty
}

// GetAll returns all states.
func (m *StateManager) GetAll() []*PollerState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*PollerState, 0, len(m.states))
	for _, state := range m.states {
		result = append(result, state)
	}
	return result
}

// Count returns the number of states.
func (m *StateManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}

// CountByOperState returns counts grouped by operational state.
func (m *StateManager) CountByOperState() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[string]int)
	for _, state := range m.states {
		_, oper, _ := state.GetState()
		counts[oper]++
	}
	return counts
}

// CountByHealthState returns counts grouped by health state.
func (m *StateManager) CountByHealthState() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[string]int)
	for _, state := range m.states {
		_, _, health := state.GetState()
		counts[health]++
	}
	return counts
}

// CountByHealth returns health counts as individual values.
func (m *StateManager) CountByHealth() (up, degraded, down, unknown int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, state := range m.states {
		_, _, health := state.GetState()
		switch health {
		case HealthStateUp:
			up++
		case HealthStateDegraded:
			degraded++
		case HealthStateDown:
			down++
		default:
			unknown++
		}
	}
	return
}

// =============================================================================
// Errors
// =============================================================================

// ErrPollerDisabled is returned when trying to start a disabled poller.
var ErrPollerDisabled = &StateError{msg: "poller is disabled"}

// ErrPollerRunning is returned when trying to start an already running poller.
var ErrPollerRunning = &StateError{msg: "poller is already running"}

// StateError represents a state transition error.
type StateError struct {
	msg string
}

// Error implements the error interface.
func (e *StateError) Error() string {
	return e.msg
}

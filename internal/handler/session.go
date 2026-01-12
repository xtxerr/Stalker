// Package handler provides request handling for the stalker protocol.
//
// This package contains session management, request dispatching, and
// specialized handlers for different entity types (namespaces, targets, pollers).
//
// DESIGN DECISION: Session Resumption Removed
// --------------------------------------------
// This implementation does NOT support session resumption after disconnect.
// Each connection creates a new session. Clients are responsible for:
//   - Re-authenticating after reconnect
//   - Re-binding to their namespace
//   - Re-subscribing to pollers
//
// Rationale:
//   - Simpler implementation with fewer edge cases
//   - No security concerns around session ID prediction/hijacking
//   - Client-side re-subscribe is cheap (single API call)
//   - Server buffers samples anyway, so no data loss during brief disconnects
//   - Similar to how gRPC streaming, Prometheus, and most monitoring systems work
package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/config"
	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/wire"
)

var log = logging.Component("session")

// =============================================================================
// Session
// =============================================================================

// Session represents a client session with the server.
//
// A session is created when a client connects and authenticates.
// It is destroyed when the client disconnects - there is no session resumption.
// Clients must re-authenticate and re-subscribe after reconnecting.
//
// Session is safe for concurrent use.
type Session struct {
	// Immutable fields (no lock needed)
	ID        string
	TokenID   string
	CreatedAt time.Time

	// Connection - protected by connMu
	connMu sync.RWMutex
	Conn   net.Conn
	Wire   *wire.Conn

	// Session state - protected by mu
	mu        sync.RWMutex
	namespace string

	// Send channel - protected by sendMu
	sendMu sync.RWMutex
	sendCh chan []byte

	// Subscriptions - protected by mu
	subscriptions map[string]struct{}

	// State management
	closed    atomic.Bool
	closeOnce sync.Once

	// Callbacks and references
	onClose func(sessionID string)
	manager *SessionManager

	// Configuration
	sendBufferSize int
	sendTimeoutMs  int
}

// SessionConfig holds session configuration options.
type SessionConfig struct {
	SendBufferSize int
	SendTimeoutMs  int
}

// NewSession creates a new session with default configuration.
func NewSession(id, tokenID string, conn net.Conn, w *wire.Conn) *Session {
	return NewSessionWithConfig(id, tokenID, conn, w, nil)
}

// NewSessionWithConfig creates a new session with custom configuration.
func NewSessionWithConfig(id, tokenID string, conn net.Conn, w *wire.Conn, cfg *SessionConfig) *Session {
	bufferSize := config.DefaultSessionSendBufferSize
	timeoutMs := config.DefaultSessionSendTimeoutMs

	if cfg != nil {
		if cfg.SendBufferSize > 0 {
			bufferSize = cfg.SendBufferSize
		}
		if cfg.SendTimeoutMs > 0 {
			timeoutMs = cfg.SendTimeoutMs
		}
	}

	return &Session{
		ID:             id,
		TokenID:        tokenID,
		CreatedAt:      time.Now(),
		Conn:           conn,
		Wire:           w,
		sendCh:         make(chan []byte, bufferSize),
		subscriptions:  make(map[string]struct{}),
		sendBufferSize: bufferSize,
		sendTimeoutMs:  timeoutMs,
	}
}

// SetManager sets the session manager reference.
func (s *Session) SetManager(sm *SessionManager) {
	s.mu.Lock()
	s.manager = sm
	s.mu.Unlock()
}

// SetOnClose sets the close callback.
func (s *Session) SetOnClose(fn func(sessionID string)) {
	s.mu.Lock()
	s.onClose = fn
	s.mu.Unlock()
}

// =============================================================================
// Namespace Operations
// =============================================================================

// BindNamespace binds the session to a namespace.
// A session can only be bound to one namespace at a time.
func (s *Session) BindNamespace(ns string) {
	s.mu.Lock()
	s.namespace = ns
	s.mu.Unlock()
}

// GetNamespace returns the bound namespace.
func (s *Session) GetNamespace() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespace
}

// IsBound returns true if session is bound to a namespace.
func (s *Session) IsBound() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespace != ""
}

// =============================================================================
// Subscription Operations
// =============================================================================

// Subscribe adds a subscription to a poller key.
func (s *Session) Subscribe(key string) {
	s.mu.Lock()
	s.subscriptions[key] = struct{}{}
	s.mu.Unlock()
}

// Unsubscribe removes a subscription.
func (s *Session) Unsubscribe(key string) {
	s.mu.Lock()
	delete(s.subscriptions, key)
	s.mu.Unlock()
}

// GetSubscriptions returns all subscriptions.
func (s *Session) GetSubscriptions() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs := make([]string, 0, len(s.subscriptions))
	for k := range s.subscriptions {
		subs = append(subs, k)
	}
	return subs
}

// HasSubscription checks if subscribed to a key.
func (s *Session) HasSubscription(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.subscriptions[key]
	return ok
}

// ClearSubscriptions removes all subscriptions.
func (s *Session) ClearSubscriptions() {
	s.mu.Lock()
	s.subscriptions = make(map[string]struct{})
	s.mu.Unlock()
}

// SubscriptionCount returns the number of active subscriptions.
func (s *Session) SubscriptionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscriptions)
}

// =============================================================================
// Send Operations
// =============================================================================

// Send sends data to the client.
// Returns false if the session is closed or the send buffer is full after timeout.
func (s *Session) Send(data []byte) bool {
	if s.closed.Load() {
		return false
	}

	s.sendMu.RLock()
	ch := s.sendCh
	s.sendMu.RUnlock()

	if ch == nil {
		return false
	}

	// Try non-blocking send first
	select {
	case ch <- data:
		return true
	default:
		// Buffer full - try with timeout
		timeout := time.Duration(s.sendTimeoutMs) * time.Millisecond
		select {
		case ch <- data:
			return true
		case <-time.After(timeout):
			log.Warn("send buffer full, dropping message",
				"session_id", s.ID,
				"timeout_ms", s.sendTimeoutMs)
			return false
		}
	}
}

// SendChan returns the send channel for the writer goroutine.
func (s *Session) SendChan() <-chan []byte {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	return s.sendCh
}

// =============================================================================
// Close
// =============================================================================

// Close closes the session permanently.
// This is idempotent - calling it multiple times has no additional effect.
func (s *Session) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		s.closed.Store(true)

		// Get callbacks under lock
		s.mu.Lock()
		onClose := s.onClose
		namespace := s.namespace
		manager := s.manager
		sessionID := s.ID
		s.mu.Unlock()

		// Clean up subscriptions from index
		if manager != nil && namespace != "" {
			manager.UnsubscribeAllForSession(sessionID, namespace)
		}

		// Close send channel
		s.sendMu.Lock()
		if s.sendCh != nil {
			close(s.sendCh)
			s.sendCh = nil
		}
		s.sendMu.Unlock()

		// Close connection
		s.connMu.Lock()
		if s.Conn != nil {
			closeErr = s.Conn.Close()
			s.Conn = nil
			s.Wire = nil
		}
		s.connMu.Unlock()

		// Invoke callback
		if onClose != nil {
			onClose(sessionID)
		}

		log.Debug("session closed", "session_id", sessionID)
	})

	return closeErr
}

// IsClosed returns true if the session is closed.
func (s *Session) IsClosed() bool {
	return s.closed.Load()
}

// =============================================================================
// Session Manager
// =============================================================================

// TokenConfig holds token configuration.
type TokenConfig struct {
	ID         string
	Token      string
	Namespaces []string // Allowed namespaces (empty = all)
}

// SessionManagerConfig holds session manager configuration.
type SessionManagerConfig struct {
	AuthTimeout     time.Duration
	CleanupInterval time.Duration
	Tokens          []TokenConfig
	OnSessionClosed func(session *Session)
}

// SessionManager manages client sessions.
//
// SessionManager is safe for concurrent use.
type SessionManager struct {
	mu sync.RWMutex

	sessions map[string]*Session     // sessionID -> session
	tokens   map[string]*TokenConfig // tokenID -> config

	// Reverse index: namespace/key -> set of session IDs
	// Enables O(1) subscriber lookup
	subscriptionIndex map[string]map[string]struct{}

	// Configuration
	authTimeout           time.Duration
	globalCleanupInterval time.Duration

	// Callback when session is closed
	onSessionClosed func(session *Session)

	// Background cleanup
	cleanupCtx    context.Context
	cleanupCancel context.CancelFunc
	cleanupWg     sync.WaitGroup
}

// NewSessionManager creates a new session manager.
func NewSessionManager(cfg *SessionManagerConfig) *SessionManager {
	if cfg == nil {
		cfg = &SessionManagerConfig{}
	}

	if cfg.AuthTimeout == 0 {
		cfg.AuthTimeout = time.Duration(config.DefaultAuthTimeoutSec) * time.Second
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = time.Duration(config.DefaultSessionCleanupIntervalSec) * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	sm := &SessionManager{
		sessions:              make(map[string]*Session),
		tokens:                make(map[string]*TokenConfig),
		subscriptionIndex:     make(map[string]map[string]struct{}),
		authTimeout:           cfg.AuthTimeout,
		globalCleanupInterval: cfg.CleanupInterval,
		onSessionClosed:       cfg.OnSessionClosed,
		cleanupCtx:            ctx,
		cleanupCancel:         cancel,
	}

	// Register tokens
	for _, t := range cfg.Tokens {
		tc := t
		sm.tokens[tc.ID] = &tc
	}

	return sm
}

// Start starts the background cleanup goroutine.
func (sm *SessionManager) Start() {
	sm.cleanupWg.Add(1)
	go sm.cleanupLoop()
	log.Info("session manager started")
}

// Stop stops the session manager and cleans up resources.
func (sm *SessionManager) Stop() {
	sm.cleanupCancel()
	sm.cleanupWg.Wait()

	// Close all remaining sessions
	sm.mu.Lock()
	for _, session := range sm.sessions {
		session.Close()
	}
	sm.sessions = make(map[string]*Session)
	sm.mu.Unlock()

	log.Info("session manager stopped")
}

// AuthTimeout returns the authentication timeout.
func (sm *SessionManager) AuthTimeout() time.Duration {
	return sm.authTimeout
}

func (sm *SessionManager) cleanupLoop() {
	defer sm.cleanupWg.Done()

	ticker := time.NewTicker(sm.globalCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.cleanupClosedSessions()
		case <-sm.cleanupCtx.Done():
			return
		}
	}
}

// cleanupClosedSessions removes closed sessions from the map.
func (sm *SessionManager) cleanupClosedSessions() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var toRemove []string
	for id, session := range sm.sessions {
		if session.IsClosed() {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		delete(sm.sessions, id)
	}

	if len(toRemove) > 0 {
		log.Debug("cleaned up closed sessions", "count", len(toRemove))
	}
}

// =============================================================================
// Token Operations
// =============================================================================

// RegisterToken adds or updates a token configuration.
func (sm *SessionManager) RegisterToken(cfg *TokenConfig) {
	sm.mu.Lock()
	sm.tokens[cfg.ID] = cfg
	sm.mu.Unlock()
}

// ValidateToken validates a token and returns its config.
func (sm *SessionManager) ValidateToken(token string) (*TokenConfig, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, cfg := range sm.tokens {
		if cfg.Token == token {
			return cfg, true
		}
	}
	return nil, false
}

// CanAccessNamespace checks if a token can access a namespace.
func (sm *SessionManager) CanAccessNamespace(tokenID, namespace string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	cfg, ok := sm.tokens[tokenID]
	if !ok {
		return false
	}

	// Empty list means access to all namespaces (admin)
	if len(cfg.Namespaces) == 0 {
		return true
	}

	for _, ns := range cfg.Namespaces {
		if ns == namespace {
			return true
		}
	}
	return false
}

// =============================================================================
// Session Lifecycle
// =============================================================================

// CreateSession creates a new session.
func (sm *SessionManager) CreateSession(tokenID string, conn net.Conn, w *wire.Conn) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := generateSessionID()
	session := NewSession(id, tokenID, conn, w)
	session.SetManager(sm)
	session.SetOnClose(func(sid string) {
		if sm.onSessionClosed != nil {
			sm.onSessionClosed(session)
		}
	})

	sm.sessions[id] = session

	log.Info("session created", "session_id", id, "token_id", tokenID)
	return session
}

// GetSession returns a session by ID.
func (sm *SessionManager) GetSession(id string) *Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[id]
}

// RemoveSession removes a session immediately.
func (sm *SessionManager) RemoveSession(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, ok := sm.sessions[id]
	if !ok {
		return
	}

	// Close the session
	session.Close()

	// Remove from map
	delete(sm.sessions, id)

	// Clean up subscription index
	namespace := session.GetNamespace()
	for key := range session.subscriptions {
		fullKey := namespace + "/" + key
		if sessions, ok := sm.subscriptionIndex[fullKey]; ok {
			delete(sessions, id)
			if len(sessions) == 0 {
				delete(sm.subscriptionIndex, fullKey)
			}
		}
	}

	log.Info("session removed", "session_id", id)
}

// =============================================================================
// Subscription Index Operations
// =============================================================================

// AddSubscription adds a subscription to the index.
func (sm *SessionManager) AddSubscription(sessionID, namespace, key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	fullKey := namespace + "/" + key
	if sm.subscriptionIndex[fullKey] == nil {
		sm.subscriptionIndex[fullKey] = make(map[string]struct{})
	}
	sm.subscriptionIndex[fullKey][sessionID] = struct{}{}
}

// RemoveSubscription removes a subscription from the index.
func (sm *SessionManager) RemoveSubscription(sessionID, namespace, key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	fullKey := namespace + "/" + key
	if sessions, ok := sm.subscriptionIndex[fullKey]; ok {
		delete(sessions, sessionID)
		if len(sessions) == 0 {
			delete(sm.subscriptionIndex, fullKey)
		}
	}
}

// GetSubscribers returns all session IDs subscribed to a key.
func (sm *SessionManager) GetSubscribers(namespace, key string) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	fullKey := namespace + "/" + key
	sessions := sm.subscriptionIndex[fullKey]
	if sessions == nil {
		return nil
	}

	result := make([]string, 0, len(sessions))
	for sid := range sessions {
		result = append(result, sid)
	}
	return result
}

// UnsubscribeAllForSession removes all subscriptions for a session.
func (sm *SessionManager) UnsubscribeAllForSession(sessionID, namespace string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for fullKey, sessions := range sm.subscriptionIndex {
		delete(sessions, sessionID)
		if len(sessions) == 0 {
			delete(sm.subscriptionIndex, fullKey)
		}
	}
}

// =============================================================================
// Statistics
// =============================================================================

// Count returns the total number of sessions.
func (sm *SessionManager) Count() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

// CountActive returns the number of active (not closed) sessions.
func (sm *SessionManager) CountActive() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	count := 0
	for _, s := range sm.sessions {
		if !s.IsClosed() {
			count++
		}
	}
	return count
}

// =============================================================================
// Helpers
// =============================================================================

// generateSessionID generates a cryptographically secure session ID.
// Uses 128 bits of randomness (16 bytes) encoded as hex (32 characters).
func generateSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// This should never happen with crypto/rand
		panic("failed to generate session ID: " + err.Error())
	}
	return hex.EncodeToString(b)
}

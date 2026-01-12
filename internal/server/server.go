// Package server provides the main SNMP proxy server implementation.
//
// The server handles client connections, authentication, request dispatching,
// and coordinates the scheduler for poll execution.
package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/handler"
	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/manager"
	"github.com/xtxerr/stalker/internal/scheduler"
	"github.com/xtxerr/stalker/internal/wire"
)

var log = logging.Component("server")

// =============================================================================
// Rate Limiter for Failed Authentication Attempts
// =============================================================================

// RateLimiter implements rate limiting for FAILED authentication attempts.
//
// FAILED auth attempts per IP address per time window. Successful
// authentications are NOT counted and reset the failure counter.
//
// Flow:
//  1. Client connects
//  2. Check IsBlocked() - if true, reject immediately
//  3. Attempt authentication
//  4. If auth FAILS: call RecordFailure()
//  5. If auth SUCCEEDS: call Reset() to clear failure count
type RateLimiter struct {
	mu       sync.RWMutex
	failures map[string]*rateLimitEntry
	limit    int           // max failures before blocking
	window   time.Duration // time window for counting failures
}

type rateLimitEntry struct {
	count     int       // number of failed attempts
	resetTime time.Time // when this entry expires
}

// NewRateLimiter creates a new rate limiter.
//
// Parameters:
//   - limit: maximum failed attempts before blocking
//   - window: time window for counting failures (e.g., 1 minute)
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		failures: make(map[string]*rateLimitEntry),
		limit:    limit,
		window:   window,
	}

	// Start cleanup goroutine
	go rl.cleanupLoop()

	return rl
}

// IsBlocked returns true if the IP has exceeded the failure limit.
// This should be called BEFORE attempting authentication.
func (rl *RateLimiter) IsBlocked(ip string) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	entry, ok := rl.failures[ip]
	if !ok {
		return false
	}

	// Check if window has expired
	if time.Now().After(entry.resetTime) {
		return false
	}

	return entry.count >= rl.limit
}

// RecordFailure records a failed authentication attempt.
// This should be called AFTER a failed authentication.
func (rl *RateLimiter) RecordFailure(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	entry, ok := rl.failures[ip]

	if !ok || now.After(entry.resetTime) {
		// New entry or window expired - start fresh
		rl.failures[ip] = &rateLimitEntry{
			count:     1,
			resetTime: now.Add(rl.window),
		}
		return
	}

	// Within window - increment counter
	entry.count++
}

// Reset clears the failure count for an IP (after successful auth).
func (rl *RateLimiter) Reset(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.failures, ip)
}

// GetFailureCount returns the current failure count for an IP (for testing/monitoring).
func (rl *RateLimiter) GetFailureCount(ip string) int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	entry, ok := rl.failures[ip]
	if !ok {
		return 0
	}

	if time.Now().After(entry.resetTime) {
		return 0
	}

	return entry.count
}

func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for ip, entry := range rl.failures {
		if now.After(entry.resetTime) {
			delete(rl.failures, ip)
		}
	}
}

// =============================================================================
// Server Configuration
// =============================================================================

// Config holds server configuration.
type Config struct {
	// Manager is the entity manager (required).
	Manager *manager.Manager

	// Listen is the address to listen on (e.g., "0.0.0.0:9161").
	Listen string

	// TLS configuration (optional).
	TLSCertFile string
	TLSKeyFile  string

	// Authentication tokens.
	Tokens []handler.TokenConfig

	// Session settings.
	AuthTimeoutSec     int
	ReconnectWindowSec int

	// Scheduler settings.
	PollerWorkers   int
	PollerQueueSize int
}

// =============================================================================
// Server
// =============================================================================

// Server is the main SNMP proxy server.
type Server struct {
	cfg       *Config
	mgr       *manager.Manager
	sessions  *handler.SessionManager
	scheduler *scheduler.Scheduler
	listener  net.Listener

	authRateLimiter *RateLimiter

	shutdown chan struct{}
	wg       sync.WaitGroup
}

// New creates a new server.
func New(cfg *Config) *Server {
	// Apply defaults
	if cfg.AuthTimeoutSec == 0 {
		cfg.AuthTimeoutSec = config.DefaultAuthTimeoutSec
	}
	if cfg.PollerWorkers == 0 {
		cfg.PollerWorkers = config.DefaultPollerWorkers
	}
	if cfg.PollerQueueSize == 0 {
		cfg.PollerQueueSize = config.DefaultPollerQueueSize
	}

	// Create session manager
	sessions := handler.NewSessionManager(&handler.SessionManagerConfig{
		AuthTimeout:     time.Duration(cfg.AuthTimeoutSec) * time.Second,
		CleanupInterval: time.Duration(config.DefaultSessionCleanupIntervalSec) * time.Second,
		Tokens:          cfg.Tokens,
	})

	// Create scheduler
	sched := scheduler.New(&scheduler.Config{
		Workers:   cfg.PollerWorkers,
		QueueSize: cfg.PollerQueueSize,
	})

	return &Server{
		cfg:       cfg,
		mgr:       cfg.Manager,
		sessions:  sessions,
		scheduler: sched,
		shutdown:  make(chan struct{}),
		authRateLimiter: NewRateLimiter(
			config.DefaultAuthRateLimitPerMinute,
			time.Minute,
		),
	}
}

// Run starts the server and blocks until shutdown.
func (s *Server) Run() error {
	// Load existing data
	log.Info("loading data from store")
	if err := s.mgr.Load(); err != nil {
		return fmt.Errorf("load data: %w", err)
	}

	// Start sync manager
	s.mgr.Start()

	// Start scheduler
	s.scheduler.Start()

	// Start result processor
	s.wg.Add(1)
	go s.processResults()

	// Start session cleanup
	s.wg.Add(1)
	go s.cleanupLoop()

	// Schedule enabled pollers
	started := s.scheduleEnabledPollers()
	log.Info("scheduled enabled pollers", "count", started)

	// Start listener
	var ln net.Listener
	var err error

	if s.cfg.TLSCertFile != "" && s.cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		ln, err = tls.Listen("tcp", s.cfg.Listen, tlsCfg)
		if err != nil {
			return fmt.Errorf("TLS listen: %w", err)
		}
		log.Info("listening with TLS", "address", s.cfg.Listen)
	} else {
		ln, err = net.Listen("tcp", s.cfg.Listen)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
		log.Info("listening without TLS", "address", s.cfg.Listen)
	}

	s.listener = ln

	// Accept connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
				log.Error("accept error", "error", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown() {
	log.Info("shutting down")
	close(s.shutdown)

	if s.listener != nil {
		s.listener.Close()
	}

	s.scheduler.Stop()
	s.wg.Wait()
	s.mgr.Stop()

	log.Info("shutdown complete")
}

// =============================================================================
// Connection Handling
// =============================================================================

// handleConn handles a new connection.
func (s *Server) handleConn(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	remoteIP := extractIP(remote)

	log.Info("connection from", "remote", remote)

	if s.authRateLimiter.IsBlocked(remoteIP) {
		log.Warn("blocked due to too many failed auth attempts", "remote", remote)
		conn.Close()
		return
	}

	w := wire.NewConn(conn)

	// Auth with timeout
	conn.SetDeadline(time.Now().Add(s.sessions.AuthTimeout()))

	env, err := w.Read()
	if err != nil {
		log.Error("auth read error", "remote", remote, "error", err)
		conn.Close()
		return
	}

	auth := env.GetAuth()
	if auth == nil {
		s.authRateLimiter.RecordFailure(remoteIP)
		w.Write(wire.NewError(env.Id, wire.ErrNotAuthenticated, "first message must be auth"))
		conn.Close()
		return
	}

	tokenCfg, ok := s.sessions.ValidateToken(auth.Token)
	if !ok {
		s.authRateLimiter.RecordFailure(remoteIP)
		s.sendAuthResponse(w, env.Id, false, "", "invalid token")
		conn.Close()
		log.Warn("auth failed", "remote", remote,
			"failure_count", s.authRateLimiter.GetFailureCount(remoteIP))
		return
	}

	s.authRateLimiter.Reset(remoteIP)

	conn.SetDeadline(time.Time{}) // Clear deadline

	// Create new session (no session resumption - see session.go for rationale)
	session := s.sessions.CreateSession(tokenCfg.ID, conn, w)
	log.Info("new session", "session_id", session.ID, "remote", remote, "token_id", tokenCfg.ID)

	// Send auth response
	if err := s.sendAuthResponse(w, env.Id, true, session.ID, ""); err != nil {
		log.Error("failed to send auth response", "remote", remote, "error", err)
		conn.Close()
		return
	}

	// Start writer goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		for data := range session.SendChan() {
			if _, err := conn.Write(data); err != nil {
				// Close() is idempotent, so it's safe to call from both goroutines.
				log.Debug("write failed, closing session",
					"session_id", session.ID,
					"error", err)
				session.Close()
				return
			}
		}
	}()

	// Read loop
	for {
		env, err := w.Read()
		if err != nil {
			break
		}
		s.handleMessage(session, env)
	}

	// Disconnect - close session and wait for writer goroutine
	session.Close()
	log.Info("session disconnected", "session_id", session.ID)
	<-done
}

// extractIP extracts the IP address from a remote address string.
func extractIP(remote string) string {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return remote
	}
	return host
}

// =============================================================================
// Message Handling (placeholder implementations)
// =============================================================================

func (s *Server) sendAuthResponse(w *wire.Conn, id uint64, ok bool, sessionID, errMsg string) error {
	// Placeholder - actual implementation would use protobuf
	return nil
}

func (s *Server) handleMessage(session *handler.Session, env interface{}) {
	// Placeholder - actual implementation would dispatch to handlers
}

func (s *Server) processResults() {
	defer s.wg.Done()

	for {
		select {
		case result, ok := <-s.scheduler.Results():
			if !ok {
				return
			}
			s.handlePollResult(result)
		case <-s.shutdown:
			return
		}
	}
}

func (s *Server) handlePollResult(result scheduler.PollResult) {
	// Placeholder - actual implementation would record result
}

func (s *Server) cleanupLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Session cleanup is handled by SessionManager
		case <-s.shutdown:
			return
		}
	}
}

func (s *Server) scheduleEnabledPollers() int {
	// Placeholder - actual implementation would schedule pollers
	return 0
}

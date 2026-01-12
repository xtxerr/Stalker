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

	"github.com/xtxerr/stalker/internal/config"
	"github.com/xtxerr/stalker/internal/handler"
	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/manager"
	"github.com/xtxerr/stalker/internal/scheduler"
	"github.com/xtxerr/stalker/internal/wire"
)

var log = logging.Component("server")

// =============================================================================
// Rate Limiter for Authentication
// =============================================================================

// RateLimiter implements a simple token bucket rate limiter per IP address.
//
// FIX #13: This prevents brute-force attacks on authentication by limiting
// the number of auth attempts per IP address per time window.
type RateLimiter struct {
	mu       sync.RWMutex
	attempts map[string]*rateLimitEntry
	limit    int
	window   time.Duration
}

type rateLimitEntry struct {
	count     int
	resetTime time.Time
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		attempts: make(map[string]*rateLimitEntry),
		limit:    limit,
		window:   window,
	}

	// Start cleanup goroutine
	go rl.cleanupLoop()

	return rl
}

// Allow returns true if the request from the given IP should be allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	entry, ok := rl.attempts[ip]

	if !ok || now.After(entry.resetTime) {
		// New entry or window expired
		rl.attempts[ip] = &rateLimitEntry{
			count:     1,
			resetTime: now.Add(rl.window),
		}
		return true
	}

	// Within window
	if entry.count >= rl.limit {
		return false
	}

	entry.count++
	return true
}

// Reset clears the rate limit for a specific IP (e.g., after successful auth).
func (rl *RateLimiter) Reset(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, ip)
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
	for ip, entry := range rl.attempts {
		if now.After(entry.resetTime) {
			delete(rl.attempts, ip)
		}
	}
}

// =============================================================================
// Server Configuration
// =============================================================================

// Config holds server configuration.
type Config struct {
	Listen      string
	TLSCertFile string
	TLSKeyFile  string
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

	// FIX #13: Rate limiter for authentication attempts
	authRateLimiter *RateLimiter

	shutdown chan struct{}
	wg       sync.WaitGroup
}

// New creates a new server.
func New(cfg *Config, mgr *manager.Manager, sessions *handler.SessionManager, sched *scheduler.Scheduler) *Server {
	return &Server{
		cfg:       cfg,
		mgr:       mgr,
		sessions:  sessions,
		scheduler: sched,
		shutdown:  make(chan struct{}),
		// FIX #13: Initialize rate limiter
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

	// FIX #13: Check rate limit before processing
	if !s.authRateLimiter.Allow(remoteIP) {
		log.Warn("rate limited", "remote", remote)
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
		w.Write(wire.NewError(env.Id, wire.ErrNotAuthenticated, "first message must be auth"))
		conn.Close()
		return
	}

	tokenCfg, ok := s.sessions.ValidateToken(auth.Token)
	if !ok {
		s.sendAuthResponse(w, env.Id, false, "", "invalid token")
		conn.Close()
		log.Warn("auth failed", "remote", remote)
		return
	}

	// FIX #13: Reset rate limit on successful auth
	s.authRateLimiter.Reset(remoteIP)

	conn.SetDeadline(time.Time{}) // Clear deadline

	// Try restore or create session
	session := s.sessions.TryRestore(tokenCfg.ID, conn, w)
	if session == nil {
		session = s.sessions.CreateSession(tokenCfg.ID, conn, w)
		log.Info("new session", "session_id", session.ID, "remote", remote, "token_id", tokenCfg.ID)
	} else {
		log.Info("restored session", "session_id", session.ID, "remote", remote)
	}

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
				// FIX #9: Mark session as lost when write fails
				// This was previously not calling MarkLost, causing the
				// session's send channel to remain open and leak resources.
				session.MarkLost()
				log.Debug("write failed, marking session lost",
					"session_id", session.ID,
					"error", err)
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

	// Disconnect
	session.MarkLost()
	log.Info("session lost", "session_id", session.ID)
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

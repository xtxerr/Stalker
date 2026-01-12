// Package client provides a client for connecting to the stalker server.
//
// FIXES APPLIED:
// - FIX #4: Uses ResettableOnce instead of sync.Once for safe reconnection
// - FIX #11: Explicit state machine with validated transitions
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/xtxerr/stalker/internal/proto"
	stalkerSync "github.com/xtxerr/stalker/internal/sync"
	"github.com/xtxerr/stalker/internal/wire"
)

// =============================================================================
// FIX #11: Explicit State Machine Definition
// =============================================================================

// ClientState represents the connection state of a client.
type ClientState int32

const (
	StateDisconnected ClientState = iota
	StateConnecting
	StateConnected
	StateClosing
	StateClosed
)

// String returns the human-readable name of the state.
func (s ClientState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// stateTransition represents a state transition.
type stateTransition struct {
	from ClientState
	to   ClientState
}

// validTransitions defines all allowed state transitions.
var validTransitions = map[stateTransition]bool{
	// From Disconnected
	{StateDisconnected, StateConnecting}: true,
	{StateDisconnected, StateClosed}:     true,

	// From Connecting
	{StateConnecting, StateConnected}:    true,
	{StateConnecting, StateDisconnected}: true,

	// From Connected
	{StateConnected, StateDisconnected}: true,
	{StateConnected, StateClosing}:      true,

	// From Closing
	{StateClosing, StateClosed}: true,
}

// =============================================================================
// Errors
// =============================================================================

var (
	ErrClientClosed      = errors.New("client is closed")
	ErrClientClosing     = errors.New("client is closing")
	ErrNotConnected      = errors.New("not connected")
	ErrAlreadyConnected  = errors.New("already connected")
	ErrAuthFailed        = errors.New("authentication failed")
	ErrTimeout           = errors.New("request timeout")
	ErrInvalidTransition = errors.New("invalid state transition")
)

// =============================================================================
// Client
// =============================================================================

// Client connects to an SNMP proxy server.
type Client struct {
	addr      string
	token     string
	tlsConfig *tls.Config

	// Connection - protected by mu
	mu        sync.Mutex
	conn      net.Conn
	wire      *wire.Conn
	sessionID string

	// FIX #11: State management with explicit transitions
	state atomic.Int32

	// FIX #4: Resettable once for safe reconnection
	closeOnce stalkerSync.ResettableOnce

	// Pending requests
	pendingMu sync.RWMutex
	pending   map[uint64]chan *pb.Envelope
	requestID atomic.Uint64

	// Callbacks
	onSample     func(*pb.Sample)
	onDisconnect func(error)

	// Channels
	shutdown chan struct{}
}

// Config holds client configuration.
type Config struct {
	Addr           string
	Token          string
	TLS            bool
	TLSSkipVerify  bool
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
}

// DefaultConfig returns default client configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:           "localhost:9161",
		ConnectTimeout: 30 * time.Second,
		RequestTimeout: 30 * time.Second,
	}
}

// New creates a new client.
func New(cfg *Config) *Client {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	c := &Client{
		addr:     cfg.Addr,
		token:    cfg.Token,
		pending:  make(map[uint64]chan *pb.Envelope),
		shutdown: make(chan struct{}),
	}

	if cfg.TLS {
		c.tlsConfig = &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify,
		}
	}

	return c
}

// =============================================================================
// FIX #11: State Transition Methods
// =============================================================================

// getState returns the current state.
func (c *Client) getState() ClientState {
	return ClientState(c.state.Load())
}

// transitionTo attempts to transition to a new state.
func (c *Client) transitionTo(newState ClientState) error {
	for {
		oldState := c.getState()
		transition := stateTransition{from: oldState, to: newState}

		if !validTransitions[transition] {
			return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, oldState, newState)
		}

		if c.state.CompareAndSwap(int32(oldState), int32(newState)) {
			return nil
		}
	}
}

// transitionFrom attempts to transition from a specific state to a new state.
func (c *Client) transitionFrom(from, to ClientState) bool {
	transition := stateTransition{from: from, to: to}
	if !validTransitions[transition] {
		return false
	}
	return c.state.CompareAndSwap(int32(from), int32(to))
}

// =============================================================================
// Connection Management
// =============================================================================

// Connect connects and authenticates to the server.
func (c *Client) Connect() error {
	return c.ConnectWithContext(context.Background())
}

// ConnectWithContext connects with a context for timeout/cancellation.
func (c *Client) ConnectWithContext(ctx context.Context) error {
	currentState := c.getState()
	switch currentState {
	case StateClosed:
		return ErrClientClosed
	case StateClosing:
		return ErrClientClosing
	case StateConnected:
		return ErrAlreadyConnected
	case StateConnecting:
		return fmt.Errorf("connection already in progress")
	}

	// FIX #11: Validated transition to connecting
	if !c.transitionFrom(StateDisconnected, StateConnecting) {
		return fmt.Errorf("cannot connect: current state is %s", c.getState())
	}

	success := false
	defer func() {
		if !success {
			c.transitionFrom(StateConnecting, StateDisconnected)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	var conn net.Conn
	var err error

	dialer := &net.Dialer{}

	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", c.addr)
	}
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	c.conn = conn
	c.wire = wire.NewConn(conn)

	if err := c.authenticate(ctx); err != nil {
		conn.Close()
		c.conn = nil
		c.wire = nil
		return err
	}

	go c.readLoop()

	if err := c.transitionTo(StateConnected); err != nil {
		conn.Close()
		c.conn = nil
		c.wire = nil
		return err
	}

	success = true
	return nil
}

func (c *Client) authenticate(ctx context.Context) error {
	if err := c.wire.Write(&pb.Envelope{
		Id: 1,
		Payload: &pb.Envelope_AuthReq{
			AuthReq: &pb.AuthRequest{Token: c.token},
		},
	}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetReadDeadline(deadline)
	} else {
		c.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	}
	defer c.conn.SetReadDeadline(time.Time{})

	env, err := c.wire.Read()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	if e := env.GetError(); e != nil {
		return fmt.Errorf("auth error: %s", e.Message)
	}

	resp := env.GetAuthResp()
	if resp == nil || !resp.Ok {
		msg := "authentication failed"
		if resp != nil && resp.Message != "" {
			msg = resp.Message
		}
		return fmt.Errorf("%s: %w", msg, ErrAuthFailed)
	}

	c.sessionID = resp.SessionId
	return nil
}

// Close closes the client connection.
//
// FIX #4: Uses ResettableOnce.Do for thread-safe close.
func (c *Client) Close() error {
	var closeErr error

	c.closeOnce.Do(func() {
		currentState := c.getState()
		if currentState == StateClosed || currentState == StateClosing {
			return
		}

		if currentState == StateConnected {
			c.transitionFrom(StateConnected, StateClosing)
		} else if currentState == StateDisconnected {
			c.transitionFrom(StateDisconnected, StateClosed)
			return
		}

		close(c.shutdown)

		c.mu.Lock()
		if c.conn != nil {
			closeErr = c.conn.Close()
			c.conn = nil
			c.wire = nil
		}
		c.mu.Unlock()

		c.pendingMu.Lock()
		for id, ch := range c.pending {
			close(ch)
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()

		c.transitionFrom(StateClosing, StateClosed)
	})

	return closeErr
}

// Reconnect attempts to reconnect to the server.
func (c *Client) Reconnect() error {
	return c.ReconnectWithContext(context.Background())
}

// ReconnectWithContext attempts to reconnect with context.
//
// FIX #4: Uses ResettableOnce.Reset() for thread-safe reset.
func (c *Client) ReconnectWithContext(ctx context.Context) error {
	currentState := c.getState()
	if currentState == StateClosed {
		return ErrClientClosed
	}

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.wire = nil
	}
	c.mu.Unlock()

	c.state.Store(int32(StateDisconnected))

	c.pendingMu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.pending = make(map[uint64]chan *pb.Envelope)
	c.pendingMu.Unlock()

	c.shutdown = make(chan struct{})

	// FIX #4: Thread-safe reset of closeOnce
	c.closeOnce.Reset()

	return c.ConnectWithContext(ctx)
}

// =============================================================================
// State Queries
// =============================================================================

// SessionID returns the session ID.
func (c *Client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// IsConnected returns true if connected.
func (c *Client) IsConnected() bool {
	return c.getState() == StateConnected
}

// IsClosed returns true if permanently closed.
func (c *Client) IsClosed() bool {
	return c.getState() == StateClosed
}

// State returns the current state as a string.
func (c *Client) State() string {
	return c.getState().String()
}

// =============================================================================
// Callbacks
// =============================================================================

// OnSample sets the handler for pushed samples.
func (c *Client) OnSample(fn func(*pb.Sample)) {
	c.pendingMu.Lock()
	c.onSample = fn
	c.pendingMu.Unlock()
}

// OnDisconnect sets the handler for disconnection.
func (c *Client) OnDisconnect(fn func(error)) {
	c.pendingMu.Lock()
	c.onDisconnect = fn
	c.pendingMu.Unlock()
}

// =============================================================================
// Read Loop
// =============================================================================

func (c *Client) readLoop() {
	var disconnectErr error

	defer func() {
		c.pendingMu.RLock()
		fn := c.onDisconnect
		c.pendingMu.RUnlock()

		if fn != nil && disconnectErr != nil {
			fn(disconnectErr)
		}
	}()

	for {
		if c.getState() != StateConnected {
			return
		}

		env, err := c.wire.Read()
		if err != nil {
			if c.getState() != StateConnected {
				return
			}

			disconnectErr = err
			c.transitionFrom(StateConnected, StateDisconnected)
			return
		}

		c.handleMessage(env)
	}
}

func (c *Client) handleMessage(env *pb.Envelope) {
	if sample := env.GetSample(); sample != nil {
		c.pendingMu.RLock()
		fn := c.onSample
		c.pendingMu.RUnlock()

		if fn != nil {
			fn(sample)
		}
		return
	}

	c.pendingMu.RLock()
	ch, ok := c.pending[env.Id]
	c.pendingMu.RUnlock()

	if ok {
		select {
		case ch <- env:
		default:
		}
	}
}

// =============================================================================
// Request/Response
// =============================================================================

func (c *Client) request(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	if c.getState() != StateConnected {
		return nil, ErrNotConnected
	}

	id := c.requestID.Add(1)
	env.Id = id

	ch := make(chan *pb.Envelope, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	c.mu.Lock()
	err := c.wire.Write(env)
	c.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrClientClosed
		}
		if e := resp.GetError(); e != nil {
			return nil, fmt.Errorf("error %d: %s", e.Code, e.Message)
		}
		return resp, nil

	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())

	case <-c.shutdown:
		return nil, ErrClientClosed
	}
}

// Request sends a request and waits for response with default timeout.
func (c *Client) Request(env *pb.Envelope) (*pb.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.request(ctx, env)
}

// RequestWithContext sends a request with a custom context.
func (c *Client) RequestWithContext(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	return c.request(ctx, env)
}

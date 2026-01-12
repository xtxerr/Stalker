// LOCATION: internal/client/client.go
// VERSION: 2.0 - Fixed double-close, improved state management
//
// FIXES:
// - State machine to prevent double-close
// - sync.Once for guaranteed single close
// - Proper connection state tracking
// - Context support for operations

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
	"github.com/xtxerr/stalker/internal/wire"
)

// ============================================================================
// Client States
// ============================================================================

const (
	stateDisconnected int32 = iota
	stateConnecting
	stateConnected
	stateClosing
	stateClosed
)

func stateName(s int32) string {
	switch s {
	case stateDisconnected:
		return "disconnected"
	case stateConnecting:
		return "connecting"
	case stateConnected:
		return "connected"
	case stateClosing:
		return "closing"
	case stateClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// ============================================================================
// Errors
// ============================================================================

var (
	ErrClientClosed     = errors.New("client is closed")
	ErrClientClosing    = errors.New("client is closing")
	ErrNotConnected     = errors.New("not connected")
	ErrAlreadyConnected = errors.New("already connected")
	ErrAuthFailed       = errors.New("authentication failed")
	ErrTimeout          = errors.New("request timeout")
)

// ============================================================================
// Client
// ============================================================================

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

	// State management
	state     atomic.Int32
	closeOnce sync.Once

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

// ============================================================================
// Connection Management
// ============================================================================

// Connect connects and authenticates to the server.
func (c *Client) Connect() error {
	return c.ConnectWithContext(context.Background())
}

// ConnectWithContext connects with a context for timeout/cancellation.
func (c *Client) ConnectWithContext(ctx context.Context) error {
	// Check state
	currentState := c.state.Load()
	if currentState == stateClosed {
		return ErrClientClosed
	}
	if currentState == stateClosing {
		return ErrClientClosing
	}
	if currentState == stateConnected {
		return ErrAlreadyConnected
	}

	// Try to transition to connecting
	if !c.state.CompareAndSwap(stateDisconnected, stateConnecting) {
		return fmt.Errorf("cannot connect: current state is %s", stateName(c.state.Load()))
	}

	// On failure, reset to disconnected
	success := false
	defer func() {
		if !success {
			c.state.Store(stateDisconnected)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Dial connection
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

	// Authenticate
	if err := c.authenticate(ctx); err != nil {
		conn.Close()
		c.conn = nil
		c.wire = nil
		return err
	}

	// Start read loop
	go c.readLoop()

	// Mark as connected
	c.state.Store(stateConnected)
	success = true

	return nil
}

func (c *Client) authenticate(ctx context.Context) error {
	// Send auth request
	if err := c.wire.Write(&pb.Envelope{
		Id: 1,
		Payload: &pb.Envelope_AuthReq{
			AuthReq: &pb.AuthRequest{Token: c.token},
		},
	}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// Set read deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetReadDeadline(deadline)
	} else {
		c.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	}
	defer c.conn.SetReadDeadline(time.Time{})

	// Read auth response
	env, err := c.wire.Read()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	// Check for error
	if e := env.GetError(); e != nil {
		return fmt.Errorf("auth error: %s", e.Message)
	}

	// Check response
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
// FIX: Uses sync.Once to prevent double-close panic.
func (c *Client) Close() error {
	var closeErr error

	c.closeOnce.Do(func() {
		// Transition to closing state
		oldState := c.state.Swap(stateClosing)
		if oldState == stateClosed || oldState == stateClosing {
			return
		}

		// Signal shutdown
		close(c.shutdown)

		// Close connection
		c.mu.Lock()
		if c.conn != nil {
			closeErr = c.conn.Close()
			c.conn = nil
			c.wire = nil
		}
		c.mu.Unlock()

		// Cancel pending requests
		c.pendingMu.Lock()
		for id, ch := range c.pending {
			close(ch)
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()

		// Mark as closed
		c.state.Store(stateClosed)
	})

	return closeErr
}

// Reconnect attempts to reconnect to the server.
func (c *Client) Reconnect() error {
	return c.ReconnectWithContext(context.Background())
}

// ReconnectWithContext attempts to reconnect with context.
func (c *Client) ReconnectWithContext(ctx context.Context) error {
	currentState := c.state.Load()
	if currentState == stateClosed {
		return ErrClientClosed
	}

	c.mu.Lock()
	// Close existing connection if any
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.wire = nil
	}
	c.mu.Unlock()

	// Reset state
	c.state.Store(stateDisconnected)

	// Cancel pending requests
	c.pendingMu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.pending = make(map[uint64]chan *pb.Envelope)
	c.pendingMu.Unlock()

	// Recreate shutdown channel
	c.shutdown = make(chan struct{})
	c.closeOnce = sync.Once{} // Reset closeOnce for new connection

	// Connect
	return c.ConnectWithContext(ctx)
}

// ============================================================================
// State Queries
// ============================================================================

// SessionID returns the session ID.
func (c *Client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// IsConnected returns true if connected.
func (c *Client) IsConnected() bool {
	return c.state.Load() == stateConnected
}

// IsClosed returns true if permanently closed.
func (c *Client) IsClosed() bool {
	return c.state.Load() == stateClosed
}

// State returns the current state as a string.
func (c *Client) State() string {
	return stateName(c.state.Load())
}

// ============================================================================
// Callbacks
// ============================================================================

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

// ============================================================================
// Read Loop
// ============================================================================

func (c *Client) readLoop() {
	var disconnectErr error

	defer func() {
		// Notify disconnect callback
		c.pendingMu.RLock()
		fn := c.onDisconnect
		c.pendingMu.RUnlock()

		if fn != nil && disconnectErr != nil {
			fn(disconnectErr)
		}
	}()

	for {
		// Check state before each read
		if c.state.Load() != stateConnected {
			return
		}

		env, err := c.wire.Read()
		if err != nil {
			// Check if intentional close
			if c.state.Load() != stateConnected {
				return
			}

			// Unexpected disconnect
			disconnectErr = err
			c.state.CompareAndSwap(stateConnected, stateDisconnected)
			return
		}

		c.handleMessage(env)
	}
}

func (c *Client) handleMessage(env *pb.Envelope) {
	// Handle pushed sample
	if sample := env.GetSample(); sample != nil {
		c.pendingMu.RLock()
		fn := c.onSample
		c.pendingMu.RUnlock()

		if fn != nil {
			fn(sample)
		}
		return
	}

	// Handle response to request
	c.pendingMu.RLock()
	ch, ok := c.pending[env.Id]
	c.pendingMu.RUnlock()

	if ok {
		select {
		case ch <- env:
		default:
			// Channel full or closed
		}
	}
}

// ============================================================================
// Request/Response
// ============================================================================

func (c *Client) request(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	if c.state.Load() != stateConnected {
		return nil, ErrNotConnected
	}

	// Generate request ID
	id := c.requestID.Add(1)
	env.Id = id

	// Create response channel
	ch := make(chan *pb.Envelope, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	// Send request
	c.mu.Lock()
	err := c.wire.Write(env)
	c.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Wait for response
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

// Request sends a request and waits for response.
func (c *Client) Request(env *pb.Envelope) (*pb.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.request(ctx, env)
}

// RequestWithContext sends a request with context.
func (c *Client) RequestWithContext(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	return c.request(ctx, env)
}

// ============================================================================
// High-Level Operations
// ============================================================================

// BindNamespace binds the session to a namespace.
func (c *Client) BindNamespace(namespace string) error {
	_, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_BindNamespaceReq{
			BindNamespaceReq: &pb.BindNamespaceRequest{Namespace: namespace},
		},
	})
	return err
}

// Browse retrieves information at the given path.
func (c *Client) Browse(req *pb.BrowseRequest) (*pb.BrowseResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_BrowseReq{BrowseReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetBrowseResp(), nil
}

// BrowsePath is a convenience method for simple path browsing.
func (c *Client) BrowsePath(path string, longFormat bool) (*pb.BrowseResponse, error) {
	return c.Browse(&pb.BrowseRequest{
		Path:       path,
		LongFormat: longFormat,
	})
}

// CreateTarget creates a new target.
func (c *Client) CreateTarget(req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_CreateTargetReq{CreateTargetReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetCreateTargetResp(), nil
}

// UpdateTarget updates a target.
func (c *Client) UpdateTarget(req *pb.UpdateTargetRequest) (*pb.UpdateTargetResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_UpdateTargetReq{UpdateTargetReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetUpdateTargetResp(), nil
}

// DeleteTarget deletes a target.
func (c *Client) DeleteTarget(req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_DeleteTargetReq{DeleteTargetReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetDeleteTargetResp(), nil
}

// CreatePoller creates a new poller.
func (c *Client) CreatePoller(req *pb.CreatePollerRequest) (*pb.CreatePollerResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_CreatePollerReq{CreatePollerReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetCreatePollerResp(), nil
}

// EnablePoller enables a poller.
func (c *Client) EnablePoller(req *pb.EnablePollerRequest) (*pb.EnablePollerResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_EnablePollerReq{EnablePollerReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEnablePollerResp(), nil
}

// DisablePoller disables a poller.
func (c *Client) DisablePoller(req *pb.DisablePollerRequest) (*pb.DisablePollerResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_DisablePollerReq{DisablePollerReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetDisablePollerResp(), nil
}

// Subscribe subscribes to poller samples.
func (c *Client) Subscribe(req *pb.SubscribeRequest) (*pb.SubscribeResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_SubscribeReq{SubscribeReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSubscribeResp(), nil
}

// Unsubscribe unsubscribes from poller samples.
func (c *Client) Unsubscribe(req *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	resp, err := c.Request(&pb.Envelope{
		Payload: &pb.Envelope_UnsubscribeReq{UnsubscribeReq: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetUnsubscribeResp(), nil
}

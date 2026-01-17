// Package handler provides request handling for the stalker protocol.
//
// Handlers are organized by entity type (namespace, target, poller, etc.)
// and use a common RequestContext for session and request information.
package handler

import (
	"fmt"

	"github.com/xtxerr/stalker/internal/errors"
	"github.com/xtxerr/stalker/internal/manager"
)

// =============================================================================
// Request Context
// =============================================================================

// RequestContext holds context for handling a request.
// It provides access to the session, request ID, and manager.
type RequestContext struct {
	Session   *Session
	RequestID uint64
	Manager   *manager.Manager
}

// Namespace returns the session's bound namespace.
// Returns an error if the session is not bound to a namespace.
func (ctx *RequestContext) Namespace() (string, error) {
	ns := ctx.Session.GetNamespace()
	if ns == "" {
		return "", errors.ErrNamespaceRequired
	}
	return ns, nil
}

// MustNamespace returns the session's bound namespace.
// Panics if the session is not bound to a namespace.
// Use this only after calling RequireNamespace.
func (ctx *RequestContext) MustNamespace() string {
	ns := ctx.Session.GetNamespace()
	if ns == "" {
		panic("MustNamespace called without namespace binding")
	}
	return ns
}

// HasNamespace returns true if the session is bound to a namespace.
func (ctx *RequestContext) HasNamespace() bool {
	return ctx.Session.IsBound()
}

// =============================================================================
// Handler
// =============================================================================

// Handler is the main request handler.
// It holds references to the manager and session manager.
type Handler struct {
	mgr            *manager.Manager
	sessionManager *SessionManager
}

// NewHandler creates a new handler.
func NewHandler(mgr *manager.Manager, sm *SessionManager) *Handler {
	return &Handler{
		mgr:            mgr,
		sessionManager: sm,
	}
}

// Manager returns the entity manager.
func (h *Handler) Manager() *manager.Manager {
	return h.mgr
}

// SessionManager returns the session manager.
func (h *Handler) SessionManager() *SessionManager {
	return h.sessionManager
}

// NewContext creates a request context.
func (h *Handler) NewContext(session *Session, requestID uint64) *RequestContext {
	return &RequestContext{
		Session:   session,
		RequestID: requestID,
		Manager:   h.mgr,
	}
}

// =============================================================================
// Error Handling - uses centralized error codes from errors package
// =============================================================================

// HandlerError represents a handler error with a wire protocol code.
type HandlerError struct {
	Code    int32  // Wire protocol code from errors.Code*
	Message string
	Cause   error // Optional underlying error
}

// Error implements the error interface.
func (e *HandlerError) Error() string {
	return e.Message
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *HandlerError) Unwrap() error {
	return e.Cause
}

// WithCause adds a cause to the error.
func (e *HandlerError) WithCause(err error) *HandlerError {
	e.Cause = err
	return e
}

// =============================================================================
// Error Constructors
// =============================================================================

// NewError creates a handler error from a wire code.
func NewError(code int32, msg string) *HandlerError {
	return &HandlerError{Code: code, Message: msg}
}

// NewErrorFromErr creates a handler error from a sentinel error.
// It automatically maps the error to the correct wire code.
func NewErrorFromErr(err error, msg string) *HandlerError {
	code := errors.ErrorToCode(err)
	fullMsg := msg
	if err != nil && msg != "" {
		fullMsg = fmt.Sprintf("%s: %v", msg, err)
	} else if err != nil {
		fullMsg = err.Error()
	}
	return &HandlerError{Code: code, Message: fullMsg, Cause: err}
}

// Errorf creates a formatted handler error.
func Errorf(code int32, format string, args ...interface{}) *HandlerError {
	return &HandlerError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WrapError wraps an error with context and auto-maps to wire code.
func WrapError(err error, format string, args ...interface{}) *HandlerError {
	msg := fmt.Sprintf(format, args...)
	return NewErrorFromErr(err, msg)
}

// =============================================================================
// Common error constructors using centralized codes
// =============================================================================

// ErrNotFound creates a not-found error.
func ErrNotFound(entityType, identifier string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeNotFound,
		Message: fmt.Sprintf("%s not found: %s", entityType, identifier),
		Cause:   errors.ErrNotFound,
	}
}

// ErrAlreadyExists creates an already-exists error.
func ErrAlreadyExists(entityType, identifier string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeAlreadyExists,
		Message: fmt.Sprintf("%s already exists: %s", entityType, identifier),
		Cause:   errors.ErrAlreadyExists,
	}
}

// ErrInvalidRequest creates an invalid request error.
func ErrInvalidRequest(msg string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeInvalidRequest,
		Message: msg,
		Cause:   errors.ErrInvalidConfig,
	}
}

// ErrInvalidRequestf creates a formatted invalid request error.
func ErrInvalidRequestf(format string, args ...interface{}) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeInvalidRequest,
		Message: fmt.Sprintf(format, args...),
		Cause:   errors.ErrInvalidConfig,
	}
}

// ErrInternal creates an internal error.
func ErrInternal(msg string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeInternal,
		Message: msg,
		Cause:   errors.ErrInternal,
	}
}

// ErrInternalf creates a formatted internal error.
func ErrInternalf(format string, args ...interface{}) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeInternal,
		Message: fmt.Sprintf(format, args...),
		Cause:   errors.ErrInternal,
	}
}

// ErrNotAuthorized creates a not-authorized error.
func ErrNotAuthorized(msg string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeNotAuthorized,
		Message: msg,
		Cause:   errors.ErrNotAuthorized,
	}
}

// ErrNotAuthorizedf creates a formatted not-authorized error.
func ErrNotAuthorizedf(format string, args ...interface{}) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeNotAuthorized,
		Message: fmt.Sprintf(format, args...),
		Cause:   errors.ErrNotAuthorized,
	}
}

// ErrTimeout creates a timeout error.
func ErrTimeout(msg string) *HandlerError {
	return &HandlerError{
		Code:    errors.CodeTimeout,
		Message: msg,
		Cause:   errors.ErrTimeout,
	}
}

// =============================================================================
// Common errors (pre-defined singletons)
// =============================================================================

var (
	// ErrSessionNotBound is returned when an operation requires a namespace binding.
	ErrSessionNotBound = &HandlerError{
		Code:    errors.CodeNamespaceRequired,
		Message: "session not bound to namespace",
		Cause:   errors.ErrNamespaceRequired,
	}

	// ErrConcurrentMod is returned on version mismatch.
	ErrConcurrentMod = &HandlerError{
		Code:    errors.CodeConcurrentMod,
		Message: "entity was modified by another client",
		Cause:   errors.ErrConcurrentModification,
	}

	// ErrNotAuthenticated is returned when authentication is required.
	ErrNotAuthenticated = &HandlerError{
		Code:    errors.CodeNotAuthenticated,
		Message: "authentication required",
		Cause:   errors.ErrNotAuthenticated,
	}
)

// =============================================================================
// Helper functions
// =============================================================================

// RequireNamespace returns error if session is not bound.
// Use this at the start of handlers that require a namespace.
func RequireNamespace(ctx *RequestContext) error {
	if !ctx.Session.IsBound() {
		return ErrSessionNotBound
	}
	return nil
}

// RequireNamespaceAccess checks if session can access the namespace.
func RequireNamespaceAccess(ctx *RequestContext, sm *SessionManager, namespace string) error {
	if !sm.CanAccessNamespace(ctx.Session.TokenID, namespace) {
		return &HandlerError{
			Code:    errors.CodeNotAuthorized,
			Message: fmt.Sprintf("not authorized to access namespace: %s", namespace),
			Cause:   errors.ErrNotAuthorized,
		}
	}
	return nil
}

// IsHandlerError checks if an error is a HandlerError.
func IsHandlerError(err error) bool {
	_, ok := err.(*HandlerError)
	return ok
}

// GetErrorCode extracts the wire code from an error.
// Returns CodeInternal if not a HandlerError.
func GetErrorCode(err error) int32 {
	if herr, ok := err.(*HandlerError); ok {
		return herr.Code
	}
	// Try to map the error using the errors package
	return errors.ErrorToCode(err)
}

// ToHandlerError converts any error to a HandlerError.
// If the error is already a HandlerError, it is returned as-is.
// Otherwise, it is wrapped with the appropriate wire code.
func ToHandlerError(err error) *HandlerError {
	if err == nil {
		return nil
	}
	if herr, ok := err.(*HandlerError); ok {
		return herr
	}
	return NewErrorFromErr(err, "")
}

// =============================================================================
// Namespace-Required Handler Wrapper
// =============================================================================

// NamespaceHandler is a handler function that requires a namespace.
type NamespaceHandlerFunc func(ctx *RequestContext, namespace string) error

// WithNamespace wraps a handler to automatically extract and validate the namespace.
func WithNamespace(fn NamespaceHandlerFunc) func(ctx *RequestContext) error {
	return func(ctx *RequestContext) error {
		ns, err := ctx.Namespace()
		if err != nil {
			return ErrSessionNotBound
		}
		return fn(ctx, ns)
	}
}

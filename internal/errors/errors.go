// LOCATION: internal/errors/errors.go
// VERSION: 2.0 - Consolidated error definitions for the entire project
//
// This file provides:
// - Wire protocol error codes
// - Sentinel errors for all error conditions
// - Error category checking functions
// - ErrorToCode and CodeToError mapping
// - Error wrapping utilities

package errors

import (
	"errors"
	"fmt"
)

// ============================================================================
// Wire protocol error codes - used in protobuf Error messages
// ============================================================================

const (
	CodeUnknown           int32 = 1
	CodeAuthFailed        int32 = 2
	CodeNotAuthenticated  int32 = 3
	CodeInvalidRequest    int32 = 4
	CodeNotFound          int32 = 5
	CodeAlreadyExists     int32 = 6
	CodeInternal          int32 = 7
	CodeNotAuthorized     int32 = 8
	CodeNamespaceRequired int32 = 9
	CodeConcurrentMod     int32 = 10
	CodeInUse             int32 = 11
	CodeSNMPError         int32 = 12
	CodeTimeout           int32 = 13
)

// CodeName returns a human-readable name for an error code.
func CodeName(code int32) string {
	switch code {
	case CodeUnknown:
		return "Unknown"
	case CodeAuthFailed:
		return "AuthFailed"
	case CodeNotAuthenticated:
		return "NotAuthenticated"
	case CodeInvalidRequest:
		return "InvalidRequest"
	case CodeNotFound:
		return "NotFound"
	case CodeAlreadyExists:
		return "AlreadyExists"
	case CodeInternal:
		return "Internal"
	case CodeNotAuthorized:
		return "NotAuthorized"
	case CodeNamespaceRequired:
		return "NamespaceRequired"
	case CodeConcurrentMod:
		return "ConcurrentModification"
	case CodeInUse:
		return "InUse"
	case CodeSNMPError:
		return "SNMPError"
	case CodeTimeout:
		return "Timeout"
	default:
		return fmt.Sprintf("Code(%d)", code)
	}
}

// ============================================================================
// Sentinel errors for common conditions
// ============================================================================

var (
	// Not found errors
	ErrNotFound          = errors.New("not found")
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrTargetNotFound    = errors.New("target not found")
	ErrPollerNotFound    = errors.New("poller not found")
	ErrSecretNotFound    = errors.New("secret not found")
	ErrPathNotFound      = errors.New("path not found")
	ErrSessionNotFound   = errors.New("session not found")

	// Already exists errors
	ErrAlreadyExists          = errors.New("already exists")
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrTargetAlreadyExists    = errors.New("target already exists")
	ErrPollerAlreadyExists    = errors.New("poller already exists")
	ErrSecretAlreadyExists    = errors.New("secret already exists")
	ErrPathAlreadyExists      = errors.New("path already exists")

	// Validation errors
	ErrInvalidName     = errors.New("invalid name")
	ErrInvalidPath     = errors.New("invalid path")
	ErrInvalidOID      = errors.New("invalid OID")
	ErrInvalidInterval = errors.New("invalid interval")
	ErrInvalidConfig   = errors.New("invalid configuration")
	ErrMissingField    = errors.New("missing required field")
	ErrInvalidVersion  = errors.New("invalid version")
	ErrInvalidProtocol = errors.New("invalid protocol")

	// State errors
	ErrInvalidState      = errors.New("invalid state")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrPollerDisabled    = errors.New("poller is disabled")
	ErrPollerRunning     = errors.New("poller is already running")
	ErrPollerStopped     = errors.New("poller is already stopped")

	// Auth/Session errors
	ErrNotAuthenticated  = errors.New("not authenticated")
	ErrNotAuthorized     = errors.New("not authorized")
	ErrInvalidToken      = errors.New("invalid token")
	ErrSessionExpired    = errors.New("session expired")
	ErrNamespaceRequired = errors.New("namespace binding required")
	ErrSessionClosed     = errors.New("session is closed")

	// Protocol errors
	ErrUnsupportedProtocol = errors.New("unsupported protocol")
	ErrSNMPError           = errors.New("SNMP error")
	ErrTimeout             = errors.New("timeout")
	ErrConnectionFailed    = errors.New("connection failed")

	// Internal errors
	ErrInternal   = errors.New("internal error")
	ErrDatabase   = errors.New("database error")
	ErrEncryption = errors.New("encryption error")

	// Store-specific errors
	ErrConcurrentModification = errors.New("concurrent modification detected (version mismatch)")
	ErrNotEmpty               = errors.New("not empty")
	ErrInUse                  = errors.New("in use")
	ErrInvalidReference       = errors.New("invalid reference")
	ErrSecretKeyNotConfigured = errors.New("secret key not configured")
	ErrBufferFull             = errors.New("buffer full")
)

// ============================================================================
// Helper functions for error checking
// ============================================================================

// Is is a convenience wrapper for errors.Is
var Is = errors.Is

// As is a convenience wrapper for errors.As
var As = errors.As

// IsNotFound returns true if err is a not-found error.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrNamespaceNotFound) ||
		errors.Is(err, ErrTargetNotFound) ||
		errors.Is(err, ErrPollerNotFound) ||
		errors.Is(err, ErrSecretNotFound) ||
		errors.Is(err, ErrPathNotFound) ||
		errors.Is(err, ErrSessionNotFound)
}

// IsAlreadyExists returns true if err is an already-exists error.
func IsAlreadyExists(err error) bool {
	return errors.Is(err, ErrAlreadyExists) ||
		errors.Is(err, ErrNamespaceAlreadyExists) ||
		errors.Is(err, ErrTargetAlreadyExists) ||
		errors.Is(err, ErrPollerAlreadyExists) ||
		errors.Is(err, ErrSecretAlreadyExists) ||
		errors.Is(err, ErrPathAlreadyExists)
}

// IsValidation returns true if err is a validation error.
func IsValidation(err error) bool {
	return errors.Is(err, ErrInvalidName) ||
		errors.Is(err, ErrInvalidPath) ||
		errors.Is(err, ErrInvalidOID) ||
		errors.Is(err, ErrInvalidInterval) ||
		errors.Is(err, ErrInvalidConfig) ||
		errors.Is(err, ErrMissingField) ||
		errors.Is(err, ErrInvalidVersion) ||
		errors.Is(err, ErrInvalidProtocol)
}

// IsStateError returns true if err is a state-related error.
func IsStateError(err error) bool {
	return errors.Is(err, ErrInvalidState) ||
		errors.Is(err, ErrInvalidTransition) ||
		errors.Is(err, ErrPollerDisabled) ||
		errors.Is(err, ErrPollerRunning) ||
		errors.Is(err, ErrPollerStopped)
}

// IsAuthError returns true if err is an authentication/authorization error.
func IsAuthError(err error) bool {
	return errors.Is(err, ErrNotAuthenticated) ||
		errors.Is(err, ErrNotAuthorized) ||
		errors.Is(err, ErrInvalidToken) ||
		errors.Is(err, ErrSessionExpired) ||
		errors.Is(err, ErrNamespaceRequired)
}

// IsProtocolError returns true if err is a protocol-related error.
func IsProtocolError(err error) bool {
	return errors.Is(err, ErrUnsupportedProtocol) ||
		errors.Is(err, ErrSNMPError) ||
		errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrConnectionFailed)
}

// IsRetriable returns true if the error is potentially retriable.
func IsRetriable(err error) bool {
	return errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrConnectionFailed) ||
		errors.Is(err, ErrConcurrentModification) ||
		errors.Is(err, ErrBufferFull)
}

// ============================================================================
// Error to wire code mapping
// ============================================================================

// ErrorToCode maps a sentinel error to its wire protocol code.
func ErrorToCode(err error) int32 {
	if err == nil {
		return CodeUnknown
	}

	switch {
	// Auth errors
	case Is(err, ErrInvalidToken):
		return CodeAuthFailed
	case Is(err, ErrNotAuthenticated):
		return CodeNotAuthenticated
	case Is(err, ErrNotAuthorized):
		return CodeNotAuthorized
	case Is(err, ErrNamespaceRequired):
		return CodeNamespaceRequired

	// Not found
	case IsNotFound(err):
		return CodeNotFound

	// Already exists
	case IsAlreadyExists(err):
		return CodeAlreadyExists

	// Validation
	case IsValidation(err):
		return CodeInvalidRequest

	// Concurrent modification
	case Is(err, ErrConcurrentModification):
		return CodeConcurrentMod

	// In use
	case Is(err, ErrInUse), Is(err, ErrNotEmpty):
		return CodeInUse

	// Protocol errors
	case Is(err, ErrSNMPError):
		return CodeSNMPError
	case Is(err, ErrTimeout):
		return CodeTimeout

	// Default to internal
	default:
		return CodeInternal
	}
}

// CodeToError maps a wire code to a sentinel error (for clients).
func CodeToError(code int32) error {
	switch code {
	case CodeUnknown:
		return ErrInternal
	case CodeAuthFailed:
		return ErrInvalidToken
	case CodeNotAuthenticated:
		return ErrNotAuthenticated
	case CodeInvalidRequest:
		return ErrInvalidConfig
	case CodeNotFound:
		return ErrNotFound
	case CodeAlreadyExists:
		return ErrAlreadyExists
	case CodeInternal:
		return ErrInternal
	case CodeNotAuthorized:
		return ErrNotAuthorized
	case CodeNamespaceRequired:
		return ErrNamespaceRequired
	case CodeConcurrentMod:
		return ErrConcurrentModification
	case CodeInUse:
		return ErrInUse
	case CodeSNMPError:
		return ErrSNMPError
	case CodeTimeout:
		return ErrTimeout
	default:
		return ErrInternal
	}
}

// ============================================================================
// Error wrapping utilities
// ============================================================================

// Wrap wraps an error with additional context.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

// Wrapf wraps an error with formatted context.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// ============================================================================
// Error constructors with context
// ============================================================================

// NewNotFound creates a not-found error with context.
func NewNotFound(entityType, identifier string) error {
	return fmt.Errorf("%s '%s': %w", entityType, identifier, ErrNotFound)
}

// NewAlreadyExists creates an already-exists error with context.
func NewAlreadyExists(entityType, identifier string) error {
	return fmt.Errorf("%s '%s': %w", entityType, identifier, ErrAlreadyExists)
}

// NewValidation creates a validation error with context.
func NewValidation(field, reason string) error {
	return fmt.Errorf("invalid %s: %s: %w", field, reason, ErrInvalidConfig)
}

// NewMissingField creates a missing field error.
func NewMissingField(field string) error {
	return fmt.Errorf("%s: %w", field, ErrMissingField)
}

// NewInvalidValue creates an invalid value error.
func NewInvalidValue(field string, value interface{}, reason string) error {
	return fmt.Errorf("invalid %s '%v': %s: %w", field, value, reason, ErrInvalidConfig)
}

// ============================================================================
// Validation Errors Collection
// ============================================================================

// ValidationErrors collects multiple validation errors.
type ValidationErrors struct {
	Errors []error
}

// NewValidationErrors creates a new ValidationErrors collector.
func NewValidationErrors() *ValidationErrors {
	return &ValidationErrors{}
}

// Add adds an error to the collection.
func (v *ValidationErrors) Add(err error) {
	if err != nil {
		v.Errors = append(v.Errors, err)
	}
}

// AddField adds a field validation error.
func (v *ValidationErrors) AddField(field, reason string) {
	v.Errors = append(v.Errors, NewValidation(field, reason))
}

// AddMissing adds a missing field error.
func (v *ValidationErrors) AddMissing(field string) {
	v.Errors = append(v.Errors, NewMissingField(field))
}

// HasErrors returns true if there are any errors.
func (v *ValidationErrors) HasErrors() bool {
	return len(v.Errors) > 0
}

// Error implements the error interface.
func (v *ValidationErrors) Error() string {
	if len(v.Errors) == 0 {
		return ""
	}
	if len(v.Errors) == 1 {
		return v.Errors[0].Error()
	}

	msg := fmt.Sprintf("validation failed with %d errors:", len(v.Errors))
	for _, err := range v.Errors {
		msg += "\n  - " + err.Error()
	}
	return msg
}

// Err returns nil if no errors, otherwise returns the ValidationErrors.
func (v *ValidationErrors) Err() error {
	if len(v.Errors) == 0 {
		return nil
	}
	return v
}

// Unwrap returns the first error for errors.Is/As support.
func (v *ValidationErrors) Unwrap() error {
	if len(v.Errors) == 0 {
		return nil
	}
	return v.Errors[0]
}

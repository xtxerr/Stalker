// Package logging provides structured logging for the stalker application.
//
// This package wraps the standard library's log/slog package to provide
// consistent logging across all components. It supports both text and JSON
// output formats, configurable log levels, and component-based loggers.
//
// Usage:
//
//	// Initialize at startup
//	logging.Init(slog.LevelInfo, false) // Text format
//	logging.Init(slog.LevelDebug, true) // JSON format for production
//
//	// Get a component logger
//	log := logging.Component("scheduler")
//	log.Info("scheduler started", "workers", 10)
//
//	// Log with context
//	log.Error("poll failed", "error", err, "key", pollerKey)
package logging

import (
	"context"
	"log/slog"
	"os"
)

// Logger is the global logger instance.
var Logger *slog.Logger

// Init initializes the global logger with the specified level and format.
// If jsonFormat is true, logs are output as JSON; otherwise, human-readable text.
func Init(level slog.Level, jsonFormat bool) {
	var handler slog.Handler

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
	}

	if jsonFormat {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

// InitWithHandler initializes the global logger with a custom handler.
// This is useful for testing or custom output destinations.
func InitWithHandler(handler slog.Handler) {
	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

// With returns a new logger with additional attributes.
// These attributes are included in every log entry from the returned logger.
func With(args ...any) *slog.Logger {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	return Logger.With(args...)
}

// Component returns a logger for a specific component.
// The component name is added as an attribute to all log entries.
//
// Example:
//
//	log := logging.Component("scheduler")
//	log.Info("started") // Output: time=... level=INFO component=scheduler msg=started
func Component(name string) *slog.Logger {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	return Logger.With("component", name)
}

// WithContext returns a logger that includes context values.
// This is useful for request-scoped logging with trace IDs, etc.
func WithContext(ctx context.Context) *slog.Logger {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}

	// Extract common context values if present
	logger := Logger

	if sessionID, ok := ctx.Value(contextKeySessionID).(string); ok {
		logger = logger.With("session_id", sessionID)
	}
	if namespace, ok := ctx.Value(contextKeyNamespace).(string); ok {
		logger = logger.With("namespace", namespace)
	}
	if requestID, ok := ctx.Value(contextKeyRequestID).(uint64); ok {
		logger = logger.With("request_id", requestID)
	}

	return logger
}

// Context key types for type-safe context value extraction.
type contextKey int

const (
	contextKeySessionID contextKey = iota
	contextKeyNamespace
	contextKeyRequestID
)

// ContextWithSessionID adds a session ID to the context for logging.
func ContextWithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, contextKeySessionID, sessionID)
}

// ContextWithNamespace adds a namespace to the context for logging.
func ContextWithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, contextKeyNamespace, namespace)
}

// ContextWithRequestID adds a request ID to the context for logging.
func ContextWithRequestID(ctx context.Context, requestID uint64) context.Context {
	return context.WithValue(ctx, contextKeyRequestID, requestID)
}

// =============================================================================
// Convenience Functions
// =============================================================================

// Debug logs at debug level.
func Debug(msg string, args ...any) {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	Logger.Debug(msg, args...)
}

// Info logs at info level.
func Info(msg string, args ...any) {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	Logger.Info(msg, args...)
}

// Warn logs at warning level.
func Warn(msg string, args ...any) {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	Logger.Warn(msg, args...)
}

// Error logs at error level.
func Error(msg string, args ...any) {
	if Logger == nil {
		Init(slog.LevelInfo, false)
	}
	Logger.Error(msg, args...)
}

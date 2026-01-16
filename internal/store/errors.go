package store

import (
	"github.com/xtxerr/stalker/internal/errors"
)

var (
	ErrNotFound               = errors.ErrNotFound
	ErrAlreadyExists          = errors.ErrAlreadyExists
	ErrConcurrentModification = errors.ErrConcurrentModification
	ErrNotEmpty               = errors.ErrNotEmpty
	ErrInUse                  = errors.ErrInUse
	ErrInvalidReference       = errors.ErrInvalidReference
	ErrSecretKeyNotConfigured = errors.ErrSecretKeyNotConfigured

	// Poller-specific aliases
	ErrPollerNotFound = errors.ErrNotFound
)

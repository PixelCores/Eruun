package jobinfo

import (
	"errors"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

// StatusError wraps an error with an explicit job status for persistence.
type StatusError struct {
	Status config.Status
	Err    error
}

func (s *StatusError) Error() string { return s.Err.Error() }

func (s *StatusError) Unwrap() error { return s.Err }

// NewStatusError constructs a StatusError with the provided status and error.
func NewStatusError(status config.Status, err error) error {
	if err == nil {
		return nil
	}
	return &StatusError{Status: status, Err: err}
}

// ExtractStatusError attempts to retrieve a StatusError from err.
func ExtractStatusError(err error) (*StatusError, bool) {
	if err == nil {
		return nil, false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}

var ErrJobExecutionIdentityChanged = errors.New("job execution identity changed")

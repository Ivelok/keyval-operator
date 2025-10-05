package errors

import "errors"

var (
	// ErrTransient indicates the operation may succeed on retry.
	ErrTransient = errors.New("transient")
	// ErrFatal indicates the operation should not be retried without human intervention.
	ErrFatal = errors.New("fatal")
)

// WrapTransient annotates the provided error as transient.
func WrapTransient(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrTransient, err)
}

// WrapFatal annotates the provided error as fatal.
func WrapFatal(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrFatal, err)
}

// IsTransient reports whether the error chain contains ErrTransient.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient)
}

// IsFatal reports whether the error chain contains ErrFatal.
func IsFatal(err error) bool {
	return errors.Is(err, ErrFatal)
}

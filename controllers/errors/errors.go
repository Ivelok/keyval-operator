package errors

import "errors"

var (
	// ErrTransient indicates the operation may succeed on retry.
	ErrTransient = errors.New("transient")
	// ErrFatal indicates the operation should not be retried without human intervention.
	ErrFatal = errors.New("fatal")
	// ErrKubeAPI indicates a Kubernetes API error.
	ErrKubeAPI = errors.New("kubeapi")
	// ErrExternalDependency indicates a Redis/Sentinel or external dependency error.
	ErrExternalDependency = errors.New("external-dependency")
	// ErrConfigDrift indicates an unexpected configuration drift.
	ErrConfigDrift = errors.New("config-drift")
	// ErrConflict indicates a write conflict or optimistic lock issue.
	ErrConflict = errors.New("conflict")
	// ErrInvalidSpec indicates a spec validation error.
	ErrInvalidSpec = errors.New("invalid-spec")
)

type Class string

const (
	ClassUnknown     Class = "unknown"
	ClassTransient   Class = "transient"
	ClassFatal       Class = "fatal"
	ClassKubeAPI     Class = "kubeapi"
	ClassExternal    Class = "external-dependency"
	ClassConfigDrift Class = "config-drift"
	ClassConflict    Class = "conflict"
	ClassInvalidSpec Class = "invalid-spec"
)

func wrapWith(marker error, err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(marker, err)
}

// WrapTransient annotates the provided error as transient.
func WrapTransient(err error) error {
	return wrapWith(ErrTransient, err)
}

// WrapFatal annotates the provided error as fatal.
func WrapFatal(err error) error {
	return wrapWith(ErrFatal, err)
}

// WrapKubeAPI annotates the provided error as a Kubernetes API failure.
func WrapKubeAPI(err error) error {
	return wrapWith(ErrKubeAPI, err)
}

// WrapExternalDependency annotates the provided error as an external dependency failure.
func WrapExternalDependency(err error) error {
	return wrapWith(ErrExternalDependency, err)
}

// WrapConfigDrift annotates the provided error as a configuration drift.
func WrapConfigDrift(err error) error {
	return wrapWith(ErrConfigDrift, err)
}

// WrapConflict annotates the provided error as a conflict.
func WrapConflict(err error) error {
	return wrapWith(ErrConflict, err)
}

// WrapInvalidSpec annotates the provided error as an invalid spec.
func WrapInvalidSpec(err error) error {
	return wrapWith(ErrInvalidSpec, err)
}

// IsTransient reports whether the error chain contains ErrTransient.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient)
}

// IsFatal reports whether the error chain contains ErrFatal.
func IsFatal(err error) bool {
	return errors.Is(err, ErrFatal)
}

// Classify returns the highest-signal class detected in the error chain.
func Classify(err error) Class {
	switch {
	case errors.Is(err, ErrFatal):
		return ClassFatal
	case errors.Is(err, ErrKubeAPI):
		return ClassKubeAPI
	case errors.Is(err, ErrExternalDependency):
		return ClassExternal
	case errors.Is(err, ErrConfigDrift):
		return ClassConfigDrift
	case errors.Is(err, ErrConflict):
		return ClassConflict
	case errors.Is(err, ErrInvalidSpec):
		return ClassInvalidSpec
	case errors.Is(err, ErrTransient):
		return ClassTransient
	default:
		return ClassUnknown
	}
}

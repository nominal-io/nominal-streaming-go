package nominal_streaming

import (
	"errors"
	"fmt"

	cerrors "github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
)

// NominalError provides structured access to Scout API errors.
// It exposes the error code, name, instance ID, and parameters from Conjure errors
// in a user-friendly way.
type NominalError struct {
	err        error
	code       string                 // e.g., "INVALID_ARGUMENT", "NOT_FOUND"
	name       string                 // e.g., "Scout:DatasetNotFound"
	instanceID string                 // UUID for error tracking/support tickets
	statusCode int                    // HTTP status code
	params     map[string]interface{} // Additional error parameters
}

// Error implements the error interface.
func (e *NominalError) Error() string {
	if e.name != "" && e.code != "" && e.instanceID != "" {
		return fmt.Sprintf("%s [%s] (instanceId=%s): %v", e.name, e.code, e.instanceID, e.err)
	}
	if e.statusCode != 0 {
		return fmt.Sprintf("HTTP %d: %v", e.statusCode, e.err)
	}
	return e.err.Error()
}

// Unwrap returns the underlying error, allowing errors.Is and errors.As to work.
func (e *NominalError) Unwrap() error {
	return e.err
}

// Code returns the error category code (e.g., "INVALID_ARGUMENT", "NOT_FOUND", "UNAUTHORIZED").
func (e *NominalError) Code() string {
	return e.code
}

// Name returns the specific error type name (e.g., "Scout:DatasetNotFound").
func (e *NominalError) Name() string {
	return e.name
}

// InstanceID returns the unique identifier for this error occurrence.
// This can be used when contacting support to identify the specific error.
func (e *NominalError) InstanceID() string {
	return e.instanceID
}

// StatusCode returns the HTTP status code associated with this error.
func (e *NominalError) StatusCode() int {
	return e.statusCode
}

// Parameters returns additional context parameters from the error.
func (e *NominalError) Parameters() map[string]interface{} {
	return e.params
}

// wrapAPIError wraps an API error in a NominalError if it contains Conjure error information.
// If the error doesn't contain Conjure error info, it still wraps it to provide
// HTTP status code information if available.
func wrapAPIError(err error) error {
	if err == nil {
		return nil
	}

	nomErr := &NominalError{
		err:    err,
		params: make(map[string]interface{}),
	}

	// Extract HTTP status code if available
	if statusCode, ok := httpclient.StatusCodeFromError(err); ok {
		nomErr.statusCode = statusCode
	}

	// Extract Conjure error information if available
	if conjureErr := cerrors.GetConjureError(err); conjureErr != nil {
		nomErr.code = conjureErr.Code().String()
		nomErr.name = conjureErr.Name()
		nomErr.instanceID = conjureErr.InstanceID().String()

		// Extract safe and unsafe parameters
		safeParams, unsafeParams := conjureErr.SafeParams(), conjureErr.UnsafeParams()
		for k, v := range safeParams {
			nomErr.params[k] = v
		}
		for k, v := range unsafeParams {
			nomErr.params[k] = v
		}
	}

	return nomErr
}

// AsNominalError extracts a NominalError from an error chain.
// Returns the NominalError and true if found, or nil and false if not.
func AsNominalError(err error) (*NominalError, bool) {
	var nomErr *NominalError
	if errors.As(err, &nomErr) {
		return nomErr, true
	}
	return nil, false
}

// IsRetryable returns true if the error is transient and the operation may succeed on retry.
// Retryable errors include timeouts, rate limiting (429), and server errors (5xx).
// Non-retryable errors include authentication failures, permission denied, invalid arguments, etc.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for NominalError first
	if nomErr, ok := AsNominalError(err); ok {
		return isRetryableStatusCode(nomErr.statusCode) || isRetryableCode(nomErr.code)
	}

	// Fall back to checking HTTP status code directly
	if statusCode, ok := httpclient.StatusCodeFromError(err); ok {
		return isRetryableStatusCode(statusCode)
	}

	// Check for Conjure error
	if conjureErr := cerrors.GetConjureError(err); conjureErr != nil {
		return isRetryableCode(conjureErr.Code().String())
	}

	return false
}

func isRetryableStatusCode(statusCode int) bool {
	switch statusCode {
	case 408, // Request Timeout
		429, // Too Many Requests
		502, // Bad Gateway
		503, // Service Unavailable
		504: // Gateway Timeout
		return true
	default:
		return statusCode >= 500 && statusCode < 600
	}
}

func isRetryableCode(code string) bool {
	switch code {
	case "TIMEOUT", "INTERNAL", "FAILED_PRECONDITION":
		return true
	default:
		return false
	}
}

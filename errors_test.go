package nominal_streaming

import (
	"errors"
	"fmt"
	"testing"

	cerrors "github.com/palantir/conjure-go-runtime/v2/conjure-go-contract/errors"
)

func TestNominalError_Error(t *testing.T) {
	tests := []struct {
		name     string
		nomErr   *NominalError
		expected string
	}{
		{
			name: "full error with all fields",
			nomErr: &NominalError{
				err:        errors.New("underlying error"),
				code:       "NOT_FOUND",
				name:       "Scout:DatasetNotFound",
				instanceID: "12345678-1234-1234-1234-123456789abc",
				statusCode: 404,
			},
			expected: "Scout:DatasetNotFound [NOT_FOUND] (instanceId=12345678-1234-1234-1234-123456789abc): underlying error",
		},
		{
			name: "error with only status code",
			nomErr: &NominalError{
				err:        errors.New("server error"),
				statusCode: 500,
			},
			expected: "HTTP 500: server error",
		},
		{
			name: "error with no extra info",
			nomErr: &NominalError{
				err: errors.New("plain error"),
			},
			expected: "plain error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.nomErr.Error()
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

func TestNominalError_Unwrap(t *testing.T) {
	underlying := errors.New("underlying error")
	nomErr := &NominalError{err: underlying}

	if nomErr.Unwrap() != underlying {
		t.Error("Unwrap should return the underlying error")
	}

	// Test that errors.Is works through the wrapper
	if !errors.Is(nomErr, underlying) {
		t.Error("errors.Is should find the underlying error")
	}
}

func TestNominalError_Accessors(t *testing.T) {
	params := map[string]interface{}{"key": "value"}
	nomErr := &NominalError{
		err:        errors.New("test"),
		code:       "INVALID_ARGUMENT",
		name:       "Scout:InvalidChannel",
		instanceID: "test-instance-id",
		statusCode: 400,
		params:     params,
	}

	if nomErr.Code() != "INVALID_ARGUMENT" {
		t.Errorf("expected code INVALID_ARGUMENT, got %s", nomErr.Code())
	}
	if nomErr.Name() != "Scout:InvalidChannel" {
		t.Errorf("expected name Scout:InvalidChannel, got %s", nomErr.Name())
	}
	if nomErr.InstanceID() != "test-instance-id" {
		t.Errorf("expected instanceID test-instance-id, got %s", nomErr.InstanceID())
	}
	if nomErr.StatusCode() != 400 {
		t.Errorf("expected statusCode 400, got %d", nomErr.StatusCode())
	}
	if nomErr.Parameters()["key"] != "value" {
		t.Error("expected params to contain key=value")
	}
}

func TestAsNominalError(t *testing.T) {
	t.Run("finds NominalError directly", func(t *testing.T) {
		nomErr := &NominalError{
			err:  errors.New("test"),
			code: "TEST",
		}
		found, ok := AsNominalError(nomErr)
		if !ok {
			t.Error("should find NominalError")
		}
		if found.Code() != "TEST" {
			t.Errorf("expected code TEST, got %s", found.Code())
		}
	})

	t.Run("finds NominalError wrapped in fmt.Errorf", func(t *testing.T) {
		nomErr := &NominalError{
			err:  errors.New("test"),
			code: "WRAPPED",
		}
		wrapped := fmt.Errorf("outer: %w", nomErr)
		found, ok := AsNominalError(wrapped)
		if !ok {
			t.Error("should find wrapped NominalError")
		}
		if found.Code() != "WRAPPED" {
			t.Errorf("expected code WRAPPED, got %s", found.Code())
		}
	})

	t.Run("returns false for non-NominalError", func(t *testing.T) {
		plainErr := errors.New("plain error")
		_, ok := AsNominalError(plainErr)
		if ok {
			t.Error("should not find NominalError in plain error")
		}
	})

	t.Run("returns false for nil", func(t *testing.T) {
		_, ok := AsNominalError(nil)
		if ok {
			t.Error("should not find NominalError in nil")
		}
	})
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		{
			name: "timeout status code",
			err: &NominalError{
				err:        errors.New("timeout"),
				statusCode: 408,
			},
			retryable: true,
		},
		{
			name: "rate limited",
			err: &NominalError{
				err:        errors.New("rate limited"),
				statusCode: 429,
			},
			retryable: true,
		},
		{
			name: "bad gateway",
			err: &NominalError{
				err:        errors.New("bad gateway"),
				statusCode: 502,
			},
			retryable: true,
		},
		{
			name: "service unavailable",
			err: &NominalError{
				err:        errors.New("unavailable"),
				statusCode: 503,
			},
			retryable: true,
		},
		{
			name: "gateway timeout",
			err: &NominalError{
				err:        errors.New("gateway timeout"),
				statusCode: 504,
			},
			retryable: true,
		},
		{
			name: "internal server error",
			err: &NominalError{
				err:        errors.New("internal"),
				statusCode: 500,
			},
			retryable: true,
		},
		{
			name: "timeout error code",
			err: &NominalError{
				err:  errors.New("timeout"),
				code: "TIMEOUT",
			},
			retryable: true,
		},
		{
			name: "internal error code",
			err: &NominalError{
				err:  errors.New("internal"),
				code: "INTERNAL",
			},
			retryable: true,
		},
		{
			name: "failed precondition error code",
			err: &NominalError{
				err:  errors.New("precondition"),
				code: "FAILED_PRECONDITION",
			},
			retryable: true,
		},
		{
			name: "bad request not retryable",
			err: &NominalError{
				err:        errors.New("bad request"),
				statusCode: 400,
			},
			retryable: false,
		},
		{
			name: "unauthorized not retryable",
			err: &NominalError{
				err:        errors.New("unauthorized"),
				statusCode: 401,
				code:       "UNAUTHORIZED",
			},
			retryable: false,
		},
		{
			name: "not found not retryable",
			err: &NominalError{
				err:        errors.New("not found"),
				statusCode: 404,
				code:       "NOT_FOUND",
			},
			retryable: false,
		},
		{
			name: "invalid argument not retryable",
			err: &NominalError{
				err:  errors.New("invalid"),
				code: "INVALID_ARGUMENT",
			},
			retryable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsRetryable(tc.err)
			if result != tc.retryable {
				t.Errorf("expected retryable=%v, got %v", tc.retryable, result)
			}
		})
	}
}

func TestWrapAPIError(t *testing.T) {
	t.Run("nil error returns nil", func(t *testing.T) {
		result := wrapAPIError(nil)
		if result != nil {
			t.Error("expected nil for nil input")
		}
	})

	t.Run("plain error is wrapped", func(t *testing.T) {
		plainErr := errors.New("plain error")
		result := wrapAPIError(plainErr)

		nomErr, ok := AsNominalError(result)
		if !ok {
			t.Fatal("expected NominalError wrapper")
		}

		// Plain error won't have conjure info
		if nomErr.Code() != "" {
			t.Errorf("expected empty code for plain error, got %s", nomErr.Code())
		}
		if !errors.Is(result, plainErr) {
			t.Error("wrapped error should contain original error")
		}
	})

	t.Run("conjure error extracts fields", func(t *testing.T) {
		// Create a real Conjure error
		conjureErr := cerrors.NewNotFound()
		result := wrapAPIError(conjureErr)

		nomErr, ok := AsNominalError(result)
		if !ok {
			t.Fatal("expected NominalError wrapper")
		}

		if nomErr.Code() != "NOT_FOUND" {
			t.Errorf("expected code NOT_FOUND, got %s", nomErr.Code())
		}
		if nomErr.InstanceID() == "" {
			t.Error("expected non-empty instanceID")
		}
	})
}

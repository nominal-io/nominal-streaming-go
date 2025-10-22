package nominal_streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  string
		opts    []Option
		wantErr bool
	}{
		{
			name:    "valid client with API key",
			apiKey:  "test-key",
			opts:    []Option{},
			wantErr: false,
		},
		{
			name:   "with custom base URL",
			apiKey: "test-key",
			opts: []Option{
				WithBaseURL("https://custom.api.com"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.apiKey, tt.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewClient() returned nil client")
			}
			if client != nil {
				client.Close()
			}
		})
	}
}

func TestNewStream(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test-dataset"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	if stream == nil {
		t.Error("NewStream() returned nil stream")
	}
}

func TestStream_EnqueueFloat(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Create a float channel stream and enqueue a data point
	cs := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A1"}))
	cs.Enqueue(time.Now().UnixNano(), 23.5)

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)
}

func TestStream_EnqueueString(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Create a string channel stream and enqueue a data point
	cs := stream.StringStream("status", WithTags(Tags{"device": "D1"}))
	cs.Enqueue(time.Now().UnixNano(), "OK")

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)
}

func TestStream_BatchingBySizeThreshold(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Create a channel stream and send multiple data points to trigger size-based flush (default is 1000)
	cs := stream.FloatStream("test_channel", WithTags(Tags{"batch": "size_test"}))
	baseTime := time.Now().UnixNano()
	for i := 0; i < 1005; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
	}

	// Give it time to flush
	time.Sleep(200 * time.Millisecond)
}

func TestStream_BatchingByTimeThreshold(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Create a channel stream and send a few data points
	cs := stream.FloatStream("test_channel", WithTags(Tags{"batch": "time_test"}))
	baseTime := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
	}

	// Wait for time-based flush (5 seconds + buffer)
	time.Sleep(6 * time.Second)
}

func TestStream_MixedDataTypes(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	baseTime := time.Now().UnixNano()

	// Create channel streams for different channels with specific types
	tempCS := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A1"}))
	statusCS := stream.StringStream("status", WithTags(Tags{"device": "D1"}))

	// Send float data points
	for i := 0; i < 5; i++ {
		tempCS.Enqueue(baseTime+int64(i*1_000_000_000), 20.0+float64(i))
	}

	// Send string data points
	statuses := []string{"OK", "WARNING", "ERROR", "OK", "OK"}
	for i, status := range statuses {
		statusCS.Enqueue(baseTime+int64(i*1_000_000_000), status)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
}

func TestStream_DifferentTagSets(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	baseTime := time.Now().UnixNano()

	// Create channel streams with different tag sets (should create separate batches)
	csNorth := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A1", "location": "north"}))
	csSouth := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A2", "location": "south"}))

	// Send data points with different tag sets
	for i := 0; i < 5; i++ {
		csNorth.Enqueue(baseTime+int64(i*1_000_000_000), 20.0+float64(i))
		csSouth.Enqueue(baseTime+int64(i*1_000_000_000), 25.0+float64(i))
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
}

func TestStream_Close(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send some data
	cs := stream.FloatStream("test", WithTags(Tags{"test": "close"}))
	cs.Enqueue(time.Now().UnixNano(), 42.0)

	// Close should flush remaining data
	if err := stream.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestRetryLogic_Success(t *testing.T) {
	// Create a mock server that succeeds immediately
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Send data - should succeed without retries
	cs := stream.FloatStream("test")
	cs.Enqueue(time.Now().UnixNano(), 42.0)
	time.Sleep(600 * time.Millisecond) // Wait for flush
}

func TestRetryLogic_RetryableError_EventualSuccess(t *testing.T) {
	var attemptCount atomic.Int32

	// Create a mock server that fails twice with 503, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attemptCount.Add(1)
		if count <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503
			w.Write([]byte("service unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Send data - should succeed after retries
	cs := stream.FloatStream("test")
	cs.Enqueue(time.Now().UnixNano(), 42.0)
	time.Sleep(2 * time.Second) // Wait for retries and flush

	// Verify we attempted 3 times (2 failures + 1 success)
	if attempts := attemptCount.Load(); attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryLogic_RetryableError_MaxRetriesExceeded(t *testing.T) {
	var attemptCount atomic.Int32

	// Create a mock server that always fails with 503
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable) // 503
		w.Write([]byte("service unavailable"))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Capture errors
	errorReceived := false
	go func() {
		for range stream.Errors() {
			errorReceived = true
		}
	}()

	// Send data - should fail after max retries
	cs := stream.FloatStream("test")
	cs.Enqueue(time.Now().UnixNano(), 42.0)
	time.Sleep(3 * time.Second) // Wait for retries and flush

	// Verify we attempted maxRetries + 1 times (default is 3 retries = 4 total attempts)
	if attempts := attemptCount.Load(); attempts != 4 {
		t.Errorf("expected 4 attempts (1 initial + 3 retries), got %d", attempts)
	}

	if !errorReceived {
		t.Error("expected error to be reported after max retries exceeded")
	}
}

func TestRetryLogic_NonRetryableError(t *testing.T) {
	var attemptCount atomic.Int32

	// Create a mock server that fails with 400 (non-retryable)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount.Add(1)
		w.WriteHeader(http.StatusBadRequest) // 400
		w.Write([]byte("bad request"))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Capture errors
	errorReceived := false
	go func() {
		for range stream.Errors() {
			errorReceived = true
		}
	}()

	// Send data - should fail immediately without retries
	cs := stream.FloatStream("test")
	cs.Enqueue(time.Now().UnixNano(), 42.0)
	time.Sleep(600 * time.Millisecond) // Wait for flush

	// Verify we only attempted once (no retries for 400)
	if attempts := attemptCount.Load(); attempts != 1 {
		t.Errorf("expected 1 attempt (no retries for 400), got %d", attempts)
	}

	if !errorReceived {
		t.Error("expected error to be reported for non-retryable error")
	}
}

func TestRetryLogic_RateLimitRetry(t *testing.T) {
	var attemptCount atomic.Int32

	// Create a mock server that fails once with 429, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attemptCount.Add(1)
		if count == 1 {
			w.WriteHeader(http.StatusTooManyRequests) // 429
			w.Write([]byte("rate limited"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Send data - should succeed after retry
	cs := stream.FloatStream("test")
	cs.Enqueue(time.Now().UnixNano(), 42.0)
	time.Sleep(1 * time.Second) // Wait for retry and flush

	// Verify we attempted 2 times (1 failure + 1 success)
	if attempts := attemptCount.Load(); attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestIsRetryableStatusCode(t *testing.T) {
	tests := []struct {
		statusCode int
		expected   bool
	}{
		{http.StatusOK, false},                 // 200
		{http.StatusBadRequest, false},         // 400
		{http.StatusUnauthorized, false},       // 401
		{http.StatusForbidden, false},          // 403
		{http.StatusNotFound, false},           // 404
		{http.StatusTooManyRequests, true},     // 429
		{http.StatusInternalServerError, true}, // 500
		{http.StatusBadGateway, true},          // 502
		{http.StatusServiceUnavailable, true},  // 503
		{http.StatusGatewayTimeout, true},      // 504
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.statusCode), func(t *testing.T) {
			result := isRetryableStatusCode(tt.statusCode)
			if result != tt.expected {
				t.Errorf("isRetryableStatusCode(%d) = %v, expected %v", tt.statusCode, result, tt.expected)
			}
		})
	}
}

func TestRetryConfig_Defaults(t *testing.T) {
	config := defaultRetryConfig()
	if config.maxRetries != 3 {
		t.Errorf("expected maxRetries=3, got %d", config.maxRetries)
	}
	if config.initialBackoff != 100*time.Millisecond {
		t.Errorf("expected initialBackoff=100ms, got %v", config.initialBackoff)
	}
	if config.maxBackoff != 10*time.Second {
		t.Errorf("expected maxBackoff=10s, got %v", config.maxBackoff)
	}
	if config.backoffFactor != 2.0 {
		t.Errorf("expected backoffFactor=2.0, got %f", config.backoffFactor)
	}
}

func TestCalculateNextBackoff(t *testing.T) {
	config := defaultRetryConfig()

	tests := []struct {
		name     string
		current  time.Duration
		expected time.Duration
	}{
		{"first backoff", 100 * time.Millisecond, 200 * time.Millisecond},
		{"second backoff", 200 * time.Millisecond, 400 * time.Millisecond},
		{"third backoff", 400 * time.Millisecond, 800 * time.Millisecond},
		{"capped at max", 6 * time.Second, 10 * time.Second},
		{"exceeds max", 10 * time.Second, 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateNextBackoff(config, tt.current)
			if result != tt.expected {
				t.Errorf("calculateNextBackoff(%v) = %v, expected %v", tt.current, result, tt.expected)
			}
		})
	}
}

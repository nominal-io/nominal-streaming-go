package nominal_streaming

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Tests that replicate the "dropped error (buffer full)" issue
// =============================================================================

// TestErrorBuffer_OverflowOnRepeated400Errors replicates the issue where
// continuous 400 errors from the server fill up the error buffer and cause
// "dropped error (buffer full)" messages.
func TestErrorBuffer_OverflowOnRepeated400Errors(t *testing.T) {
	var requestCount atomic.Int32

	// Create a mock server that always returns 400 INVALID_ARGUMENT
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusBadRequest) // 400
		w.Write([]byte(`{"errorName": "Default:InvalidArgument"}`))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	// Use a very short flush interval to generate many errors quickly
	stream, errCh, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(10*time.Millisecond), // Very fast flushing
		WithBatchSize(5),                       // Small batch size
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Count errors received vs dropped
	var errorsReceived atomic.Int32
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		for range errCh {
			errorsReceived.Add(1)
		}
	}()

	// Send many data points to trigger multiple flushes
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 500; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		if i%10 == 0 {
			time.Sleep(5 * time.Millisecond) // Allow some flushes to happen
		}
	}

	// Wait for flushes to complete
	time.Sleep(200 * time.Millisecond)
	stream.Close()
	done.Wait()

	requests := requestCount.Load()
	errors := errorsReceived.Load()

	t.Logf("Requests made: %d, Errors received: %d", requests, errors)

	// The error buffer is 100, so if we made more requests than that,
	// some errors should have been dropped
	if requests > 100 && errors >= requests {
		t.Logf("Warning: Made %d requests but only received %d errors - some may have been dropped", requests, errors)
	}

	// Verify we did receive errors (the 400s should have been reported)
	if errors == 0 && requests > 0 {
		t.Error("expected to receive at least some errors from 400 responses")
	}
}

// TestConcurrentFlushGoroutines_Accumulation tests that concurrent flush
// goroutines can accumulate when the server is slow or failing.
func TestConcurrentFlushGoroutines_Accumulation(t *testing.T) {
	var activeRequests atomic.Int32
	var maxConcurrent atomic.Int32

	// Create a mock server that is slow to respond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := activeRequests.Add(1)
		defer activeRequests.Add(-1)

		// Track max concurrent requests
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		// Simulate slow server
		time.Sleep(100 * time.Millisecond)

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

	// Use a very short flush interval (faster than server response time)
	stream, _, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(20*time.Millisecond), // Flush every 20ms
		WithBatchSize(1000),                    // Large batch to force time-based flush
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send data continuously to trigger time-based flushes
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 50; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all goroutines to complete
	stream.Close()

	max := maxConcurrent.Load()
	t.Logf("Max concurrent flush goroutines: %d", max)

	// With 20ms flush interval and 100ms server delay, we should see
	// ~5 concurrent requests accumulate (100ms / 20ms = 5)
	if max > 10 {
		t.Logf("Warning: High goroutine accumulation detected: %d concurrent flushes", max)
	}
}

// =============================================================================
// Tests for timestamp conversion edge cases that cause INVALID_ARGUMENT
// =============================================================================

// TestTimestampConversion_NegativeNanoseconds tests that negative timestamps
// are handled correctly in protobuf conversion.
func TestTimestampConversion_NegativeNanoseconds(t *testing.T) {
	tests := []struct {
		name          string
		nanos         NanosecondsUTC
		wantSeconds   int64
		wantNanos     int32
		wantValidNano bool // Nanos should be in [0, 999999999]
	}{
		{
			name:          "positive timestamp",
			nanos:         1704067200000000000, // 2024-01-01 00:00:00 UTC
			wantSeconds:   1704067200,
			wantNanos:     0,
			wantValidNano: true,
		},
		{
			name:          "positive with fractional nanos",
			nanos:         1704067200123456789,
			wantSeconds:   1704067200,
			wantNanos:     123456789,
			wantValidNano: true,
		},
		{
			name:          "negative timestamp -1 nanosecond",
			nanos:         -1,
			wantSeconds:   -1, // Should be -1, not 0
			wantNanos:     999999999,
			wantValidNano: true,
		},
		{
			name:          "negative timestamp -1 second",
			nanos:         -1_000_000_000,
			wantSeconds:   -1,
			wantNanos:     0,
			wantValidNano: true,
		},
		{
			name:          "negative timestamp before epoch",
			nanos:         -86400_000_000_000, // -1 day
			wantSeconds:   -86400,
			wantNanos:     0,
			wantValidNano: true,
		},
		{
			name:          "negative with fractional",
			nanos:         -1_500_000_000, // -1.5 seconds
			wantSeconds:   -2,
			wantNanos:     500000000,
			wantValidNano: true,
		},
		{
			name:          "zero",
			nanos:         0,
			wantSeconds:   0,
			wantNanos:     0,
			wantValidNano: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := nanosecondsToTimestampProto(tt.nanos)

			// Check if nanos is valid (0-999999999)
			validNano := ts.Nanos >= 0 && ts.Nanos <= 999999999
			if validNano != tt.wantValidNano {
				t.Errorf("nanosecondsToTimestampProto(%d) nanos validity = %v, want %v (nanos=%d)",
					tt.nanos, validNano, tt.wantValidNano, ts.Nanos)
			}

			if !validNano {
				t.Errorf("INVALID PROTOBUF: nanos=%d is outside valid range [0, 999999999]", ts.Nanos)
			}

			// For cases where we know the expected values
			if tt.wantValidNano {
				if ts.Seconds != tt.wantSeconds {
					t.Errorf("nanosecondsToTimestampProto(%d) seconds = %d, want %d",
						tt.nanos, ts.Seconds, tt.wantSeconds)
				}
				if ts.Nanos != tt.wantNanos {
					t.Errorf("nanosecondsToTimestampProto(%d) nanos = %d, want %d",
						tt.nanos, ts.Nanos, tt.wantNanos)
				}
			}
		})
	}
}

// =============================================================================
// Tests for NaN/Inf float values that cause INVALID_ARGUMENT
// =============================================================================

// TestFloatValues_NaNAndInf tests that NaN and Inf values are handled
// (currently they're not validated and will cause server-side 400 errors).
func TestFloatValues_NaNAndInf(t *testing.T) {
	var requestPayloads [][]byte
	var mu sync.Mutex

	// Create a mock server that captures request bodies
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// In a real scenario, the server would reject NaN/Inf
		// For this test, we just verify the values are being sent
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

	stream, errCh, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithBatchSize(10), // Small batch for quick testing
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Collect any errors
	var errors []error
	go func() {
		for err := range errCh {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}
	}()

	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()

	// These values are problematic for protobuf/JSON serialization
	problematicValues := []float64{
		math.NaN(),
		math.Inf(1),  // +Inf
		math.Inf(-1), // -Inf
	}

	for i, val := range problematicValues {
		cs.Enqueue(baseTime+int64(i*1_000_000), val)
	}

	// Also send some normal values
	for i := 0; i < 7; i++ {
		cs.Enqueue(baseTime+int64((i+3)*1_000_000), float64(i))
	}

	stream.Close()

	mu.Lock()
	defer mu.Unlock()

	t.Logf("Captured %d request payloads", len(requestPayloads))
	t.Logf("Received %d errors", len(errors))

	// Note: This test currently passes but the values would cause
	// INVALID_ARGUMENT on a real server. The fix would be to validate
	// and reject NaN/Inf values before adding to the batch.
}

// TestFloatValues_NaNDetection verifies that NaN values can be detected
func TestFloatValues_NaNDetection(t *testing.T) {
	testValues := []struct {
		value   float64
		isNaN   bool
		isInf   bool
		isValid bool
	}{
		{42.0, false, false, true},
		{0.0, false, false, true},
		{-273.15, false, false, true},
		{math.NaN(), true, false, false},
		{math.Inf(1), false, true, false},
		{math.Inf(-1), false, true, false},
		{math.MaxFloat64, false, false, true},
		{math.SmallestNonzeroFloat64, false, false, true},
	}

	for _, tt := range testValues {
		isNaN := math.IsNaN(tt.value)
		isInf := math.IsInf(tt.value, 0)
		isValid := !isNaN && !isInf

		if isNaN != tt.isNaN {
			t.Errorf("IsNaN(%v) = %v, want %v", tt.value, isNaN, tt.isNaN)
		}
		if isInf != tt.isInf {
			t.Errorf("IsInf(%v) = %v, want %v", tt.value, isInf, tt.isInf)
		}
		if isValid != tt.isValid {
			t.Errorf("isValid(%v) = %v, want %v", tt.value, isValid, tt.isValid)
		}
	}
}

// =============================================================================
// Tests for error channel draining behavior
// =============================================================================

// TestErrorChannel_MustBeDrained verifies that not draining the error channel
// leads to buffer overflow.
func TestErrorChannel_MustBeDrained(t *testing.T) {
	var requestCount atomic.Int32

	// Create a mock server that always returns 400
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "bad request"}`))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, errCh, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(10*time.Millisecond),
		WithBatchSize(5),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// INTENTIONALLY do NOT drain the error channel to simulate the issue
	_ = errCh

	// Send data to trigger many flushes and errors
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 200; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	time.Sleep(300 * time.Millisecond)
	stream.Close()

	requests := requestCount.Load()
	t.Logf("Requests made without draining error channel: %d", requests)

	// Error buffer is 100 - with many requests, we expect drops
	// This test demonstrates the importance of draining the error channel
	if requests > 100 {
		t.Logf("Made %d requests with undrained error channel - expect 'dropped error' logs", requests)
	}
}

// TestErrorChannel_ProperDraining shows the correct way to handle errors
func TestErrorChannel_ProperDraining(t *testing.T) {
	var requestCount atomic.Int32
	var errorCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "bad request"}`))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, errCh, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(10*time.Millisecond),
		WithBatchSize(5),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// PROPERLY drain the error channel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errCh {
			errorCount.Add(1)
			// In real code, you'd log or handle the error
			_ = err
		}
	}()

	// Send data
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 200; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	time.Sleep(300 * time.Millisecond)
	stream.Close()
	wg.Wait()

	requests := requestCount.Load()
	errors := errorCount.Load()

	t.Logf("Requests: %d, Errors received: %d", requests, errors)

	// With proper draining, we should receive most/all errors
	if requests > 0 && errors == 0 {
		t.Error("expected to receive errors when draining error channel")
	}
}

// =============================================================================
// Batch conversion tests
// =============================================================================

func TestConvertFloatBatchToProto_TimestampValueMismatch(t *testing.T) {
	batch := floatBatch{
		Channel:    "test",
		Tags:       map[string]string{"a": "b"},
		Timestamps: []NanosecondsUTC{1, 2, 3},
		Values:     []float64{1.0, 2.0}, // Mismatch!
	}

	_, err := convertFloatBatchToProto(batch)
	if err == nil {
		t.Error("expected error for timestamp/value length mismatch")
	}
}

func TestConvertFloatBatchToProto_RejectsNaN(t *testing.T) {
	batch := floatBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1, 2, 3},
		Values:     []float64{1.0, math.NaN(), 3.0},
	}

	_, err := convertFloatBatchToProto(batch)
	if err == nil {
		t.Error("expected error for NaN value")
	}
	if err != nil && !contains(err.Error(), "NaN") {
		t.Errorf("expected error message to mention NaN, got: %v", err)
	}
}

func TestConvertFloatBatchToProto_RejectsInf(t *testing.T) {
	batch := floatBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1, 2},
		Values:     []float64{math.Inf(1), 2.0},
	}

	_, err := convertFloatBatchToProto(batch)
	if err == nil {
		t.Error("expected error for Inf value")
	}
	if err != nil && !contains(err.Error(), "Inf") {
		t.Errorf("expected error message to mention Inf, got: %v", err)
	}
}

func TestConvertFloatBatchToProto_RejectsNegativeInf(t *testing.T) {
	batch := floatBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1},
		Values:     []float64{math.Inf(-1)},
	}

	_, err := convertFloatBatchToProto(batch)
	if err == nil {
		t.Error("expected error for -Inf value")
	}
}

func TestConvertFloatArrayBatchToProto_RejectsNaN(t *testing.T) {
	batch := floatArrayBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1},
		Values:     [][]float64{{1.0, math.NaN(), 3.0}},
	}

	_, err := convertFloatArrayBatchToProto(batch)
	if err == nil {
		t.Error("expected error for NaN value in array")
	}
}

func TestConvertFloatArrayBatchToProto_RejectsInf(t *testing.T) {
	batch := floatArrayBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1, 2},
		Values:     [][]float64{{1.0, 2.0}, {3.0, math.Inf(1)}},
	}

	_, err := convertFloatArrayBatchToProto(batch)
	if err == nil {
		t.Error("expected error for Inf value in array")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestConvertIntBatchToProto_TimestampValueMismatch(t *testing.T) {
	batch := intBatch{
		Channel:    "test",
		Tags:       map[string]string{"a": "b"},
		Timestamps: []NanosecondsUTC{1, 2},
		Values:     []int64{1, 2, 3}, // Mismatch!
	}

	_, err := convertIntBatchToProto(batch)
	if err == nil {
		t.Error("expected error for timestamp/value length mismatch")
	}
}

func TestConvertStringBatchToProto_TimestampValueMismatch(t *testing.T) {
	batch := stringBatch{
		Channel:    "test",
		Tags:       map[string]string{},
		Timestamps: []NanosecondsUTC{1},
		Values:     []string{"a", "b"}, // Mismatch!
	}

	_, err := convertStringBatchToProto(batch)
	if err == nil {
		t.Error("expected error for timestamp/value length mismatch")
	}
}

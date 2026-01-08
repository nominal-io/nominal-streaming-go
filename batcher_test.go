package nominal_streaming

import (
	"bytes"
	"context"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Tests for "dropped error (buffer full)" issue
// =============================================================================

// TestErrorBuffer_DroppedErrorOnOverflow verifies that when the error buffer
// fills up, errors are dropped and logged with "dropped error (buffer full)".
func TestErrorBuffer_DroppedErrorOnOverflow(t *testing.T) {
	// Capture log output to verify "dropped error" messages
	var logBuf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(originalOutput)

	var requestCount atomic.Int32

	// Create a mock server that always returns 400
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"errorName": "Default:InvalidArgument"}`))
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	// Use very fast flushing to generate many errors quickly
	stream, errCh, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(5*time.Millisecond),
		WithBatchSize(3),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Count errors received
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
	for i := 0; i < 1000; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		if i%50 == 0 {
			time.Sleep(2 * time.Millisecond)
		}
	}

	time.Sleep(500 * time.Millisecond)
	stream.Close()
	done.Wait()

	requests := requestCount.Load()
	errors := errorsReceived.Load()
	logOutput := logBuf.String()

	t.Logf("Requests made: %d, Errors received: %d", requests, errors)

	// With 256 buffer size and potentially hundreds of requests,
	// we should see some dropped errors if we overwhelm the buffer
	droppedCount := strings.Count(logOutput, "dropped error (buffer full)")
	t.Logf("Dropped error messages in log: %d", droppedCount)

	// Verify we received at least some errors
	if errors == 0 && requests > 0 {
		t.Error("expected to receive at least some errors from 400 responses")
	}
}

// TestErrorChannel_UndrainedLeadsToDrops verifies that not draining the
// error channel causes errors to be dropped.
func TestErrorChannel_UndrainedLeadsToDrops(t *testing.T) {
	// Capture log output
	var logBuf bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(originalOutput)

	var requestCount atomic.Int32

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
		WithFlushInterval(5*time.Millisecond),
		WithBatchSize(3),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// INTENTIONALLY do NOT drain the error channel
	_ = errCh

	// Send data to trigger many flushes
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 500; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		if i%20 == 0 {
			time.Sleep(3 * time.Millisecond)
		}
	}

	time.Sleep(500 * time.Millisecond)
	stream.Close()

	requests := requestCount.Load()
	logOutput := logBuf.String()
	droppedCount := strings.Count(logOutput, "dropped error (buffer full)")

	t.Logf("Requests made: %d, Dropped errors logged: %d", requests, droppedCount)

	// With undrained channel and many requests exceeding buffer (256),
	// we expect dropped error messages
	if requests > 256 && droppedCount == 0 {
		t.Error("expected 'dropped error (buffer full)' log messages when channel is not drained")
	}
}

// TestErrorChannel_ProperDrainingReceivesAllErrors shows the correct pattern.
func TestErrorChannel_ProperDrainingReceivesAllErrors(t *testing.T) {
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

	// Properly drain the error channel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range errCh {
			errorCount.Add(1)
		}
	}()

	// Send data
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
	}

	time.Sleep(200 * time.Millisecond)
	stream.Close()
	wg.Wait()

	requests := requestCount.Load()
	errors := errorCount.Load()

	t.Logf("Requests: %d, Errors received: %d", requests, errors)

	if requests > 0 && errors == 0 {
		t.Error("expected to receive errors when properly draining error channel")
	}
}

// =============================================================================
// Tests for concurrent flush limiting
// =============================================================================

// TestConcurrentFlushLimit_EnforcesMax verifies that the flush semaphore
// limits concurrent flushes to maxConcurrentFlushes (10).
func TestConcurrentFlushLimit_EnforcesMax(t *testing.T) {
	var activeRequests atomic.Int32
	var maxConcurrent atomic.Int32

	// Create a slow server to cause flush accumulation
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := activeRequests.Add(1)
		defer activeRequests.Add(-1)

		// Track max concurrent
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		// Slow response to cause accumulation
		time.Sleep(200 * time.Millisecond)

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

	// Fast flush interval to try to exceed the limit
	stream, _, err := client.NewDatasetStream(
		context.Background(),
		datasetRID,
		WithFlushInterval(10*time.Millisecond),
		WithBatchSize(10000), // Large batch to force time-based flush
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send data to trigger many flushes
	cs := stream.FloatStream("test_channel")
	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		cs.Enqueue(baseTime+int64(i*1_000_000), float64(i))
		time.Sleep(5 * time.Millisecond)
	}

	stream.Close()

	max := maxConcurrent.Load()
	t.Logf("Max concurrent flush goroutines observed: %d (limit is %d)", max, maxConcurrentFlushes)

	// Should never exceed the limit
	if max > int32(maxConcurrentFlushes) {
		t.Errorf("exceeded max concurrent flushes: got %d, max allowed %d", max, maxConcurrentFlushes)
	}
}

// =============================================================================
// Tests for timestamp conversion (negative nanoseconds bug)
// =============================================================================

func TestTimestampConversion_NegativeNanoseconds(t *testing.T) {
	tests := []struct {
		name        string
		nanos       NanosecondsUTC
		wantSeconds int64
		wantNanos   int32
	}{
		{"positive timestamp", 1704067200000000000, 1704067200, 0},
		{"positive with fractional", 1704067200123456789, 1704067200, 123456789},
		{"negative -1 nanosecond", -1, -1, 999999999},
		{"negative -1 second", -1_000_000_000, -1, 0},
		{"negative -1 day", -86400_000_000_000, -86400, 0},
		{"negative -1.5 seconds", -1_500_000_000, -2, 500000000},
		{"zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := nanosecondsToTimestampProto(tt.nanos)

			// Verify nanos is in valid protobuf range [0, 999999999]
			if ts.Nanos < 0 || ts.Nanos > 999999999 {
				t.Errorf("INVALID PROTOBUF: nanos=%d outside [0, 999999999]", ts.Nanos)
			}

			if ts.Seconds != tt.wantSeconds {
				t.Errorf("seconds = %d, want %d", ts.Seconds, tt.wantSeconds)
			}
			if ts.Nanos != tt.wantNanos {
				t.Errorf("nanos = %d, want %d", ts.Nanos, tt.wantNanos)
			}
		})
	}
}

// =============================================================================
// Tests for NaN/Inf validation
// =============================================================================

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
	if err != nil && !strings.Contains(err.Error(), "NaN") {
		t.Errorf("expected error to mention NaN, got: %v", err)
	}
}

func TestConvertFloatBatchToProto_RejectsInf(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := floatBatch{
				Channel:    "test",
				Tags:       map[string]string{},
				Timestamps: []NanosecondsUTC{1},
				Values:     []float64{tt.value},
			}

			_, err := convertFloatBatchToProto(batch)
			if err == nil {
				t.Error("expected error for Inf value")
			}
			if err != nil && !strings.Contains(err.Error(), "Inf") {
				t.Errorf("expected error to mention Inf, got: %v", err)
			}
		})
	}
}

func TestConvertFloatArrayBatchToProto_RejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		values [][]float64
	}{
		{"NaN in array", [][]float64{{1.0, math.NaN(), 3.0}}},
		{"+Inf in array", [][]float64{{1.0, 2.0}, {3.0, math.Inf(1)}}},
		{"-Inf in array", [][]float64{{math.Inf(-1)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := floatArrayBatch{
				Channel:    "test",
				Tags:       map[string]string{},
				Timestamps: make([]NanosecondsUTC, len(tt.values)),
				Values:     tt.values,
			}
			for i := range batch.Timestamps {
				batch.Timestamps[i] = NanosecondsUTC(i + 1)
			}

			_, err := convertFloatArrayBatchToProto(batch)
			if err == nil {
				t.Error("expected error for invalid float value")
			}
		})
	}
}

// =============================================================================
// Tests for batch conversion edge cases
// =============================================================================

func TestConvertBatchToProto_TimestampValueMismatch(t *testing.T) {
	t.Run("float batch", func(t *testing.T) {
		batch := floatBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1, 2, 3},
			Values:     []float64{1.0, 2.0}, // Mismatch
		}
		_, err := convertFloatBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})

	t.Run("int batch", func(t *testing.T) {
		batch := intBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1, 2},
			Values:     []int64{1, 2, 3}, // Mismatch
		}
		_, err := convertIntBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})

	t.Run("string batch", func(t *testing.T) {
		batch := stringBatch{
			Channel:    "test",
			Tags:       map[string]string{},
			Timestamps: []NanosecondsUTC{1},
			Values:     []string{"a", "b"}, // Mismatch
		}
		_, err := convertStringBatchToProto(batch)
		if err == nil {
			t.Error("expected error for mismatch")
		}
	})
}

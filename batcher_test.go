package nominal_streaming

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"context"
)

// =============================================================================
// Tests for concurrent flush limiting (validates the fix)
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
	t.Logf("Max concurrent flush goroutines observed: %d (limit is %d)", max, defaultMaxConcurrentFlushes)

	// Should never exceed the limit
	if max > int32(defaultMaxConcurrentFlushes) {
		t.Errorf("exceeded max concurrent flushes: got %d, max allowed %d", max, defaultMaxConcurrentFlushes)
	}
}

// TestConcurrentFlushLimit_Configurable verifies the limit can be configured.
func TestConcurrentFlushLimit_Configurable(t *testing.T) {
	var maxConcurrent atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := maxConcurrent.Add(1)
		defer maxConcurrent.Add(-1)

		// Track max (simplified)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
		_ = current
	}))
	defer server.Close()

	client, err := NewClient("test-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	customLimit := 3
	stream, _, err := client.NewDatasetStream(
		context.Background(),
		"ri.nominal.main.dataset.test",
		WithFlushInterval(10*time.Millisecond),
		WithBatchSize(10000),
		WithMaxConcurrentFlushes(customLimit),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Verify the batcher has the custom limit
	if stream.batcher.maxConcurrentFlushes != customLimit {
		t.Errorf("maxConcurrentFlushes = %d, want %d", stream.batcher.maxConcurrentFlushes, customLimit)
	}

	stream.Close()
}

// =============================================================================
// Tests for timestamp conversion (validates the negative nanoseconds fix)
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
// Tests for NaN/Inf validation (validates the fix)
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
// Tests for backpressure policy configuration
// =============================================================================

func TestBackpressurePolicy_Configurable(t *testing.T) {
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

	// Test default policy
	stream1, _, err := client.NewDatasetStream(context.Background(), "ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	if stream1.batcher.backpressurePolicy != BackpressureRequeue {
		t.Errorf("default backpressurePolicy = %v, want BackpressureRequeue", stream1.batcher.backpressurePolicy)
	}
	stream1.Close()

	// Test configuring DropBatch policy
	stream2, _, err := client.NewDatasetStream(
		context.Background(),
		"ri.nominal.main.dataset.test",
		WithBackpressurePolicy(BackpressureDropBatch),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	if stream2.batcher.backpressurePolicy != BackpressureDropBatch {
		t.Errorf("backpressurePolicy = %v, want BackpressureDropBatch", stream2.batcher.backpressurePolicy)
	}
	stream2.Close()
}

func TestMaxBufferPoints_Configurable(t *testing.T) {
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

	// Test default
	stream1, _, err := client.NewDatasetStream(context.Background(), "ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	if stream1.batcher.maxBufferPoints != defaultMaxBufferPoints {
		t.Errorf("default maxBufferPoints = %d, want %d", stream1.batcher.maxBufferPoints, defaultMaxBufferPoints)
	}
	stream1.Close()

	// Test custom value
	customLimit := 50000
	stream2, _, err := client.NewDatasetStream(
		context.Background(),
		"ri.nominal.main.dataset.test",
		WithMaxBufferPoints(customLimit),
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	if stream2.batcher.maxBufferPoints != customLimit {
		t.Errorf("maxBufferPoints = %d, want %d", stream2.batcher.maxBufferPoints, customLimit)
	}
	stream2.Close()
}

func TestEnforceBufferLimit_DropsOldestPoints(t *testing.T) {
	// Create a batcher with a small buffer limit
	b := &batcher{
		maxBufferPoints: 5,
		floatBuffers:    make(map[channelReferenceKey]*floatBuffer),
		totalPoints:     0,
	}

	// Add 10 points - should trigger overflow handling when enforceBufferLimitLocked is called
	key := channelReferenceKey{channel: "test", tagsHash: ""}
	b.floatBuffers[key] = &floatBuffer{
		ref:        channelReference{channelReferenceKey: key, tags: nil},
		timestamps: []NanosecondsUTC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		values:     []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
	}
	b.totalPoints = 10

	// Enforce the limit
	b.enforceBufferLimitLocked()

	// Should have dropped 5 oldest points
	if b.totalPoints != 5 {
		t.Errorf("totalPoints = %d, want 5", b.totalPoints)
	}

	buffer := b.floatBuffers[key]
	if len(buffer.timestamps) != 5 {
		t.Errorf("buffer.timestamps length = %d, want 5", len(buffer.timestamps))
	}

	// Oldest points should be dropped, newest kept (6, 7, 8, 9, 10)
	if buffer.timestamps[0] != 6 {
		t.Errorf("first remaining timestamp = %d, want 6 (oldest dropped)", buffer.timestamps[0])
	}
	if buffer.values[0] != 6.0 {
		t.Errorf("first remaining value = %f, want 6.0", buffer.values[0])
	}
}

func TestCountBatchPoints(t *testing.T) {
	floatBatches := []floatBatch{
		{Timestamps: make([]NanosecondsUTC, 5)},
		{Timestamps: make([]NanosecondsUTC, 3)},
	}
	intBatches := []intBatch{
		{Timestamps: make([]NanosecondsUTC, 2)},
	}

	total := countBatchPoints(floatBatches, intBatches, nil, nil, nil)
	if total != 10 {
		t.Errorf("countBatchPoints = %d, want 10", total)
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

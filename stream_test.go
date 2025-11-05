package nominal_streaming

import (
	"context"
	"testing"
	"time"
)

func TestGetChannelStream_ReturnsSameInstance(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, _, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	// Get a channel stream twice with same parameters
	cs1 := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A1"}))
	cs2 := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A1"}))

	// They should be the exact same instance
	if cs1 != cs2 {
		t.Error("FloatStream should return the same instance for identical parameters")
	}

	// Different tags should return different instance
	cs3 := stream.FloatStream("temperature", WithTags(Tags{"sensor": "A2"}))
	if cs1 == cs3 {
		t.Error("FloatStream should return different instances for different tags")
	}

	// Different type should return different instance (even if same channel+tags)
	cs4 := stream.IntStream("temperature", WithTags(Tags{"sensor": "A1"}))
	if cs4 == nil {
		t.Error("IntStream should return a valid instance")
	}
}

func TestEnqueueDynamic_TypeDispatch(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, _, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	timestamp := time.Now().UnixNano()

	tests := []struct {
		name      string
		value     any
		wantError bool
	}{
		{"float64", 23.5, false},
		{"int64", int64(42), false},
		{"string", "OK", false},
		{"[]float64", []float64{1.0, 2.0, 3.0}, false},
		{"[]string", []string{"a", "b", "c"}, false},
		{"unsupported_bool", true, true},
		{"unsupported_[]int", []int{1, 2, 3}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stream.EnqueueDynamic("test_channel", timestamp, tt.value)
			if (err != nil) != tt.wantError {
				t.Errorf("EnqueueDynamic() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestEnqueueDynamic_WithTags(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, _, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	timestamp := time.Now().UnixNano()
	tags := Tags{"sensor": "A1", "location": "north"}

	err = stream.EnqueueDynamic("temperature", timestamp, 23.5, WithTags(tags))
	if err != nil {
		t.Errorf("EnqueueDynamic() with tags error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

func TestArrayStreams(t *testing.T) {
	client, err := NewClient("test-key")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	datasetRID := "ri.nominal.main.dataset.test"

	stream, _, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	timestamp := time.Now().UnixNano()

	// Test FloatArrayStream
	floatArrayStream := stream.FloatArrayStream("sensor_readings")
	floatArrayStream.Enqueue(timestamp, []float64{1.1, 2.2, 3.3, 4.4})
	floatArrayStream.Enqueue(timestamp+1000, []float64{5.5, 6.6})

	// Test StringArrayStream
	stringArrayStream := stream.StringArrayStream("status_codes")
	stringArrayStream.Enqueue(timestamp, []string{"OK", "WARN", "ERROR"})
	stringArrayStream.Enqueue(timestamp+1000, []string{"OK"})

	// Test with tags
	taggedFloatArray := stream.FloatArrayStream("measurements", WithTags(Tags{"sensor": "A1"}))
	taggedFloatArray.Enqueue(timestamp, []float64{10.0, 20.0, 30.0})

	// Verify same instance is returned
	floatArrayStream2 := stream.FloatArrayStream("sensor_readings")
	if floatArrayStream != floatArrayStream2 {
		t.Error("FloatArrayStream should return the same instance for identical parameters")
	}

	time.Sleep(100 * time.Millisecond)
}

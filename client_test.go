package nominal_streaming

import (
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test-dataset")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.test")
	if err != nil {
		t.Fatalf("failed to parse dataset RID: %v", err)
	}

	stream, err := client.NewStream(datasetRID)
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

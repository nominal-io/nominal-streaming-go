package nominal_streaming

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	writerapi "github.com/nominal-io/nominal-api-go/storage/writer/api"
)

func TestClient_IntegrationWithMockServer(t *testing.T) {
	// Track received requests
	var mu sync.Mutex
	var receivedRequests []writerapi.WriteBatchesRequestExternal

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer test-api-key" {
			t.Errorf("Expected Authorization header 'Bearer test-api-key', got %q", authHeader)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Verify path (conjure-generated client uses /storage/writer/v1)
		if r.URL.Path != "/storage/writer/v1" {
			t.Errorf("Expected path '/storage/writer/v1', got %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// Parse request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var req writerapi.WriteBatchesRequestExternal
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("Failed to unmarshal request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Store request
		mu.Lock()
		receivedRequests = append(receivedRequests, req)
		mu.Unlock()

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	// Create client pointing to mock server
	client, err := NewClient("test-api-key", WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Parse dataset RID
	datasetRID, err := ParseDatasetRID("ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc")
	if err != nil {
		t.Fatalf("Failed to parse dataset RID: %v", err)
	}

	// Create stream with short flush interval for quick testing
	flushInterval := 20 * time.Millisecond
	stream, err := client.NewDatasetStream(context.Background(), datasetRID, WithFlushInterval(flushInterval))
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Setup error handler
	errorChan := make(chan error, 10)
	stream.ProcessErrors(func(err error) {
		errorChan <- err
	})

	baseTime := time.Now().UnixNano()

	// Send float data with tags
	floatStream := stream.FloatStream("temperature", WithTags(Tags{
		"sensor":   "A1",
		"location": "north",
	}))
	for i := 0; i < 3; i++ {
		floatStream.Enqueue(baseTime+int64(i*1_000_000_000), 20.0+float64(i))
	}

	// Send int data with different tags
	intStream := stream.IntStream("event_count", WithTags(Tags{
		"device": "counter_1",
	}))
	for i := 0; i < 2; i++ {
		intStream.Enqueue(baseTime+int64(i*1_000_000_000), int64(100+i*10))
	}

	// Send string data without tags
	stringStream := stream.StringStream("status")
	stringStream.Enqueue(baseTime, "OK")

	// Wait for time-based flush (flush interval + buffer)
	time.Sleep(flushInterval + 10*time.Millisecond)

	// Check for errors
	select {
	case err := <-errorChan:
		t.Fatalf("Unexpected error: %v", err)
	default:
	}

	// Verify we received at least one request
	mu.Lock()
	requestCount := len(receivedRequests)
	mu.Unlock()

	if requestCount == 0 {
		t.Fatal("Expected to receive at least one batch request, got none")
	}

	// Verify first request contents
	mu.Lock()
	firstReq := receivedRequests[0]
	mu.Unlock()

	if len(firstReq.Batches) == 0 {
		t.Fatal("Expected batches in request, got none")
	}

	// Verify we have batches (at least float and int, string might be in a separate request)
	batchCount := len(firstReq.Batches)
	if batchCount < 2 {
		t.Errorf("Expected at least 2 batches (float, int), got %d", batchCount)
	}

	// Verify dataset RID
	if firstReq.DataSourceRid.String() != datasetRID.String() {
		t.Errorf("Expected dataset RID %v, got %v", datasetRID, firstReq.DataSourceRid)
	}

	// Check that batches have channels
	for i, batch := range firstReq.Batches {
		if string(batch.Channel) == "" {
			t.Errorf("Batch %d missing channel name", i)
		}
		// Verify tags field exists (may be empty)
		_ = batch.Tags
		// Verify points field exists (it's a union type, always present)
		_ = batch.Points
	}

	t.Logf("Successfully received %d request(s) with %d batches", requestCount, batchCount)
}

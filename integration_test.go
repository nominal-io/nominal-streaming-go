package nominal_streaming

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/nominal-io/nominal-streaming/proto"
	"google.golang.org/protobuf/proto"
)

func TestClient_IntegrationWithMockServer(t *testing.T) {
	// Track received requests
	var mu sync.Mutex
	var receivedRequests []*pb.WriteRequestNominal

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer test-api-key" {
			t.Errorf("Expected Authorization header 'Bearer test-api-key', got %q", authHeader)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Verify path (protobuf endpoint uses /storage/writer/v1/nominal/{rid})
		expectedPathPrefix := "/storage/writer/v1/nominal/"
		if !strings.HasPrefix(r.URL.Path, expectedPathPrefix) {
			t.Errorf("Expected path to start with %q, got %q", expectedPathPrefix, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// Verify Content-Type header
		contentType := r.Header.Get("Content-Type")
		if contentType != "application/x-protobuf" {
			t.Errorf("Expected Content-Type 'application/x-protobuf', got %q", contentType)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Parse request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var req pb.WriteRequestNominal
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("Failed to unmarshal protobuf request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Store request
		mu.Lock()
		receivedRequests = append(receivedRequests, &req)
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

	datasetRID := "ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc"

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

	if len(firstReq.Series) == 0 {
		t.Fatal("Expected series in request, got none")
	}

	// Verify we have series (at least float and int, string might be in a separate request)
	seriesCount := len(firstReq.Series)
	if seriesCount < 2 {
		t.Errorf("Expected at least 2 series (float, int), got %d", seriesCount)
	}

	// Check that series have channels
	for i, series := range firstReq.Series {
		if series.Channel == nil || series.Channel.Name == "" {
			t.Errorf("Series %d missing channel name", i)
		}
		// Verify tags field exists (may be empty)
		_ = series.Tags
		// Verify points field exists
		if series.Points == nil {
			t.Errorf("Series %d missing points", i)
		}
	}

	t.Logf("Successfully received %d request(s) with %d series", requestCount, seriesCount)
}

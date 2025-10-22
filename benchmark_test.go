package nominal_streaming

// Network throughput benchmarks measuring points per second sent over HTTP.
// These benchmarks use default batch sizes and realistic configurations.
//
// Run all benchmarks:
//   go test -bench=Throughput -benchmem -benchtime=3s
//
// Results show sustained network throughput including HTTP serialization and sending.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BenchmarkThroughput_Float measures network throughput for float64 data with default settings
func BenchmarkThroughput_Float(b *testing.B) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))

	client, err := NewClient("bench-key", WithBaseURL(server.URL))
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	datasetRID := "ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc"

	// Use default settings
	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		b.Fatalf("Failed to create stream: %v", err)
	}

	floatStream := stream.FloatStream("temperature")
	baseTime := time.Now().UnixNano()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		floatStream.Enqueue(baseTime+int64(i), 25.0+float64(i))
	}

	stream.Close() // Blocks until all HTTP requests complete

	b.StopTimer()

	client.Close()
	server.Close()

	if requestCount > 0 {
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
		b.ReportMetric(float64(requestCount)/b.Elapsed().Seconds(), "requests/sec")
	}
}

// BenchmarkThroughput_Int measures network throughput for int64 data with default settings
func BenchmarkThroughput_Int(b *testing.B) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))

	client, err := NewClient("bench-key", WithBaseURL(server.URL))
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	datasetRID := "ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		b.Fatalf("Failed to create stream: %v", err)
	}

	intStream := stream.IntStream("counter")
	baseTime := time.Now().UnixNano()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		intStream.Enqueue(baseTime+int64(i), int64(i))
	}

	stream.Close() // Blocks until all HTTP requests complete

	b.StopTimer()

	client.Close()
	server.Close()

	if requestCount > 0 {
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
		b.ReportMetric(float64(requestCount)/b.Elapsed().Seconds(), "requests/sec")
	}
}

// BenchmarkThroughput_String measures network throughput for string data with default settings
func BenchmarkThroughput_String(b *testing.B) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))

	client, err := NewClient("bench-key", WithBaseURL(server.URL))
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	datasetRID := "ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		b.Fatalf("Failed to create stream: %v", err)
	}

	stringStream := stream.StringStream("status")
	baseTime := time.Now().UnixNano()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stringStream.Enqueue(baseTime+int64(i), "OK")
	}

	stream.Close() // Blocks until all HTTP requests complete

	b.StopTimer()

	client.Close()
	server.Close()

	if requestCount > 0 {
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
		b.ReportMetric(float64(requestCount)/b.Elapsed().Seconds(), "requests/sec")
	}
}

// BenchmarkThroughput_Mixed measures network throughput with mixed data types
func BenchmarkThroughput_Mixed(b *testing.B) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))

	client, err := NewClient("bench-key", WithBaseURL(server.URL))
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	datasetRID := "ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc"

	stream, err := client.NewDatasetStream(context.Background(), datasetRID)
	if err != nil {
		b.Fatalf("Failed to create stream: %v", err)
	}

	floatStream := stream.FloatStream("temperature")
	intStream := stream.IntStream("counter")
	stringStream := stream.StringStream("status")

	baseTime := time.Now().UnixNano()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			floatStream.Enqueue(baseTime+int64(i), 25.0+float64(i))
		case 1:
			intStream.Enqueue(baseTime+int64(i), int64(i))
		case 2:
			stringStream.Enqueue(baseTime+int64(i), "OK")
		}
	}

	stream.Close() // Blocks until all HTTP requests complete

	b.StopTimer()

	client.Close()
	server.Close()

	if requestCount > 0 {
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
		b.ReportMetric(float64(requestCount)/b.Elapsed().Seconds(), "requests/sec")
	}
}

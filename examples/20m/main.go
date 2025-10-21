package main

import (
	"flag"
	"log"
	"math/rand"
	"time"

	nominal_streaming "github.com/nominal-io/nominal-streaming"
)

func main() {
	// Parse command line flags
	token := flag.String("token", "", "Nominal API token (required)")
	ridStr := flag.String("rid", "", "Dataset RID (required)")
	flag.Parse()

	if *token == "" || *ridStr == "" {
		log.Fatal("Usage: go run main.go -token=<token> -rid=<dataset-rid>")
	}

	// Create a new client
	client, err := nominal_streaming.NewClient(*token)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Parse the dataset RID
	datasetRID, err := nominal_streaming.ParseDatasetRID(*ridStr)
	if err != nil {
		log.Fatalf("Failed to parse dataset RID: %v", err)
	}

	// Create a stream for a specific dataset
	stream, err := client.NewStream(datasetRID)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	// Process errors synchronously so we see them
	errorCount := 0
	stream.ProcessErrors(func(err error) {
		log.Printf("❌ Batch send error: %v", err)
		errorCount++
	})

	log.Println("Streaming 20 million points of random walk data...")

	// Create a single channel stream (no tags)
	randomWalk := stream.FloatStream("random_walk")

	// Calculate time range: 20 million points, 1 microsecond apart, ending now
	const numPoints = 20_000_000
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(numPoints) * time.Microsecond)
	startNanos := startTime.UnixNano()

	log.Printf("Time range: %s to %s", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

	// Generate random walk data
	value := 100.0
	for i := 0; i < numPoints; i++ {
		timestamp := startNanos + int64(i)*1000 // 1 microsecond = 1000 nanoseconds
		value += (rand.Float64() - 0.5) * 0.2   // Random walk: +/- 0.1
		randomWalk.Enqueue(timestamp, value)
	}

	log.Println("Data enqueued, flushing...")
	stream.Close()

	if errorCount > 0 {
		log.Fatalf("Failed with %d errors", errorCount)
	}

	log.Println("✅ Successfully sent all data!")
}

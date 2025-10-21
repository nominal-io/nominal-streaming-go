package main

import (
	"flag"
	"log"
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
	defer stream.Close()

	// Optional: Process errors asynchronously
	stream.ProcessErrors(func(err error) {
		log.Printf("error sending batch to Nominal: %v", err)
	})

	log.Println("Starting data streaming demo...")
	log.Println()

	baseTime := time.Now().UnixNano()

	// Create channel streams for different channel + tag combinations
	tempNorthCS := stream.FloatStream("temperature", nominal_streaming.WithTags(nominal_streaming.Tags{
		"sensor":   "A1",
		"location": "north",
		"unit":     "celsius",
	}))

	tempSouthCS := stream.FloatStream("temperature", nominal_streaming.WithTags(nominal_streaming.Tags{
		"sensor":   "A2",
		"location": "south",
		"unit":     "celsius",
	}))

	pressureCS := stream.FloatStream("pressure")

	countCS := stream.IntStream("event_count", nominal_streaming.WithTags(nominal_streaming.Tags{
		"device": "counter_1",
	}))

	statusCS := stream.StringStream("system_status")

	modeCS := stream.StringStream("device_mode", nominal_streaming.WithTags(nominal_streaming.Tags{
		"device": "pump_1",
	}))

	log.Println("Sending temperature data from multiple sensors...")
	for i := 0; i < 15; i++ {
		tempNorthCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			20.0+float64(i)*0.5,
		)
		tempSouthCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			25.0+float64(i)*0.3,
		)
	}

	log.Println("Sending pressure data...")
	for i := 0; i < 10; i++ {
		pressureCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			101.3+float64(i)*0.1,
		)
	}

	log.Println("Sending event count data...")
	for i := 0; i < 8; i++ {
		countCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			int64(100+i*10),
		)
	}

	log.Println("Sending status data...")
	statuses := []string{"OK", "OK", "WARNING", "OK", "ERROR", "OK", "OK", "WARNING", "OK", "OK"}
	for i, status := range statuses {
		statusCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			status,
		)
	}

	log.Println("Sending device mode data...")
	modes := []string{"IDLE", "ACTIVE", "ACTIVE", "ACTIVE", "IDLE"}
	for i, mode := range modes {
		modeCS.Enqueue(
			baseTime+int64(i*2_000_000_000),
			mode,
		)
	}

	log.Println()
	log.Println("Data sent. Waiting for time-based flush...")
	log.Println()

	time.Sleep(1 * time.Second)

	log.Println()
	log.Println("Demo complete. Stream will flush remaining data on close.")
}

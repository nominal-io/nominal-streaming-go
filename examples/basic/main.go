package main

import (
	"log"
	"time"

	nominal_streaming "github.com/nominal-io/nominal-streaming"
)

func main() {
	// Create a new client
	client, err := nominal_streaming.NewClient("demo-key")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Parse the dataset RID
	datasetRID, err := nominal_streaming.ParseDatasetRID("ri.nominal.main.dataset.12345678-1234-1234-1234-123456789abc")
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
		log.Printf("❌ Batch send error: %v", err)
	})

	log.Println("Starting data streaming demo...")
	log.Println("Batches will flush after 1000 points OR 5 seconds, whichever comes first")
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

	pressureCS := stream.FloatStream("pressure", nominal_streaming.WithTags(nominal_streaming.Tags{
		"sensor": "P1",
		"unit":   "kPa",
	}))

	countCS := stream.IntStream("event_count", nominal_streaming.WithTags(nominal_streaming.Tags{
		"device": "counter_1",
	}))

	// Example without tags - simple channel reference
	statusCS := stream.StringStream("system_status")

	modeCS := stream.StringStream("device_mode", nominal_streaming.WithTags(nominal_streaming.Tags{
		"device": "pump_1",
	}))

	// Example 1: Send float data points for temperature sensors
	log.Println("Sending temperature data from multiple sensors...")
	for i := 0; i < 15; i++ {
		// Sensor A1 in North location
		tempNorthCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			20.0+float64(i)*0.5,
		)

		// Sensor A2 in South location
		tempSouthCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			25.0+float64(i)*0.3,
		)
	}

	// Example 2: Send float data points for pressure
	log.Println("Sending pressure data...")
	for i := 0; i < 10; i++ {
		pressureCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			101.3+float64(i)*0.1,
		)
	}

	// Example 3: Send int64 data points for event counts
	log.Println("Sending event count data...")
	for i := 0; i < 8; i++ {
		countCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			int64(100+i*10),
		)
	}

	// Example 4: Send string data points for status
	log.Println("Sending status data...")
	statuses := []string{"OK", "OK", "WARNING", "OK", "ERROR", "OK", "OK", "WARNING", "OK", "OK"}
	for i, status := range statuses {
		statusCS.Enqueue(
			baseTime+int64(i*1_000_000_000),
			status,
		)
	}

	// Example 5: Send string data points for another device
	log.Println("Sending device mode data...")
	modes := []string{"IDLE", "ACTIVE", "ACTIVE", "ACTIVE", "IDLE"}
	for i, mode := range modes {
		modeCS.Enqueue(
			baseTime+int64(i*2_000_000_000),
			mode,
		)
	}

	log.Println()
	log.Println("Data sent. Waiting for time-based flush (5 seconds)...")
	log.Println()

	// Wait for the time-based flush to occur
	time.Sleep(6 * time.Second)

	log.Println()
	log.Println("Demo complete. Stream will flush remaining data on close.")
}

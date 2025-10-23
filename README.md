# ⟢ nominal-streaming-go

Go client for streaming time-series data to Nominal.

```bash
go get github.com/nominal-io/nominal-streaming
```

## Quick Start

Get a typed stream for a channel and write data points:
```go
ds, _ := client.NewDatasetStream(context.Background(), "ri.nominal.main.dataset.your-dataset-id")
defer ds.Close()
temperature := ds.FloatStream("temperature")
temperature.Enqueue(time.Now().UnixNano(), 23.5)
temperature.Enqueue(time.Now().UnixNano(), 24.5)
temperature.Enqueue(time.Now().UnixNano(), 25.5)
```

Or, write data points with the channel name and values, and the library will delegate to the appropriate stream for you:
```go
ds, _ := client.NewDatasetStream(context.Background(), "ri.nominal.main.dataset.your-dataset-id")
defer ds.Close()
ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 23.5)
ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 24.5)
ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 25.5)
```

### Complete Example

```go
package main

import (
    "context"
    "time"

    nominal "github.com/nominal-io/nominal-streaming"
)

func main() {
    client, _ := nominal.NewClient("your-api-key")
    defer client.Close()

    stream, _ := client.NewDatasetStream(
        context.Background(),
        "ri.nominal.main.dataset.your-dataset-id",
    )
    // Closing the stream blocks until all requests have been sent to Nominal
    defer stream.Close()

    // Set error callback
    stream.ProcessErrors(func(err error) {
        log.Printf("Stream error: %v", err)
    })

    // Get typed data streams
    temp := stream.FloatStream("temperature")
    count := stream.IntStream("count")
    // Optionally with tags..
    status := stream.StringStream("status", nominal.WithTags(nominal.Tags{
        "sensor_id": "A1",
        "location":  "lab",
    }))

    // Add data to streams
    temp.Enqueue(time.Now().UnixNano(), 23.5)
    count.Enqueue(time.Now().UnixNano(), 42)
    status.Enqueue(time.Now().UnixNano(), "OK")

    // Add data without retrieving a channel stream up-front
    ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 23.5)
    ds.EnqueueDynamic("status", time.Now().UnixNano(), "OK", nominal.WithTags(nominal.Tags{
        "sensor_id": "A1",
        "location":  "lab",
    }))
}
```

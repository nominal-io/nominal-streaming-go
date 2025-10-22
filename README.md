# nominal-streaming-go

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

Or, just write data points with the channel name and values, and the library will delegate to the appropriate stream for you:
```go
ds, _ := client.NewDatasetStream(context.Background(), "ri.nominal.main.dataset.your-dataset-id")
defer ds.Close()
temperature := ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 23.5)
temperature := ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 24.5)
temperature := ds.EnqueueDynamic("temperature", time.Now().UnixNano(), 25.5)
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
    defer stream.Close()

    // Write typed data points
    stream.FloatStream("temperature").Enqueue(time.Now().UnixNano(), 23.5)
    stream.IntStream("count").Enqueue(time.Now().UnixNano(), 42)
    stream.StringStream("status").Enqueue(time.Now().UnixNano(), "OK")
}
```

### With Tags

```go
sensor := stream.FloatStream("temperature", nominal.WithTags(nominal.Tags{
    "sensor_id": "A1",
    "location":  "lab",
}))

sensor.Enqueue(time.Now().UnixNano(), 23.5)
```

### Error Handling

```go
stream.ProcessErrors(func(err error) {
    log.Printf("Stream error: %v", err)
})
```

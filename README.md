# ⟢ nominal-streaming-go

Go client for streaming time-series data to Nominal.

```bash
go get github.com/nominal-io/nominal-streaming-go
```

## Quick Start

Get a typed stream for a channel and write data points:
```go
ds, _, _ := client.NewDatasetStream(
    context.Background(),
    "ri.nominal.main.dataset.your-dataset-id",
)
defer ds.Close()

temperature := ds.FloatStream("temperature")
temperature.Enqueue(time.Now().UnixNano(), 23.5)
temperature.Enqueue(time.Now().UnixNano(), 24.5)
temperature.Enqueue(time.Now().UnixNano(), 25.5)
```

Or, write data points with the channel name and values, and the library will delegate to the appropriate stream for you:
```go
ds, _, _ := client.NewDatasetStream(
    context.Background(),
    "ri.nominal.main.dataset.your-dataset-id",
)
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

    nominal "github.com/nominal-io/nominal-streaming-go"
)

func main() {
    client, _ := nominal.NewClient("your-api-key")
    defer client.Close()

    ds, errCh, _ := client.NewDatasetStream(
        context.Background(),
        "ri.nominal.main.dataset.your-dataset-id",
    )
    // Closing the stream blocks until all requests have been sent to Nominal
    defer ds.Close()

    // Handle errors from the stream asynchronously
    go func() {
        for err := range errCh {
            log.Printf("Stream error: %v", err)
        }
    }()

    // Get typed data streams
    temp := ds.FloatStream("temperature")
    count := ds.IntStream("count")
    // Optionally with tags..
    status := ds.StringStream("status", nominal.WithTags(nominal.Tags{
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

## Configuration Options

```go
ds, errCh, _ := client.NewDatasetStream(
    ctx,
    datasetRID,
    nominal.WithFlushInterval(time.Second),                       // Time between flushes (default: 500ms)
    nominal.WithBatchSize(100_000),                               // Points before size-triggered flush (default: 65,536)
    nominal.WithMaxConcurrentFlushes(20),                         // Max concurrent HTTP requests (default: 10)
    nominal.WithBackpressurePolicy(nominal.BackpressureRequeue),  // How to handle backpressure (default: Requeue)
    nominal.WithMaxBufferPoints(500_000),                         // Max buffered points before dropping oldest (default: 1M)
)
```

### Backpressure Policies

| Policy                          | Behavior                                                          | Use When                                 |
| ------------------------------- | ----------------------------------------------------------------- | ---------------------------------------- |
| `BackpressureRequeue` (default) | Re-queue data for next flush, drop oldest if buffer exceeds limit | Data completeness matters most           |
| `BackpressureDropBatch`         | Drop the entire batch when backpressure occurs                    | Freshness matters more than completeness |

### Memory Budget

The default `maxBufferPoints` (1,000,000) provides approximately:

- ~16 MB memory for float data
- ~30 seconds of data at 60Hz × 500 channels
- ~20 seconds of data at 1000Hz × 50 channels

## Notes

- **Error channel**: Always drain the error channel in a goroutine. Errors are reported asynchronously and not draining can cause internal buffer pressure.
- **Float values**: `NaN` and `Inf` values are not supported and will result in an error.
- **Backpressure**: If you see log messages like `"flush skipped due to backpressure"`, the server is responding slower than your send rate. With the default `BackpressureRequeue` policy, data is re-queued (not lost). If the buffer exceeds `maxBufferPoints`, oldest data is dropped to prevent unbounded memory growth.

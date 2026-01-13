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

## Error Handling

Errors from the API are wrapped in `NominalError` which exposes structured error information:

```go
go func() {
    for err := range errCh {
        if nomErr, ok := nominal.AsNominalError(err); ok {
            fmt.Printf("Error: %s\n", nomErr.Name())        // e.g., "Scout:DatasetNotFound"
            fmt.Printf("Code: %s\n", nomErr.Code())         // e.g., "NOT_FOUND"
            fmt.Printf("Instance ID: %s\n", nomErr.InstanceID()) // UUID for support
            fmt.Printf("HTTP Status: %d\n", nomErr.StatusCode()) // e.g., 404
        }
        
        // Check if the error is retryable (transient)
        if nominal.IsRetryable(err) {
            // Implement retry logic
        }
    }
}()
```

### Available Error Methods

- `Code()` - Error category (e.g., `INVALID_ARGUMENT`, `NOT_FOUND`, `UNAUTHORIZED`)
- `Name()` - Specific error type (e.g., `Scout:DatasetNotFound`)
- `InstanceID()` - Unique identifier for this error occurrence (useful for support tickets)
- `StatusCode()` - HTTP status code
- `Parameters()` - Additional context parameters from the error

### Helper Functions

- `AsNominalError(err)` - Extract `NominalError` from an error chain
- `IsRetryable(err)` - Check if the error is transient and may succeed on retry

## Notes

- **Error channel**: Always drain the error channel in a goroutine. Errors are reported asynchronously and not draining can cause internal buffer pressure.
- **Float values**: `NaN` and `Inf` values are not supported and will result in an error.

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

## Configuration Options

Customize stream behavior using functional options. By default, the batcher flushes when it accumulates **65,536 data points** or every **500 milliseconds**, whichever comes first.

### Data Stream Options

| Option                             | Description                            | Default |
| ---------------------------------- | -------------------------------------- | ------- |
| `WithFlushInterval(time.Duration)` | Time between automatic flushes         | 500ms   |
| `WithBatchSize(int)`               | Number of points that triggers a flush | 65,536  |

```go
ds, errCh, err := client.NewDatasetStream(
    context.Background(),
    "ri.nominal.main.dataset.your-dataset-id",
    nominal.WithFlushInterval(2 * time.Second),  // Flush every 2 seconds
    nominal.WithBatchSize(10_000),                // Or when 10,000 points accumulated
)
```

### Log Stream Options

| Option                                | Description                                 | Default |
| ------------------------------------- | ------------------------------------------- | ------- |
| `WithLogFlushInterval(time.Duration)` | Time between automatic flushes              | 500ms   |
| `WithLogBatchSize(int)`               | Number of log entries that triggers a flush | 4,096   |

```go
logStream, errCh, err := client.NewDatasetLogStream(
    context.Background(),
    "ri.nominal.main.dataset.your-dataset-id",
    nominal.WithLogFlushInterval(1 * time.Second),
    nominal.WithLogBatchSize(1_000),
)
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

## Log Streaming

Stream log messages with per-entry key-value arguments:

```go
logStream, errCh, err := client.NewDatasetLogStream(
    context.Background(),
    "ri.nominal.main.dataset.your-dataset-id",
    nominal.WithLogFlushInterval(1 * time.Second),
)
defer logStream.Close()

// Handle errors asynchronously
go func() {
    for err := range errCh {
        log.Printf("Log stream error: %v", err)
    }
}()

// Get a log channel stream
logs := logStream.LogStream("logs")

// Enqueue a simple log message
logs.Enqueue(time.Now().UnixNano(), "System started")

// Enqueue a log message with key-value arguments
logs.EnqueueWithArgs(time.Now().UnixNano(), "Motor command sent", map[string]string{
    "motor_id":   "M1",
    "position":   "45.2",
    "error_code": "0",
})
```

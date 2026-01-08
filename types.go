package nominal_streaming

// NanosecondsUTC represents a timestamp in nanoseconds since Unix epoch (UTC).
type NanosecondsUTC = int64

// Tags is a map of tag names to tag values for labeling data points in a channel stream.
type Tags = map[string]string

// Value is a constraint for supported data point value types.
type Value interface {
	float64 | int64 | string | []float64 | []string
}

// Log represents a log message string that will be sent via the WriteLogs endpoint.
type Log string

// BackpressurePolicy determines how the batcher handles backpressure when
// the maximum number of concurrent flushes is reached.
type BackpressurePolicy int

const (
	// BackpressureRequeue re-queues skipped batch data for the next flush attempt.
	// This is the safest option (no data loss) but can cause memory growth
	// and stale data accumulation under sustained backpressure.
	// This is the default policy.
	BackpressureRequeue BackpressurePolicy = iota

	// BackpressureDropBatch drops the entire batch when backpressure occurs.
	// This prevents staleness but loses data. Use when freshness matters more
	// than completeness.
	BackpressureDropBatch
)

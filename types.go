package nominal

// NanosecondsUTC represents a timestamp in nanoseconds since Unix epoch (UTC).
type NanosecondsUTC = int64

// Tags is a map of tag names to tag values for labeling data points.
type Tags = map[string]string

// Value is a constraint for supported data point value types.
type Value interface {
	float64 | int64 | string
}

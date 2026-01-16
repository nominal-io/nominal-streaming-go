package nominal_streaming

import (
	"fmt"
	"sync"
	"time"
)

type DatasetStream struct {
	datasetRID string
	batcher    *batcher

	mu                 sync.Mutex
	floatStreams       map[channelReferenceKey]*ChannelStream[float64]
	intStreams         map[channelReferenceKey]*ChannelStream[int64]
	stringStreams      map[channelReferenceKey]*ChannelStream[string]
	floatArrayStreams  map[channelReferenceKey]*ChannelStream[[]float64]
	stringArrayStreams map[channelReferenceKey]*ChannelStream[[]string]
}

type DatasetStreamOption func(*DatasetStream) error

func WithBatchSize(size int) DatasetStreamOption {
	return func(s *DatasetStream) error {
		s.batcher.flushSize = size
		return nil
	}
}

func WithFlushInterval(interval time.Duration) DatasetStreamOption {
	return func(s *DatasetStream) error {
		s.batcher.flushPeriod = interval
		return nil
	}
}

// WithMaxConcurrentFlushes sets the maximum number of concurrent HTTP requests
// for sending batches. When this limit is reached, new flushes are handled
// according to the backpressure policy. Default is 10.
func WithMaxConcurrentFlushes(n int) DatasetStreamOption {
	return func(s *DatasetStream) error {
		if n <= 0 {
			n = 1
		}
		s.batcher.maxConcurrentFlushes = n
		s.batcher.flushSem = make(chan struct{}, n)
		return nil
	}
}

// WithBackpressurePolicy sets how the batcher handles backpressure when
// the maximum concurrent flushes limit is reached.
//
// Options:
//   - BackpressureRequeue (default): Re-queue data for the next flush attempt.
//     Safe (no data loss) but can cause memory growth under sustained backpressure.
//   - BackpressureDropBatch: Drop the entire batch when backpressure occurs.
//     Prevents staleness but loses data. Use when freshness matters more than completeness.
func WithBackpressurePolicy(policy BackpressurePolicy) DatasetStreamOption {
	return func(s *DatasetStream) error {
		s.batcher.backpressurePolicy = policy
		return nil
	}
}

// WithMaxBufferPoints sets the maximum number of data points to buffer before
// dropping oldest data. This prevents unbounded memory growth under sustained backpressure.
//
// When the buffer exceeds this limit (after re-queuing), the oldest points are dropped.
// Default is 1,000,000 points (~16 MB for floats, ~30 seconds at 60Hz × 500 channels).
// Set to 0 to disable the limit (not recommended for production).
func WithMaxBufferPoints(max int) DatasetStreamOption {
	return func(s *DatasetStream) error {
		s.batcher.maxBufferPoints = max
		return nil
	}
}

// ChannelOption is a function that configures a channel stream.
type ChannelOption func(*channelConfig)

type channelConfig struct {
	tags map[string]string
}

// WithTags sets tags for a channel stream.
func WithTags(tags Tags) ChannelOption {
	return func(cfg *channelConfig) {
		cfg.tags = tags
	}
}

// buildChannelReference processes options and creates a channel reference with key.
func buildChannelReference(ch string, options []ChannelOption) channelReference {
	cfg := &channelConfig{tags: map[string]string{}}
	for _, option := range options {
		option(cfg)
	}

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(cfg.tags),
	}

	return channelReference{
		channelReferenceKey: key,
		tags:                cfg.tags,
	}
}

func (s *DatasetStream) FloatStream(ch string, options ...ChannelOption) *ChannelStream[float64] {
	ref := buildChannelReference(ch, options)

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, exists := s.floatStreams[ref.channelReferenceKey]; exists {
		return stream
	}

	stream := &ChannelStream[float64]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v float64) {
			s.batcher.addFloat(ref, ts, v)
		},
	}
	s.floatStreams[ref.channelReferenceKey] = stream
	return stream
}

func (s *DatasetStream) IntStream(ch string, options ...ChannelOption) *ChannelStream[int64] {
	ref := buildChannelReference(ch, options)

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, exists := s.intStreams[ref.channelReferenceKey]; exists {
		return stream
	}

	stream := &ChannelStream[int64]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v int64) {
			s.batcher.addInt(ref, ts, v)
		},
	}
	s.intStreams[ref.channelReferenceKey] = stream
	return stream
}

func (s *DatasetStream) StringStream(ch string, options ...ChannelOption) *ChannelStream[string] {
	ref := buildChannelReference(ch, options)

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, exists := s.stringStreams[ref.channelReferenceKey]; exists {
		return stream
	}

	stream := &ChannelStream[string]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v string) {
			s.batcher.addString(ref, ts, v)
		},
	}
	s.stringStreams[ref.channelReferenceKey] = stream
	return stream
}

func (s *DatasetStream) FloatArrayStream(ch string, options ...ChannelOption) *ChannelStream[[]float64] {
	ref := buildChannelReference(ch, options)

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, exists := s.floatArrayStreams[ref.channelReferenceKey]; exists {
		return stream
	}

	stream := &ChannelStream[[]float64]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v []float64) {
			s.batcher.addFloatArray(ref, ts, v)
		},
	}
	s.floatArrayStreams[ref.channelReferenceKey] = stream
	return stream
}

func (s *DatasetStream) StringArrayStream(ch string, options ...ChannelOption) *ChannelStream[[]string] {
	ref := buildChannelReference(ch, options)

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, exists := s.stringArrayStreams[ref.channelReferenceKey]; exists {
		return stream
	}

	stream := &ChannelStream[[]string]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v []string) {
			s.batcher.addStringArray(ref, ts, v)
		},
	}
	s.stringArrayStreams[ref.channelReferenceKey] = stream
	return stream
}

// EnqueueDynamic is a convenience method that accepts any supported value type and
// automatically dispatches to the appropriate typed channel stream.
// For slightly better performance, use typed stream methods directly.
func (s *DatasetStream) EnqueueDynamic(channel string, timestamp NanosecondsUTC, value any, options ...ChannelOption) error {
	switch v := value.(type) {
	case float64:
		s.FloatStream(channel, options...).Enqueue(timestamp, v)
	case int64:
		s.IntStream(channel, options...).Enqueue(timestamp, v)
	case string:
		s.StringStream(channel, options...).Enqueue(timestamp, v)
	case []float64:
		s.FloatArrayStream(channel, options...).Enqueue(timestamp, v)
	case []string:
		s.StringArrayStream(channel, options...).Enqueue(timestamp, v)
	default:
		return fmt.Errorf("unsupported value type: %T (supported types: float64, int64, string, []float64, []string)", value)
	}
	return nil
}

func (s *DatasetStream) Close() error {
	return s.batcher.close()
}

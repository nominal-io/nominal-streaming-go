package nominal_streaming

import (
	"sync"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
)

type DatasetStream struct {
	datasetRID rids.NominalDataSourceOrDatasetRid
	batcher    *batcher

	mu            sync.Mutex
	floatStreams  map[channelReferenceKey]*ChannelStream[float64]
	intStreams    map[channelReferenceKey]*ChannelStream[int64]
	stringStreams map[channelReferenceKey]*ChannelStream[string]
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

func (s *DatasetStream) Close() error {
	return s.batcher.close()
}

func (s *DatasetStream) Errors() <-chan error {
	return s.batcher.errors
}

func (s *DatasetStream) DroppedErrorCount() int64 {
	return s.batcher.droppedErrors.Load()
}

// ProcessErrors launches a goroutine to handle async errors until the stream closes.
func (s *DatasetStream) ProcessErrors(handler func(error)) {
	go func() {
		for err := range s.Errors() {
			handler(err)
		}
	}()
}

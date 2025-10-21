package nominal_streaming

import (
	"sync"
	"time"
)

type DatasetStream struct {
	datasetRID string
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

// ChannelOption is an option for configuring a channel stream.
type ChannelOption interface {
	apply(*channelConfig)
}

type channelConfig struct {
	tags map[string]string
}

type tagsOption struct {
	tags Tags
}

func (o tagsOption) apply(cfg *channelConfig) {
	cfg.tags = o.tags
}

// WithTags sets tags for a channel stream.
func WithTags(tags Tags) ChannelOption {
	return tagsOption{tags: tags}
}

func (s *DatasetStream) FloatStream(ch string, opts ...ChannelOption) *ChannelStream[float64] {
	cfg := &channelConfig{tags: map[string]string{}}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(cfg.tags),
	}

	if stream, exists := s.floatStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                cfg.tags,
	}
	stream := &ChannelStream[float64]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v float64) {
			s.batcher.addFloat(ref, ts, v)
		},
	}
	s.floatStreams[key] = stream
	return stream
}

func (s *DatasetStream) IntStream(ch string, opts ...ChannelOption) *ChannelStream[int64] {
	cfg := &channelConfig{tags: map[string]string{}}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(cfg.tags),
	}

	if stream, exists := s.intStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                cfg.tags,
	}
	stream := &ChannelStream[int64]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v int64) {
			s.batcher.addInt(ref, ts, v)
		},
	}
	s.intStreams[key] = stream
	return stream
}

func (s *DatasetStream) StringStream(ch string, opts ...ChannelOption) *ChannelStream[string] {
	cfg := &channelConfig{tags: map[string]string{}}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(cfg.tags),
	}

	if stream, exists := s.stringStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                cfg.tags,
	}
	stream := &ChannelStream[string]{
		batcher: s.batcher,
		ref:     ref,
		enqueue: func(ts NanosecondsUTC, v string) {
			s.batcher.addString(ref, ts, v)
		},
	}
	s.stringStreams[key] = stream
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

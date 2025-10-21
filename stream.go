package nominal

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

func (s *DatasetStream) FloatStream(ch string, tags Tags) *ChannelStream[float64] {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(tags),
	}

	if stream, exists := s.floatStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                tags,
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

func (s *DatasetStream) IntStream(ch string, tags Tags) *ChannelStream[int64] {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(tags),
	}

	if stream, exists := s.intStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                tags,
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

func (s *DatasetStream) StringStream(ch string, tags Tags) *ChannelStream[string] {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := channelReferenceKey{
		channel:  channelName(ch),
		tagsHash: hashTags(tags),
	}

	if stream, exists := s.stringStreams[key]; exists {
		return stream
	}

	ref := channelReference{
		channelReferenceKey: key,
		tags:                tags,
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

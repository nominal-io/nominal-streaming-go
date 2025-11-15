package nominal_streaming

import (
	"sync"
	"time"
)

// DatasetLogStream is parallel to DatasetStream but specifically for logs.
// Use LogStream() to get a channel-specific stream.
type DatasetLogStream struct {
	datasetRID string
	batcher    *logBatcher

	mu         sync.Mutex
	logStreams map[channelReferenceKey]*LogChannelStream
}

type DatasetLogStreamOption func(*DatasetLogStream) error

func WithLogBatchSize(size int) DatasetLogStreamOption {
	return func(s *DatasetLogStream) error {
		s.batcher.flushSize = size
		return nil
	}
}

func WithLogFlushInterval(interval time.Duration) DatasetLogStreamOption {
	return func(s *DatasetLogStream) error {
		s.batcher.flushPeriod = interval
		return nil
	}
}

type LogChannelStream struct {
	batcher *logBatcher
	channel string
	enqueue func(NanosecondsUTC, Log, map[string]string)
}

func (ls *LogChannelStream) Enqueue(timestamp NanosecondsUTC, message Log) {
	ls.enqueue(timestamp, message, nil)
}

func (ls *LogChannelStream) EnqueueWithArgs(timestamp NanosecondsUTC, message Log, args map[string]string) {
	ls.enqueue(timestamp, message, args)
}

func (s *DatasetLogStream) LogStream(ch string) *LogChannelStream {
	s.mu.Lock()
	defer s.mu.Unlock()

	channel := channelName(ch)
	if stream, exists := s.logStreams[channelReferenceKey{channel: channel}]; exists {
		return stream
	}

	stream := &LogChannelStream{
		batcher: s.batcher,
		channel: string(channel),
		enqueue: func(ts NanosecondsUTC, message Log, args map[string]string) {
			s.batcher.addLog(channel, ts, message, args)
		},
	}
	s.logStreams[channelReferenceKey{channel: channel}] = stream
	return stream
}

func (s *DatasetLogStream) Close() error {
	return s.batcher.close()
}

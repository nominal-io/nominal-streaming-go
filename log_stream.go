package nominal_streaming

import (
	"sync"
	"time"
)

// DatasetLogStream is parallel to DatasetStream but specifically for logs.
// Logs have different "tag" semantics. With logs, key-value pairs are provided
// per-message and not indexed as tags on the channel, they may be high-cardinality.
//
// Use LogStream() to get a channel-specific stream to write logs to.
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

// WithLogMaxConcurrentFlushes sets the maximum number of concurrent HTTP requests
// for sending log batches. Default is 10.
func WithLogMaxConcurrentFlushes(n int) DatasetLogStreamOption {
	return func(s *DatasetLogStream) error {
		if n <= 0 {
			n = 1
		}
		s.batcher.maxConcurrentFlushes = n
		s.batcher.flushSem = make(chan struct{}, n)
		return nil
	}
}

// WithLogBackpressurePolicy sets how the log batcher handles backpressure.
// See WithBackpressurePolicy for details.
func WithLogBackpressurePolicy(policy BackpressurePolicy) DatasetLogStreamOption {
	return func(s *DatasetLogStream) error {
		s.batcher.backpressurePolicy = policy
		return nil
	}
}

// WithLogMaxBufferPoints sets the maximum number of log points to buffer before
// dropping oldest data. Default is 1,000,000 points.
func WithLogMaxBufferPoints(max int) DatasetLogStreamOption {
	return func(s *DatasetLogStream) error {
		s.batcher.maxBufferPoints = max
		return nil
	}
}

type LogChannelStream struct {
	batcher *logBatcher
	channel string
	enqueue func(timestamp NanosecondsUTC, message Log, args map[string]string)
}

// Enqueue enqueues a log message.
func (ls *LogChannelStream) Enqueue(timestamp NanosecondsUTC, message Log) {
	ls.enqueue(timestamp, message, nil)
}

// EnqueueWithArgs enqueues a log message with associated key-value pair arguments.
// These arguments are specific to this log entry. For example, if you have a log message
//
//	{"message": "error setting actuator position", "error_code": 123, "system": "hydraulics"}
//
// you could call this function as
//
//	EnqueueWithArgs(timestamp, message, map[string]string{"error_code": "123", "system": "hydraulics"})
func (ls *LogChannelStream) EnqueueWithArgs(timestamp NanosecondsUTC, message Log, args map[string]string) {
	ls.enqueue(timestamp, message, args)
}

// LogStream returns a LogChannelStream for the specified channel name.
// It is recommended to use the channel name "logs".
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

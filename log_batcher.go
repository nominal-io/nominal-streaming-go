package nominal_streaming

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
	"github.com/nominal-io/nominal-api-go/io/nominal/api"
	writerapi "github.com/nominal-io/nominal-api-go/storage/writer/api"
	"github.com/palantir/pkg/safelong"
)

type logWithArgs struct {
	message Log
	args    map[string]string
}

type logBatch struct {
	Channel    channelName
	Timestamps []NanosecondsUTC
	Values     []logWithArgs
}

type logBuffer struct {
	channel    channelName
	timestamps []NanosecondsUTC
	values     []logWithArgs
}

type logBatcher struct {
	closeChan   chan struct{}
	wg          sync.WaitGroup
	flushSize   int
	flushPeriod time.Duration

	// Error handling
	errorsMu sync.Mutex
	errors   chan error
	closed   bool

	// API client
	ctx        context.Context
	apiClient  *nominalAPIClient
	datasetRID rids.NominalDataSourceOrDatasetRid

	mu          sync.Mutex
	logBuffers  map[channelName]*logBuffer
	totalPoints int
}

func newLogBatcher(
	ctx context.Context,
	apiClient *nominalAPIClient,
	datasetRID rids.NominalDataSourceOrDatasetRid,
	flushSize int,
	flushPeriod time.Duration,
) *logBatcher {
	return &logBatcher{
		closeChan:   make(chan struct{}),
		flushSize:   flushSize,
		flushPeriod: flushPeriod,
		errors:      make(chan error, 256), // Increased from 100 to handle burst errors
		ctx:         ctx,
		apiClient:   apiClient,
		datasetRID:  datasetRID,
		logBuffers:  make(map[channelName]*logBuffer),
	}
}

func (b *logBatcher) start() {
	b.wg.Add(1)
	go b.run()
}

func (b *logBatcher) close() error {
	b.errorsMu.Lock()
	if b.closed {
		b.errorsMu.Unlock()
		return nil
	}
	b.closed = true
	b.errorsMu.Unlock()

	close(b.closeChan)
	b.wg.Wait()

	b.errorsMu.Lock()
	close(b.errors)
	b.errorsMu.Unlock()

	return nil
}

func (b *logBatcher) reportError(err error) {
	b.errorsMu.Lock()
	defer b.errorsMu.Unlock()

	if b.closed {
		return
	}

	select {
	case b.errors <- err:
	default:
		// Channel full, drop oldest error and log warning
		select {
		case <-b.errors:
			log.Printf("Warning: Errors channel full, dropped oldest error to make room")
		default:
		}
		select {
		case b.errors <- err:
		default:
			log.Printf("Warning: Failed to report error after dropping oldest: %v", err)
		}
	}
}

func (b *logBatcher) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-b.closeChan:
			b.flush()
			return
		case <-ticker.C:
			b.flush()
		}
	}
}

func (b *logBatcher) addLog(channel channelName, timestamp NanosecondsUTC, message Log, args map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.logBuffers[channel]
	if !exists {
		buffer = &logBuffer{
			channel:    channel,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([]logWithArgs, 0),
		}
		b.logBuffers[channel] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, logWithArgs{
		message: message,
		args:    args,
	})
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *logBatcher) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *logBatcher) flushLocked() {
	if b.totalPoints == 0 {
		return
	}

	// Create batches from buffers
	logBatches := make([]logBatch, 0, len(b.logBuffers))
	for _, buffer := range b.logBuffers {
		if len(buffer.timestamps) > 0 {
			logBatches = append(logBatches, logBatch{
				Channel:    buffer.channel,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([]logWithArgs, 0)
		}
	}

	b.totalPoints = 0

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		if err := b.sendLogBatches(logBatches); err != nil {
			b.reportError(err)
		}
	}()
}

func (b *logBatcher) sendLogBatches(batches []logBatch) error {

	totalCapacity := 0
	for _, batch := range batches {
		totalCapacity += len(batch.Timestamps)
	}
	logPoints := make([]writerapi.LogPoint, 0, totalCapacity)

	for _, batch := range batches {
		if len(batch.Timestamps) != len(batch.Values) {
			return fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
		}
		for i := range batch.Timestamps {
			seconds := batch.Timestamps[i] / 1_000_000_000
			nanos := batch.Timestamps[i] % 1_000_000_000
			logPoint := writerapi.LogPoint{
				Timestamp: api.Timestamp{
					Seconds: safelong.SafeLong(seconds),
					Nanos:   safelong.SafeLong(nanos),
				},
				Value: writerapi.LogValue{
					Message: string(batch.Values[i].message),
					Args:    batch.Values[i].args,
				},
			}
			logPoints = append(logPoints, logPoint)
		}
	}
	req := writerapi.WriteLogsRequest{
		Logs: logPoints,
	}

	if err := b.apiClient.writeLogs(b.ctx, b.datasetRID, req); err != nil {
		return fmt.Errorf("failed to write logs: %w", err)
	}

	return nil
}

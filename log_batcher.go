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

	// Concurrency control for flush goroutines
	maxConcurrentFlushes int
	flushSem             chan struct{}

	// Backpressure handling
	backpressurePolicy BackpressurePolicy
	maxBufferPoints    int

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
		closeChan:            make(chan struct{}),
		flushSize:            flushSize,
		flushPeriod:          flushPeriod,
		errors:               make(chan error, 256), // Increased from 100 to handle burst errors
		maxConcurrentFlushes: defaultMaxConcurrentFlushes,
		flushSem:             make(chan struct{}, defaultMaxConcurrentFlushes),
		backpressurePolicy:   BackpressureRequeue,
		maxBufferPoints:      defaultMaxBufferPoints,
		ctx:                  ctx,
		apiClient:            apiClient,
		datasetRID:           datasetRID,
		logBuffers:           make(map[channelName]*logBuffer),
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

	// Try to acquire semaphore (non-blocking to avoid deadlock while holding mu)
	select {
	case b.flushSem <- struct{}{}:
		// Acquired semaphore, proceed with flush
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() { <-b.flushSem }() // Release semaphore when done
			if err := b.sendLogBatches(logBatches); err != nil {
				b.reportError(err)
			}
		}()
	default:
		// Too many concurrent flushes - this indicates backpressure
		batchPoints := countLogBatchPoints(logBatches)

		switch b.backpressurePolicy {
		case BackpressureDropBatch:
			log.Printf("nominal-streaming: log flush skipped due to backpressure, dropped batch of %d points (policy: DropBatch)", batchPoints)
			// Data is intentionally dropped

		case BackpressureRequeue:
			fallthrough
		default:
			log.Printf("nominal-streaming: log flush skipped due to backpressure (%d concurrent flushes), re-queued %d points", b.maxConcurrentFlushes, batchPoints)
			// Re-add the batches back to the buffers
			b.reAddLogBatches(logBatches)
			// Check for buffer overflow and drop oldest if needed
			b.enforceBufferLimitLocked()
		}
	}
}

// countLogBatchPoints counts total points across all log batches.
func countLogBatchPoints(batches []logBatch) int {
	total := 0
	for _, b := range batches {
		total += len(b.Timestamps)
	}
	return total
}

// reAddLogBatches puts log batches back into the buffers when a flush is skipped due to backpressure.
// Note: caller must hold b.mu lock.
func (b *logBatcher) reAddLogBatches(batches []logBatch) {
	for _, batch := range batches {
		buffer, exists := b.logBuffers[batch.Channel]
		if !exists {
			buffer = &logBuffer{
				channel:    batch.Channel,
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([]logWithArgs, 0),
			}
			b.logBuffers[batch.Channel] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}
}

// enforceBufferLimitLocked drops oldest log points when buffer exceeds maxBufferPoints.
// Note: caller must hold b.mu lock.
func (b *logBatcher) enforceBufferLimitLocked() {
	if b.maxBufferPoints <= 0 || b.totalPoints <= b.maxBufferPoints {
		return
	}

	pointsToDrop := b.totalPoints - b.maxBufferPoints
	droppedPoints := 0

	for channel, buffer := range b.logBuffers {
		if pointsToDrop <= 0 {
			break
		}
		if len(buffer.timestamps) > 0 {
			toDrop := min(len(buffer.timestamps), pointsToDrop)
			buffer.timestamps = buffer.timestamps[toDrop:]
			buffer.values = buffer.values[toDrop:]
			pointsToDrop -= toDrop
			droppedPoints += toDrop
			b.totalPoints -= toDrop
			if len(buffer.timestamps) == 0 {
				delete(b.logBuffers, channel)
			}
		}
	}

	if droppedPoints > 0 {
		log.Printf("nominal-streaming: log buffer limit exceeded (%d points), dropped %d oldest points", b.maxBufferPoints, droppedPoints)
	}
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

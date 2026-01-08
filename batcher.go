package nominal_streaming

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
	pb "github.com/nominal-io/nominal-streaming-go/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type channelName string

type floatBatch struct {
	Channel    channelName
	Tags       map[string]string
	Timestamps []NanosecondsUTC
	Values     []float64
}

type intBatch struct {
	Channel    channelName
	Tags       map[string]string
	Timestamps []NanosecondsUTC
	Values     []int64
}

type stringBatch struct {
	Channel    channelName
	Tags       map[string]string
	Timestamps []NanosecondsUTC
	Values     []string
}

type floatArrayBatch struct {
	Channel    channelName
	Tags       map[string]string
	Timestamps []NanosecondsUTC
	Values     [][]float64
}

type stringArrayBatch struct {
	Channel    channelName
	Tags       map[string]string
	Timestamps []NanosecondsUTC
	Values     [][]string
}

// channelReferenceKey is a lightweight key for map lookups (channel name + tags hash).
type channelReferenceKey struct {
	channel  channelName
	tagsHash string
}

// channelReference represents a full channel reference with key and tags.
type channelReference struct {
	channelReferenceKey                   // Embedded key for map lookups
	tags                map[string]string // Full tags for batch creation
}

type floatBuffer struct {
	ref        channelReference
	timestamps []NanosecondsUTC
	values     []float64
}

type intBuffer struct {
	ref        channelReference
	timestamps []NanosecondsUTC
	values     []int64
}

type stringBuffer struct {
	ref        channelReference
	timestamps []NanosecondsUTC
	values     []string
}

type floatArrayBuffer struct {
	ref        channelReference
	timestamps []NanosecondsUTC
	values     [][]float64
}

type stringArrayBuffer struct {
	ref        channelReference
	timestamps []NanosecondsUTC
	values     [][]string
}

// defaultMaxConcurrentFlushes limits the number of concurrent HTTP requests to prevent
// unbounded goroutine accumulation when the server is slow or failing.
const defaultMaxConcurrentFlushes = 10

type batcher struct {
	closeChan   chan struct{}
	wg          sync.WaitGroup
	flushSize   int
	flushPeriod time.Duration

	// Error handling
	errorsMu sync.Mutex // Protects errors channel and closed flag
	errors   chan error
	closed   bool

	// Concurrency control for flush goroutines
	maxConcurrentFlushes int
	flushSem             chan struct{}

	// API client
	ctx        context.Context
	apiClient  *nominalAPIClient
	datasetRID rids.NominalDataSourceOrDatasetRid

	mu                 sync.Mutex
	floatBuffers       map[channelReferenceKey]*floatBuffer
	intBuffers         map[channelReferenceKey]*intBuffer
	stringBuffers      map[channelReferenceKey]*stringBuffer
	floatArrayBuffers  map[channelReferenceKey]*floatArrayBuffer
	stringArrayBuffers map[channelReferenceKey]*stringArrayBuffer
	totalPoints        int
}

func newBatcher(
	ctx context.Context,
	apiClient *nominalAPIClient,
	datasetRID rids.NominalDataSourceOrDatasetRid,
	flushSize int,
	flushPeriod time.Duration,
) *batcher {
	return &batcher{
		closeChan:            make(chan struct{}),
		flushSize:            flushSize,
		flushPeriod:          flushPeriod,
		errors:               make(chan error, 256), // Increased from 100 to handle burst errors
		maxConcurrentFlushes: defaultMaxConcurrentFlushes,
		flushSem:             make(chan struct{}, defaultMaxConcurrentFlushes),
		ctx:                  ctx,
		apiClient:            apiClient,
		datasetRID:           datasetRID,
		floatBuffers:         make(map[channelReferenceKey]*floatBuffer),
		intBuffers:           make(map[channelReferenceKey]*intBuffer),
		stringBuffers:        make(map[channelReferenceKey]*stringBuffer),
		floatArrayBuffers:    make(map[channelReferenceKey]*floatArrayBuffer),
		stringArrayBuffers:   make(map[channelReferenceKey]*stringArrayBuffer),
	}
}

func (b *batcher) start() {
	b.wg.Add(1)
	go b.run()
}

func (b *batcher) close() error {
	// Check if already closed
	b.errorsMu.Lock()
	if b.closed {
		b.errorsMu.Unlock()
		return nil
	}
	b.closed = true
	b.errorsMu.Unlock()

	// Signal shutdown and wait for all goroutines to finish
	close(b.closeChan)
	b.wg.Wait()

	// Close the errors channel
	b.errorsMu.Lock()
	close(b.errors)
	b.errorsMu.Unlock()

	return nil
}

// reportError sends an error to the errors channel without blocking.
// If the channel is full, it attempts to drop the oldest error to make room for the new one.
// Dropped errors are logged as warnings.
// The errorsMu mutex prevents sending on a closed channel (flush goroutines may outlive run()).
func (b *batcher) reportError(err error) {
	b.errorsMu.Lock()
	defer b.errorsMu.Unlock()

	if b.closed {
		return
	}

	// Try to send error (non-blocking)
	select {
	case b.errors <- err:
		return // Success
	default:
	}

	// Channel is full - try to drop oldest error to make room
	select {
	case oldErr := <-b.errors:
		log.Printf("nominal-streaming: dropped error (buffer full): %v", oldErr)
	default:
	}

	// Try to send new error again
	select {
	case b.errors <- err:
		return // Success
	default:
		log.Printf("nominal-streaming: dropped error (buffer full): %v", err)
	}
}

// run is the main batching loop.
func (b *batcher) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()

		case <-b.closeChan:
			b.flush()
			return
		}
	}
}

func (b *batcher) addFloat(ref channelReference, timestamp NanosecondsUTC, value float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.floatBuffers[ref.channelReferenceKey]
	if !exists {
		buffer = &floatBuffer{
			ref:        ref,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([]float64, 0),
		}
		b.floatBuffers[ref.channelReferenceKey] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, value)
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *batcher) addInt(ref channelReference, timestamp NanosecondsUTC, value int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.intBuffers[ref.channelReferenceKey]
	if !exists {
		buffer = &intBuffer{
			ref:        ref,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([]int64, 0),
		}
		b.intBuffers[ref.channelReferenceKey] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, value)
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *batcher) addString(ref channelReference, timestamp NanosecondsUTC, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.stringBuffers[ref.channelReferenceKey]
	if !exists {
		buffer = &stringBuffer{
			ref:        ref,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([]string, 0),
		}
		b.stringBuffers[ref.channelReferenceKey] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, value)
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *batcher) addFloatArray(ref channelReference, timestamp NanosecondsUTC, value []float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.floatArrayBuffers[ref.channelReferenceKey]
	if !exists {
		buffer = &floatArrayBuffer{
			ref:        ref,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([][]float64, 0),
		}
		b.floatArrayBuffers[ref.channelReferenceKey] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, value)
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *batcher) addStringArray(ref channelReference, timestamp NanosecondsUTC, value []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buffer, exists := b.stringArrayBuffers[ref.channelReferenceKey]
	if !exists {
		buffer = &stringArrayBuffer{
			ref:        ref,
			timestamps: make([]NanosecondsUTC, 0),
			values:     make([][]string, 0),
		}
		b.stringArrayBuffers[ref.channelReferenceKey] = buffer
	}

	buffer.timestamps = append(buffer.timestamps, timestamp)
	buffer.values = append(buffer.values, value)
	b.totalPoints++

	if b.totalPoints >= b.flushSize {
		b.flushLocked()
	}
}

func (b *batcher) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *batcher) flushLocked() {
	if b.totalPoints == 0 {
		return
	}

	floatBatches := make([]floatBatch, 0, len(b.floatBuffers))
	for _, buffer := range b.floatBuffers {
		if len(buffer.timestamps) > 0 {
			floatBatches = append(floatBatches, floatBatch{
				Channel:    buffer.ref.channel,
				Tags:       buffer.ref.tags,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([]float64, 0)
		}
	}

	intBatches := make([]intBatch, 0, len(b.intBuffers))
	for _, buffer := range b.intBuffers {
		if len(buffer.timestamps) > 0 {
			intBatches = append(intBatches, intBatch{
				Channel:    buffer.ref.channel,
				Tags:       buffer.ref.tags,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([]int64, 0)
		}
	}

	stringBatches := make([]stringBatch, 0, len(b.stringBuffers))
	for _, buffer := range b.stringBuffers {
		if len(buffer.timestamps) > 0 {
			stringBatches = append(stringBatches, stringBatch{
				Channel:    buffer.ref.channel,
				Tags:       buffer.ref.tags,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([]string, 0)
		}
	}

	floatArrayBatches := make([]floatArrayBatch, 0, len(b.floatArrayBuffers))
	for _, buffer := range b.floatArrayBuffers {
		if len(buffer.timestamps) > 0 {
			floatArrayBatches = append(floatArrayBatches, floatArrayBatch{
				Channel:    buffer.ref.channel,
				Tags:       buffer.ref.tags,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([][]float64, 0)
		}
	}

	stringArrayBatches := make([]stringArrayBatch, 0, len(b.stringArrayBuffers))
	for _, buffer := range b.stringArrayBuffers {
		if len(buffer.timestamps) > 0 {
			stringArrayBatches = append(stringArrayBatches, stringArrayBatch{
				Channel:    buffer.ref.channel,
				Tags:       buffer.ref.tags,
				Timestamps: buffer.timestamps,
				Values:     buffer.values,
			})
			buffer.timestamps = make([]NanosecondsUTC, 0)
			buffer.values = make([][]string, 0)
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
			if err := b.sendBatches(floatBatches, intBatches, stringBatches, floatArrayBatches, stringArrayBatches); err != nil {
				b.reportError(err)
			}
		}()
	default:
		// Too many concurrent flushes - this indicates backpressure
		// Log warning but don't lose the data - it will be retried on next flush
		log.Printf("nominal-streaming: flush skipped due to backpressure (%d concurrent flushes in progress)", b.maxConcurrentFlushes)
		// Re-add the batches back to the buffers
		b.reAddBatches(floatBatches, intBatches, stringBatches, floatArrayBatches, stringArrayBatches)
	}
}

// reAddBatches puts batches back into the buffers when a flush is skipped due to backpressure.
// This ensures data is not lost and will be included in the next flush attempt.
// Note: caller must hold b.mu lock.
func (b *batcher) reAddBatches(floatBatches []floatBatch, intBatches []intBatch, stringBatches []stringBatch, floatArrayBatches []floatArrayBatch, stringArrayBatches []stringArrayBatch) {
	for _, batch := range floatBatches {
		key := channelReferenceKey{channel: batch.Channel, tagsHash: hashTags(batch.Tags)}
		buffer, exists := b.floatBuffers[key]
		if !exists {
			buffer = &floatBuffer{
				ref:        channelReference{channelReferenceKey: key, tags: batch.Tags},
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([]float64, 0),
			}
			b.floatBuffers[key] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}

	for _, batch := range intBatches {
		key := channelReferenceKey{channel: batch.Channel, tagsHash: hashTags(batch.Tags)}
		buffer, exists := b.intBuffers[key]
		if !exists {
			buffer = &intBuffer{
				ref:        channelReference{channelReferenceKey: key, tags: batch.Tags},
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([]int64, 0),
			}
			b.intBuffers[key] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}

	for _, batch := range stringBatches {
		key := channelReferenceKey{channel: batch.Channel, tagsHash: hashTags(batch.Tags)}
		buffer, exists := b.stringBuffers[key]
		if !exists {
			buffer = &stringBuffer{
				ref:        channelReference{channelReferenceKey: key, tags: batch.Tags},
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([]string, 0),
			}
			b.stringBuffers[key] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}

	for _, batch := range floatArrayBatches {
		key := channelReferenceKey{channel: batch.Channel, tagsHash: hashTags(batch.Tags)}
		buffer, exists := b.floatArrayBuffers[key]
		if !exists {
			buffer = &floatArrayBuffer{
				ref:        channelReference{channelReferenceKey: key, tags: batch.Tags},
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([][]float64, 0),
			}
			b.floatArrayBuffers[key] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}

	for _, batch := range stringArrayBatches {
		key := channelReferenceKey{channel: batch.Channel, tagsHash: hashTags(batch.Tags)}
		buffer, exists := b.stringArrayBuffers[key]
		if !exists {
			buffer = &stringArrayBuffer{
				ref:        channelReference{channelReferenceKey: key, tags: batch.Tags},
				timestamps: make([]NanosecondsUTC, 0),
				values:     make([][]string, 0),
			}
			b.stringArrayBuffers[key] = buffer
		}
		buffer.timestamps = append(batch.Timestamps, buffer.timestamps...)
		buffer.values = append(batch.Values, buffer.values...)
		b.totalPoints += len(batch.Timestamps)
	}
}

func (b *batcher) sendBatches(floatBatches []floatBatch, intBatches []intBatch, stringBatches []stringBatch, floatArrayBatches []floatArrayBatch, stringArrayBatches []stringArrayBatch) error {

	series := make([]*pb.Series, 0, len(floatBatches)+len(intBatches)+len(stringBatches)+len(floatArrayBatches)+len(stringArrayBatches))

	for _, batch := range floatBatches {
		s, err := convertFloatBatchToProto(batch)
		if err != nil {
			return fmt.Errorf("failed to convert float batch: %w", err)
		}
		series = append(series, s)
	}

	for _, batch := range intBatches {
		s, err := convertIntBatchToProto(batch)
		if err != nil {
			return fmt.Errorf("failed to convert int batch: %w", err)
		}
		series = append(series, s)
	}

	for _, batch := range stringBatches {
		s, err := convertStringBatchToProto(batch)
		if err != nil {
			return fmt.Errorf("failed to convert string batch: %w", err)
		}
		series = append(series, s)
	}

	for _, batch := range floatArrayBatches {
		s, err := convertFloatArrayBatchToProto(batch)
		if err != nil {
			return fmt.Errorf("failed to convert float array batch: %w", err)
		}
		series = append(series, s)
	}

	for _, batch := range stringArrayBatches {
		s, err := convertStringArrayBatchToProto(batch)
		if err != nil {
			return fmt.Errorf("failed to convert string array batch: %w", err)
		}
		series = append(series, s)
	}

	request := &pb.WriteRequestNominal{
		Series: series,
	}

	if err := b.apiClient.writeNominalData(b.ctx, b.datasetRID, request); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}

func hashTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte("="))
		h.Write([]byte(tags[k]))
		h.Write([]byte(";"))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func convertFloatBatchToProto(batch floatBatch) (*pb.Series, error) {
	if len(batch.Timestamps) != len(batch.Values) {
		return nil, fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
	}

	points := make([]*pb.DoublePoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		// Validate float values - NaN and Inf are not valid for protobuf/JSON serialization
		if math.IsNaN(batch.Values[i]) {
			return nil, fmt.Errorf("invalid float value at index %d: NaN is not allowed", i)
		}
		if math.IsInf(batch.Values[i], 0) {
			return nil, fmt.Errorf("invalid float value at index %d: Inf is not allowed", i)
		}
		points[i] = &pb.DoublePoint{
			Timestamp: nanosecondsToTimestampProto(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return &pb.Series{
		Channel: &pb.Channel{Name: string(batch.Channel)},
		Tags:    batch.Tags,
		Points: &pb.Points{
			PointsType: &pb.Points_DoublePoints{
				DoublePoints: &pb.DoublePoints{Points: points},
			},
		},
	}, nil
}

func convertIntBatchToProto(batch intBatch) (*pb.Series, error) {
	if len(batch.Timestamps) != len(batch.Values) {
		return nil, fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
	}

	points := make([]*pb.IntegerPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = &pb.IntegerPoint{
			Timestamp: nanosecondsToTimestampProto(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return &pb.Series{
		Channel: &pb.Channel{Name: string(batch.Channel)},
		Tags:    batch.Tags,
		Points: &pb.Points{
			PointsType: &pb.Points_IntegerPoints{
				IntegerPoints: &pb.IntegerPoints{Points: points},
			},
		},
	}, nil
}

func convertStringBatchToProto(batch stringBatch) (*pb.Series, error) {
	if len(batch.Timestamps) != len(batch.Values) {
		return nil, fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
	}

	points := make([]*pb.StringPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = &pb.StringPoint{
			Timestamp: nanosecondsToTimestampProto(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return &pb.Series{
		Channel: &pb.Channel{Name: string(batch.Channel)},
		Tags:    batch.Tags,
		Points: &pb.Points{
			PointsType: &pb.Points_StringPoints{
				StringPoints: &pb.StringPoints{Points: points},
			},
		},
	}, nil
}

func convertFloatArrayBatchToProto(batch floatArrayBatch) (*pb.Series, error) {
	if len(batch.Timestamps) != len(batch.Values) {
		return nil, fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
	}

	points := make([]*pb.DoubleArrayPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		// Validate all float values in the array
		for j, v := range batch.Values[i] {
			if math.IsNaN(v) {
				return nil, fmt.Errorf("invalid float array value at index [%d][%d]: NaN is not allowed", i, j)
			}
			if math.IsInf(v, 0) {
				return nil, fmt.Errorf("invalid float array value at index [%d][%d]: Inf is not allowed", i, j)
			}
		}
		points[i] = &pb.DoubleArrayPoint{
			Timestamp: nanosecondsToTimestampProto(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return &pb.Series{
		Channel: &pb.Channel{Name: string(batch.Channel)},
		Tags:    batch.Tags,
		Points: &pb.Points{
			PointsType: &pb.Points_ArrayPoints{
				ArrayPoints: &pb.ArrayPoints{
					ArrayType: &pb.ArrayPoints_DoubleArrayPoints{
						DoubleArrayPoints: &pb.DoubleArrayPoints{Points: points},
					},
				},
			},
		},
	}, nil
}

func convertStringArrayBatchToProto(batch stringArrayBatch) (*pb.Series, error) {
	if len(batch.Timestamps) != len(batch.Values) {
		return nil, fmt.Errorf("timestamp/value length mismatch: %d timestamps, %d values", len(batch.Timestamps), len(batch.Values))
	}

	points := make([]*pb.StringArrayPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = &pb.StringArrayPoint{
			Timestamp: nanosecondsToTimestampProto(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return &pb.Series{
		Channel: &pb.Channel{Name: string(batch.Channel)},
		Tags:    batch.Tags,
		Points: &pb.Points{
			PointsType: &pb.Points_ArrayPoints{
				ArrayPoints: &pb.ArrayPoints{
					ArrayType: &pb.ArrayPoints_StringArrayPoints{
						StringArrayPoints: &pb.StringArrayPoints{Points: points},
					},
				},
			},
		},
	}, nil
}

// nanosecondsToTimestampProto converts nanoseconds to protobuf Timestamp.
// Handles negative timestamps correctly by ensuring Nanos is always in [0, 999999999].
func nanosecondsToTimestampProto(nanos NanosecondsUTC) *timestamppb.Timestamp {
	nanosInt64 := int64(nanos)
	seconds := nanosInt64 / 1_000_000_000
	remainingNanos := nanosInt64 % 1_000_000_000

	// For negative timestamps, the modulo can be negative.
	// Protobuf Timestamp requires Nanos to be in [0, 999999999].
	// Adjust by borrowing from seconds.
	if remainingNanos < 0 {
		seconds--
		remainingNanos += 1_000_000_000
	}

	return &timestamppb.Timestamp{
		Seconds: seconds,
		Nanos:   int32(remainingNanos),
	}
}

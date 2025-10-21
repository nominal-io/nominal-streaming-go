package nominal_streaming

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
	nominalapi "github.com/nominal-io/nominal-api-go/io/nominal/api"
	writerapi "github.com/nominal-io/nominal-api-go/storage/writer/api"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/safelong"
)

type channelName string

type floatBatch struct {
	Channel    channelName       `json:"channel"`
	Tags       map[string]string `json:"tags"`
	Timestamps []NanosecondsUTC  `json:"timestamps"`
	Values     []float64         `json:"values"`
}

type intBatch struct {
	Channel    channelName       `json:"channel"`
	Tags       map[string]string `json:"tags"`
	Timestamps []NanosecondsUTC  `json:"timestamps"`
	Values     []int64           `json:"values"`
}

type stringBatch struct {
	Channel    channelName       `json:"channel"`
	Tags       map[string]string `json:"tags"`
	Timestamps []NanosecondsUTC  `json:"timestamps"`
	Values     []string          `json:"values"`
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

type batcher struct {
	closeChan   chan struct{}
	wg          sync.WaitGroup
	flushSize   int
	flushPeriod time.Duration

	// Error handling
	errors        chan error
	droppedErrors atomic.Int64
	closed        atomic.Bool

	// API client
	writerClient writerapi.NominalChannelWriterServiceClient
	authToken    bearertoken.Token
	datasetRID   rids.NominalDataSourceOrDatasetRid

	mu            sync.Mutex
	floatBuffers  map[channelReferenceKey]*floatBuffer
	intBuffers    map[channelReferenceKey]*intBuffer
	stringBuffers map[channelReferenceKey]*stringBuffer
	totalPoints   int
}

func newBatcher(
	writerClient writerapi.NominalChannelWriterServiceClient,
	authToken bearertoken.Token,
	datasetRID rids.NominalDataSourceOrDatasetRid,
	flushSize int,
	flushPeriod time.Duration,
) *batcher {
	return &batcher{
		closeChan:     make(chan struct{}),
		flushSize:     flushSize,
		flushPeriod:   flushPeriod,
		errors:        make(chan error, 100),
		writerClient:  writerClient,
		authToken:     authToken,
		datasetRID:    datasetRID,
		floatBuffers:  make(map[channelReferenceKey]*floatBuffer),
		intBuffers:    make(map[channelReferenceKey]*intBuffer),
		stringBuffers: make(map[channelReferenceKey]*stringBuffer),
	}
}

func (b *batcher) start() {
	b.wg.Add(1)
	go b.run()
}

func (b *batcher) close() error {
	b.closed.Store(true)
	close(b.closeChan)
	b.wg.Wait()
	close(b.errors)
	return nil
}

// reportError sends an error to the errors channel without blocking.
// If the channel is full, it attempts to drop the oldest error to make room for the new one.
// Dropped errors are counted and can be retrieved via DroppedErrorCount().
func (b *batcher) reportError(err error) {
	if b.closed.Load() {
		return
	}

	// Try to send error
	select {
	case b.errors <- err:
		return // Success
	default:
	}

	// Channel is full - try to drop oldest error to make room
	select {
	case <-b.errors:
		b.droppedErrors.Add(1)
	default:
	}

	// Try to send new error again
	select {
	case b.errors <- err:
		return // Success
	default:
		b.droppedErrors.Add(1)
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

	b.totalPoints = 0

	go b.sendBatches(floatBatches, intBatches, stringBatches)
}

func (b *batcher) sendBatches(floatBatches []floatBatch, intBatches []intBatch, stringBatches []stringBatch) {
	recordsBatches := make([]writerapi.RecordsBatchExternal, 0, len(floatBatches)+len(intBatches)+len(stringBatches))

	for _, batch := range floatBatches {
		recordsBatches = append(recordsBatches, convertFloatBatch(batch))
	}

	for _, batch := range intBatches {
		recordsBatches = append(recordsBatches, convertIntBatch(batch))
	}

	for _, batch := range stringBatches {
		recordsBatches = append(recordsBatches, convertStringBatch(batch))
	}

	if err := b.sendToNominal(recordsBatches); err != nil {
		b.reportError(fmt.Errorf("failed to send batch: %w", err))
	}
}

func (b *batcher) sendToNominal(batches []writerapi.RecordsBatchExternal) error {
	request := writerapi.WriteBatchesRequestExternal{
		Batches:       batches,
		DataSourceRid: b.datasetRID,
	}

	ctx := context.Background()
	if err := b.writerClient.WriteBatches(ctx, b.authToken, request); err != nil {
		return fmt.Errorf("API call failed: %w", err)
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

func convertFloatBatch(batch floatBatch) writerapi.RecordsBatchExternal {
	points := make([]writerapi.DoublePoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = writerapi.DoublePoint{
			Timestamp: nanosecondsToTimestamp(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return writerapi.RecordsBatchExternal{
		Channel: nominalapi.Channel(batch.Channel),
		Tags:    convertTags(batch.Tags),
		Points:  writerapi.NewPointsExternalFromDouble(points),
	}
}

func convertIntBatch(batch intBatch) writerapi.RecordsBatchExternal {
	points := make([]writerapi.IntPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = writerapi.IntPoint{
			Timestamp: nanosecondsToTimestamp(batch.Timestamps[i]),
			Value:     int(batch.Values[i]),
		}
	}

	return writerapi.RecordsBatchExternal{
		Channel: nominalapi.Channel(batch.Channel),
		Tags:    convertTags(batch.Tags),
		Points:  writerapi.NewPointsExternalFromInt(points),
	}
}

func convertStringBatch(batch stringBatch) writerapi.RecordsBatchExternal {
	points := make([]writerapi.StringPoint, len(batch.Timestamps))
	for i := range batch.Timestamps {
		points[i] = writerapi.StringPoint{
			Timestamp: nanosecondsToTimestamp(batch.Timestamps[i]),
			Value:     batch.Values[i],
		}
	}

	return writerapi.RecordsBatchExternal{
		Channel: nominalapi.Channel(batch.Channel),
		Tags:    convertTags(batch.Tags),
		Points:  writerapi.NewPointsExternalFromString(points),
	}
}

// convertTags converts tags map to Nominal API format.
func convertTags(tags map[string]string) map[nominalapi.TagName]nominalapi.TagValue {
	result := make(map[nominalapi.TagName]nominalapi.TagValue, len(tags))
	for k, v := range tags {
		result[nominalapi.TagName(k)] = nominalapi.TagValue(v)
	}
	return result
}

// nanosecondsToTimestamp converts nanoseconds to Nominal API Timestamp.
func nanosecondsToTimestamp(nanos NanosecondsUTC) nominalapi.Timestamp {
	nanosInt64 := int64(nanos)
	seconds := nanosInt64 / 1_000_000_000
	remainingNanos := nanosInt64 % 1_000_000_000

	return nominalapi.Timestamp{
		Seconds: safelong.SafeLong(seconds),
		Nanos:   safelong.SafeLong(remainingNanos),
	}
}

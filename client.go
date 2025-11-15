package nominal_streaming

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
	writerapi "github.com/nominal-io/nominal-api-go/storage/writer/api"
	pb "github.com/nominal-io/nominal-streaming-go/proto"
	"github.com/palantir/conjure-go-runtime/v2/conjure-go-client/httpclient"
	"github.com/palantir/pkg/bearertoken"
	"google.golang.org/protobuf/proto"
)

type nominalAPIClient struct {
	writerClient writerapi.NominalChannelWriterServiceClient
	authToken    bearertoken.Token
}

// Overrides the Content-Type header to application/x-protobuf for requests to the
// /storage/writer/v1/nominal/ endpoint. For binary requests, conjure-go-client sets
// Content-Type to application/octet-stream by default.
func protobufContentTypeMiddleware() httpclient.Middleware {
	return httpclient.MiddlewareFunc(func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/storage/writer/v1/nominal/") {
			req.Header.Set("Content-Type", "application/x-protobuf")
		}
		return next.RoundTrip(req)
	})
}

func newNominalAPIClient(baseURL string, authToken string) (*nominalAPIClient, error) {
	conjureClient, err := httpclient.NewClient(
		httpclient.WithBaseURLs([]string{baseURL}),
		httpclient.WithMiddleware(protobufContentTypeMiddleware()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create conjure client: %w", err)
	}

	return &nominalAPIClient{
		writerClient: writerapi.NewNominalChannelWriterServiceClient(conjureClient),
		authToken:    bearertoken.Token(authToken),
	}, nil
}

func (c *nominalAPIClient) writeNominalData(ctx context.Context, datasetRID rids.NominalDataSourceOrDatasetRid, request *pb.WriteRequestNominal) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	requestBody := httpclient.RequestBodyInMemory(bytes.NewBuffer(data))

	return c.writerClient.WriteNominalBatches(ctx, c.authToken, datasetRID, requestBody)
}

func (c *nominalAPIClient) writeLogs(ctx context.Context, datasetRID rids.NominalDataSourceOrDatasetRid, request writerapi.WriteLogsRequest) error {
	return c.writerClient.WriteLogs(ctx, c.authToken, datasetRID, request)
}

type Client struct {
	baseURL   string
	authToken string
}

type Option func(*Client) error

func WithBaseURL(baseURL string) Option {
	return func(c *Client) error {
		if baseURL == "" {
			return fmt.Errorf("base URL cannot be empty")
		}
		c.baseURL = baseURL
		return nil
	}
}

func NewClient(apiKey string, options ...Option) (*Client, error) {
	client := &Client{
		baseURL:   "https://api.gov.nominal.io/api",
		authToken: apiKey,
	}

	for _, option := range options {
		if err := option(client); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	return client, nil
}

func (c *Client) NewDatasetStream(ctx context.Context, datasetRID string, options ...DatasetStreamOption) (*DatasetStream, <-chan error, error) {
	var rid rids.NominalDataSourceOrDatasetRid
	if err := rid.UnmarshalText([]byte(datasetRID)); err != nil {
		return nil, nil, fmt.Errorf("invalid dataset RID: %w", err)
	}

	apiClient, err := newNominalAPIClient(c.baseURL, c.authToken)
	if err != nil {
		return nil, nil, err
	}
	batcher := newBatcher(ctx, apiClient, rid, 65_536, 500*time.Millisecond)

	stream := &DatasetStream{
		datasetRID:         datasetRID,
		batcher:            batcher,
		floatStreams:       make(map[channelReferenceKey]*ChannelStream[float64]),
		intStreams:         make(map[channelReferenceKey]*ChannelStream[int64]),
		stringStreams:      make(map[channelReferenceKey]*ChannelStream[string]),
		floatArrayStreams:  make(map[channelReferenceKey]*ChannelStream[[]float64]),
		stringArrayStreams: make(map[channelReferenceKey]*ChannelStream[[]string]),
	}

	for _, option := range options {
		if err := option(stream); err != nil {
			return nil, nil, fmt.Errorf("failed to apply stream option: %w", err)
		}
	}

	batcher.start()

	return stream, batcher.errors, nil
}

func (c *Client) NewDatasetLogStream(ctx context.Context, datasetRID string, options ...DatasetLogStreamOption) (*DatasetLogStream, <-chan error, error) {
	var rid rids.NominalDataSourceOrDatasetRid
	if err := rid.UnmarshalText([]byte(datasetRID)); err != nil {
		return nil, nil, fmt.Errorf("invalid dataset RID: %w", err)
	}

	apiClient, err := newNominalAPIClient(c.baseURL, c.authToken)
	if err != nil {
		return nil, nil, err
	}
	logBatcher := newLogBatcher(ctx, apiClient, rid, 65_536, 500*time.Millisecond)

	stream := &DatasetLogStream{
		datasetRID: datasetRID,
		batcher:    logBatcher,
		logStreams: make(map[channelReferenceKey]*LogChannelStream),
	}

	for _, option := range options {
		if err := option(stream); err != nil {
			return nil, nil, fmt.Errorf("failed to apply stream option: %w", err)
		}
	}

	logBatcher.start()

	return stream, logBatcher.errors, nil
}

func (c *Client) Close() error {
	return nil
}

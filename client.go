package nominal_streaming

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nominal-io/nominal-api-go/api/rids"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/rid"
)

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	authToken  bearertoken.Token
}

// Option is a function that configures a Client.
type Option func(*Client) error

// WithBaseURL sets a custom base URL for the API.
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
		apiKey:     apiKey,
		baseURL:    "https://api.gov.nominal.io/api",
		httpClient: &http.Client{Timeout: 30 * time.Second},
		authToken:  bearertoken.Token(apiKey),
	}

	for _, option := range options {
		if err := option(client); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	return client, nil
}

func (c *Client) NewDatasetStream(ctx context.Context, datasetRID rids.NominalDataSourceOrDatasetRid, options ...DatasetStreamOption) (*DatasetStream, error) {

	batcher := newBatcher(ctx, c.httpClient, c.baseURL, c.authToken, datasetRID, 65_536, 500*time.Millisecond)
	batcher.start()

	stream := &DatasetStream{
		datasetRID:    datasetRID,
		batcher:       batcher,
		floatStreams:  make(map[channelReferenceKey]*ChannelStream[float64]),
		intStreams:    make(map[channelReferenceKey]*ChannelStream[int64]),
		stringStreams: make(map[channelReferenceKey]*ChannelStream[string]),
	}

	for _, option := range options {
		if err := option(stream); err != nil {
			return nil, fmt.Errorf("failed to apply stream option: %w", err)
		}
	}

	return stream, nil
}

func (c *Client) Close() error {
	return nil
}

// ParseDatasetRID parses a dataset RID string into a typed RID.
// Returns an error if the RID string is invalid.
func ParseDatasetRID(ridString string) (rids.NominalDataSourceOrDatasetRid, error) {
	parsedRID, err := rid.ParseRID(ridString)
	if err != nil {
		return rids.NominalDataSourceOrDatasetRid{}, fmt.Errorf("invalid dataset RID %q: %w", ridString, err)
	}
	return rids.NominalDataSourceOrDatasetRid(parsedRID), nil
}

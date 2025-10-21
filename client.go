package nominal_streaming

import (
	"fmt"
	"time"
)

type Client struct {
	apiKey  string
	baseURL string
}

func NewClient(opts ...Option) (*Client, error) {
	client := &Client{
		baseURL: "https://api.gov.nominal.io/api",
	}

	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	if client.apiKey == "" {
		return nil, ErrMissingAPIKey
	}

	return client, nil
}

func (c *Client) NewStream(datasetRID string, opts ...DatasetStreamOption) (*DatasetStream, error) {

	batcher := newBatcher(c.apiKey, c.baseURL, datasetRID, 65_536, 500*time.Millisecond)
	batcher.start()

	stream := &DatasetStream{
		datasetRID:    datasetRID,
		batcher:       batcher,
		floatStreams:  make(map[channelReferenceKey]*ChannelStream[float64]),
		intStreams:    make(map[channelReferenceKey]*ChannelStream[int64]),
		stringStreams: make(map[channelReferenceKey]*ChannelStream[string]),
	}

	for _, opt := range opts {
		if err := opt(stream); err != nil {
			return nil, fmt.Errorf("failed to apply stream option: %w", err)
		}
	}

	return stream, nil
}

func (c *Client) Close() error {
	return nil
}

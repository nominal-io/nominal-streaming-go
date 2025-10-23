package nominal_streaming

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	pb "github.com/nominal-io/nominal-streaming/proto"
	"google.golang.org/protobuf/proto"
)

type retryConfig struct {
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
}

func defaultRetryConfig() retryConfig {
	return retryConfig{
		maxRetries:     3,
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     10 * time.Second,
		backoffFactor:  2.0,
	}
}

func isRetryableStatusCode(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests, // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

type nominalAPIClient struct {
	httpClient  *http.Client
	baseURL     string
	authToken   string
	retryConfig retryConfig
}

func newNominalAPIClient(httpClient *http.Client, baseURL string, authToken string) *nominalAPIClient {
	return &nominalAPIClient{
		httpClient:  httpClient,
		baseURL:     baseURL,
		authToken:   authToken,
		retryConfig: defaultRetryConfig(),
	}
}

func (c *nominalAPIClient) writeNominalData(ctx context.Context, datasetRID string, request *pb.WriteRequestNominal) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	url := fmt.Sprintf("%s/storage/writer/v1/nominal/%s", c.baseURL, datasetRID)

	return retryWithBackoff(ctx, c.retryConfig, func() error {
		return c.doRequest(ctx, url, data)
	})
}

func (c *nominalAPIClient) doRequest(ctx context.Context, url string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+c.authToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &retryableError{err: fmt.Errorf("failed to send request: %w", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	err = fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))

	if isRetryableStatusCode(resp.StatusCode) {
		return &retryableError{err: err}
	}

	return err
}

// retryableError wraps an error to indicate it should be retried.
type retryableError struct {
	err error
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	return e.err
}

func retryWithBackoff(ctx context.Context, config retryConfig, fn func() error) error {
	var lastErr error
	backoff := config.initialBackoff

	for attempt := 0; attempt <= config.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled before attempt %d: %w", attempt+1, err)
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		var retryErr *retryableError
		isRetryable := false
		if e, ok := err.(*retryableError); ok {
			isRetryable = true
			retryErr = e
		}

		if !isRetryable || attempt >= config.maxRetries {
			if retryErr != nil {
				return retryErr.err
			}
			return err
		}

		sleepWithJitter(ctx, backoff)
		backoff = calculateNextBackoff(config, backoff)
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", config.maxRetries, lastErr)
}

func calculateNextBackoff(config retryConfig, current time.Duration) time.Duration {
	next := time.Duration(float64(current) * config.backoffFactor)
	if next > config.maxBackoff {
		return config.maxBackoff
	}
	return next
}

// sleepWithJitter adds +/-25% jitter to avoid thundering herd.
func sleepWithJitter(ctx context.Context, base time.Duration) {
	jitter := time.Duration(float64(base) * (0.75 + 0.5*rand.Float64()))
	timer := time.NewTimer(jitter)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	authToken  string
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
		apiKey:     apiKey,
		baseURL:    "https://api.gov.nominal.io/api",
		httpClient: &http.Client{Timeout: 30 * time.Second},
		authToken:  apiKey,
	}

	for _, option := range options {
		if err := option(client); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	return client, nil
}

func (c *Client) NewDatasetStream(ctx context.Context, datasetRID string, options ...DatasetStreamOption) (*DatasetStream, <-chan error, error) {
	apiClient := newNominalAPIClient(c.httpClient, c.baseURL, c.authToken)
	batcher := newBatcher(ctx, apiClient, datasetRID, 65_536, 500*time.Millisecond)

	stream := &DatasetStream{
		datasetRID:    datasetRID,
		batcher:       batcher,
		floatStreams:  make(map[channelReferenceKey]*ChannelStream[float64]),
		intStreams:    make(map[channelReferenceKey]*ChannelStream[int64]),
		stringStreams: make(map[channelReferenceKey]*ChannelStream[string]),
	}

	for _, option := range options {
		if err := option(stream); err != nil {
			return nil, nil, fmt.Errorf("failed to apply stream option: %w", err)
		}
	}

	batcher.start()

	return stream, batcher.errors, nil
}

func (c *Client) Close() error {
	return nil
}

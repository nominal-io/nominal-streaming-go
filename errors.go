package nominal_streaming

import "errors"

// Common errors returned by the client.
var (
	// ErrMissingAPIKey is returned when no API key is provided.
	ErrMissingAPIKey = errors.New("api key is required")

	// ErrChannelClosed is returned when trying to send on a closed channel.
	ErrChannelClosed = errors.New("channel is closed")
)

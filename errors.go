package nominal_streaming

import "errors"

// Common errors returned by the client.
var (
	// ErrChannelClosed is returned when trying to send on a closed channel.
	ErrChannelClosed = errors.New("channel is closed")
)

package nominal

import (
	"fmt"
)

// Option is a function that configures a Client.
type Option func(*Client) error

// WithAPIKey sets the API key for authentication.
func WithAPIKey(apiKey string) Option {
	return func(c *Client) error {
		if apiKey == "" {
			return fmt.Errorf("api key cannot be empty")
		}
		c.apiKey = apiKey
		return nil
	}
}

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

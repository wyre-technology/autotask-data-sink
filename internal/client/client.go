package client

import (
	"net/http"
)

// Logger defines the interface for logging operations
type Logger interface {
	Error(msg string, fields map[string]interface{})
}

type Client struct {
	httpClient *http.Client
	logger     Logger
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			c.logger.Error("Failed to close response body", map[string]interface{}{
				"error": cerr.Error(),
			})
		}
	}()
	return resp, nil
}

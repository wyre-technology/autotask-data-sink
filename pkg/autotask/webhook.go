package autotask

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// WebhookHandler handles webhook operations
type WebhookHandler struct {
	client     *Client
	webhookURL string
}

// NewWebhookHandler creates a new WebhookHandler
func NewWebhookHandler(client *Client, webhookURL string) *WebhookHandler {
	return &WebhookHandler{
		client:     client,
		webhookURL: webhookURL,
	}
}

// HandleWebhook processes a webhook event
func (h *WebhookHandler) HandleWebhook(ctx context.Context, payload map[string]interface{}) error {
	// Convert payload to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.webhookURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook request failed with status %d", resp.StatusCode)
	}

	return nil
}

package autotask

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWebhookHandler(t *testing.T) {
	// Create a test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	}))
	defer ts.Close()

	// Create a client
	client := &Client{}

	// Create a webhook handler
	handler := NewWebhookHandler(client, ts.URL)

	// Test webhook handling
	ctx := context.Background()
	err := handler.HandleWebhook(ctx, map[string]interface{}{
		"event": "test_event",
		"data":  "test_data",
	})

	assert.NoError(t, err)

	// Test error case
	ts.Close()
	err = handler.HandleWebhook(ctx, map[string]interface{}{
		"event": "test_event",
		"data":  "test_data",
	})
	assert.Error(t, err)
}

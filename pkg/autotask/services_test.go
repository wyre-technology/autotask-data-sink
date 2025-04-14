package autotask

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServiceHandlers(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			// Handle GET
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"data": "get"}`)); err != nil {
				t.Errorf("Failed to write response: %v", err)
			}
		case http.MethodPost:
			// Handle POST
			w.WriteHeader(http.StatusCreated)
			if _, err := w.Write([]byte(`{"data": "post"}`)); err != nil {
				t.Errorf("Failed to write response: %v", err)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	// Create test server
	server := httptest.NewServer(handler)
	defer server.Close()

	// Test GET request
	req, err := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}
}

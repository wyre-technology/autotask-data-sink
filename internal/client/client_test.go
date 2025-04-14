package client

import (
	"net/http"
	"testing"
)

func TestClient(t *testing.T) {
	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"test": "response"}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	})

	http.HandleFunc("/error", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"error": "test error"}`)); err != nil {
			t.Errorf("Failed to write response: %v", err)
		}
	})
}

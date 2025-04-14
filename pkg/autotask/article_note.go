package autotask

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ArticleNoteHandler handles ArticleNote entity operations
type ArticleNoteHandler struct {
	*BaseEntityHandler
}

// NewArticleNoteHandler creates a new ArticleNoteHandler
func NewArticleNoteHandler(client *Client) *ArticleNoteHandler {
	base := NewBaseEntityHandler(client, EntityTypeArticleNote, "ArticleNotes")
	return &ArticleNoteHandler{base}
}

// Get retrieves all article notes
func (h *ArticleNoteHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// Use the standard approach
	notes, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return notes, nil
	}

	// If that fails, try with empty filter
	return h.client.queryWithEmptyFilter(ctx, h.entityEndpoint)
}

// GetByID retrieves a single article note by ID
func (h *ArticleNoteHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	notes, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(notes) > 0 {
		return notes[0], nil
	}

	return nil, err
}

// GetSince retrieves all article notes modified since the given time
func (h *ArticleNoteHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	notes, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return notes, nil
	}

	// If that fails, get all notes and filter client-side
	notes, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter notes based on last modified time
	var filtered []map[string]interface{}
	for _, note := range notes {
		if lastModified, ok := note["lastActivityDate"]; ok {
			if timeVal, err := parseTime(lastModified); err == nil {
				if !timeVal.Before(since) {
					filtered = append(filtered, note)
				}
			}
		}
	}

	return filtered, nil
}

// GetByArticleID retrieves all notes for a specific article
func (h *ArticleNoteHandler) GetByArticleID(ctx context.Context, articleID int64) ([]map[string]interface{}, error) {
	// Try using the query endpoint with a filter on articleID
	filter := []map[string]interface{}{
		{
			"op":    "eq",
			"field": "articleID",
			"value": articleID,
		},
	}

	reqBody := map[string]interface{}{
		"filter": filter,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := h.entityEndpoint + "/query"
	req, err := h.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var resp struct {
		Items []map[string]interface{} `json:"items"`
	}

	err = h.client.ExecuteConcurrent(ctx, h.entityEndpoint, func() error {
		_, err := h.client.Do(req, &resp)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return resp.Items, nil
}

// Validate validates an article note entity
func (h *ArticleNoteHandler) Validate(note map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "articleID", "note"}
	for _, field := range requiredFields {
		if _, ok := note[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeArticleNote,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := note["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticleNote,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := note["articleID"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticleNote,
			Field:      "articleID",
			Message:    "articleID must be a number",
		}
	}

	if _, ok := note["note"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticleNote,
			Field:      "note",
			Message:    "note must be a string",
		}
	}

	// Validate created date if present
	if createdStr, ok := note["created"].(string); ok {
		if _, err := parseTime(createdStr); err != nil {
			return &ValidationError{
				EntityType: EntityTypeArticleNote,
				Field:      "created",
				Message:    "created must be a valid date/time",
			}
		}
	}

	return nil
}

package autotask

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ConfigurationItemHandler handles ConfigurationItem entity operations
type ConfigurationItemHandler struct {
	*BaseEntityHandler
}

// NewConfigurationItemHandler creates a new ConfigurationItemHandler
func NewConfigurationItemHandler(client *Client) *ConfigurationItemHandler {
	base := NewBaseEntityHandler(client, EntityTypeConfigurationItem, "ConfigurationItems")
	return &ConfigurationItemHandler{base}
}

// Get retrieves all configuration items
func (h *ConfigurationItemHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// Use the standard approach
	items, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return items, nil
	}

	// If that fails, try with empty filter
	return h.client.queryWithEmptyFilter(ctx, h.entityEndpoint)
}

// GetByID retrieves a single configuration item by ID
func (h *ConfigurationItemHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	items, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(items) > 0 {
		return items[0], nil
	}

	return nil, err
}

// GetSince retrieves all configuration items modified since the given time
func (h *ConfigurationItemHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	items, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return items, nil
	}

	// If that fails, get all items and filter client-side
	items, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter items based on last modified time
	var filtered []map[string]interface{}
	for _, item := range items {
		if lastModified, ok := item["lastActivityDate"]; ok {
			if timeVal, err := parseTime(lastModified); err == nil {
				if !timeVal.Before(since) {
					filtered = append(filtered, item)
				}
			}
		}
	}

	return filtered, nil
}

// GetByCompanyID retrieves all configuration items for a specific company
func (h *ConfigurationItemHandler) GetByCompanyID(ctx context.Context, companyID int64) ([]map[string]interface{}, error) {
	// Try using the query endpoint with a filter on companyID
	filter := []map[string]interface{}{
		{
			"op":    "eq",
			"field": "companyID",
			"value": companyID,
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

// Validate validates a configuration item entity
func (h *ConfigurationItemHandler) Validate(item map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "companyID", "name", "type", "status"}
	for _, field := range requiredFields {
		if _, ok := item[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeConfigurationItem,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := item["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeConfigurationItem,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := item["companyID"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeConfigurationItem,
			Field:      "companyID",
			Message:    "companyID must be a number",
		}
	}

	if _, ok := item["name"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeConfigurationItem,
			Field:      "name",
			Message:    "name must be a string",
		}
	}

	if _, ok := item["type"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeConfigurationItem,
			Field:      "type",
			Message:    "type must be a string",
		}
	}

	if _, ok := item["status"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeConfigurationItem,
			Field:      "status",
			Message:    "status must be a string",
		}
	}

	// Validate dates if present
	if createdStr, ok := item["created"].(string); ok {
		if _, err := parseTime(createdStr); err != nil {
			return &ValidationError{
				EntityType: EntityTypeConfigurationItem,
				Field:      "created",
				Message:    "created must be a valid date/time",
			}
		}
	}

	if lastModifiedStr, ok := item["lastModified"].(string); ok {
		if _, err := parseTime(lastModifiedStr); err != nil {
			return &ValidationError{
				EntityType: EntityTypeConfigurationItem,
				Field:      "lastModified",
				Message:    "lastModified must be a valid date/time",
			}
		}
	}

	return nil
}

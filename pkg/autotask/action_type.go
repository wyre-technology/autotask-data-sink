package autotask

import (
	"context"
	"time"
)

// ActionTypeHandler handles ActionType entity operations
type ActionTypeHandler struct {
	*BaseEntityHandler
}

// NewActionTypeHandler creates a new ActionTypeHandler
func NewActionTypeHandler(client *Client) *ActionTypeHandler {
	base := NewBaseEntityHandler(client, EntityTypeActionType, "ActionTypes")
	return &ActionTypeHandler{base}
}

// Get retrieves all action types
func (h *ActionTypeHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// Use the standard approach
	actionTypes, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return actionTypes, nil
	}

	// If that fails, try with empty filter
	return h.client.queryWithEmptyFilter(ctx, h.entityEndpoint)
}

// GetByID retrieves a single action type by ID
func (h *ActionTypeHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	actionTypes, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(actionTypes) > 0 {
		return actionTypes[0], nil
	}

	return nil, err
}

// GetSince retrieves all action types modified since the given time
func (h *ActionTypeHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	actionTypes, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return actionTypes, nil
	}

	// If that fails, try with lastActivityDate field
	actionTypes, err = h.client.queryWithDateFilter(ctx, h.entityEndpoint, "lastActivityDate", since)
	if err == nil {
		return actionTypes, nil
	}

	// If that also fails, try with LastActivityDate (case sensitivity matters in some API versions)
	actionTypes, err = h.client.queryWithDateFilter(ctx, h.entityEndpoint, "LastActivityDate", since)
	if err == nil {
		return actionTypes, nil
	}

	// If all direct queries fail, get all action types and filter client-side
	actionTypes, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter action types based on last modified time
	var filtered []map[string]interface{}
	for _, actionType := range actionTypes {
		// Check both lastActivityDate and LastActivityDate
		var lastModified time.Time
		var parseErr error

		if val, ok := actionType["lastActivityDate"]; ok {
			lastModified, parseErr = parseTime(val)
		} else if val, ok := actionType["LastActivityDate"]; ok {
			lastModified, parseErr = parseTime(val)
		}

		if parseErr == nil && !lastModified.Before(since) {
			filtered = append(filtered, actionType)
		}
	}

	return filtered, nil
}

// Validate validates an action type entity
func (h *ActionTypeHandler) Validate(actionType map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "name", "description", "active"}
	for _, field := range requiredFields {
		if _, ok := actionType[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeActionType,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if id, ok := actionType["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeActionType,
			Field:      "id",
			Message:    "id must be a number",
		}
	} else if id <= 0 {
		return &ValidationError{
			EntityType: EntityTypeActionType,
			Field:      "id",
			Message:    "id must be a positive number",
		}
	}

	if _, ok := actionType["name"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeActionType,
			Field:      "name",
			Message:    "name must be a string",
		}
	}

	if _, ok := actionType["description"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeActionType,
			Field:      "description",
			Message:    "description must be a string",
		}
	}

	if _, ok := actionType["active"].(bool); !ok {
		return &ValidationError{
			EntityType: EntityTypeActionType,
			Field:      "active",
			Message:    "active must be a boolean",
		}
	}

	// Optional field validations
	if displayName, ok := actionType["displayName"].(string); ok {
		if len(displayName) == 0 {
			return &ValidationError{
				EntityType: EntityTypeActionType,
				Field:      "displayName",
				Message:    "displayName if provided must not be empty",
			}
		}
	}

	if systemActionType, ok := actionType["systemActionType"].(bool); ok {
		if systemActionType && actionType["systemActionTypeID"] == nil {
			return &ValidationError{
				EntityType: EntityTypeActionType,
				Field:      "systemActionTypeID",
				Message:    "systemActionTypeID is required when systemActionType is true",
			}
		}
	}

	return nil
}

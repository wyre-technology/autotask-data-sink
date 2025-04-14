package autotask

import (
	"context"
	"time"
)

// ResourceHandler handles Resource entity operations
type ResourceHandler struct {
	*BaseEntityHandler
}

// NewResourceHandler creates a new ResourceHandler
func NewResourceHandler(client *Client) *ResourceHandler {
	base := NewBaseEntityHandler(client, EntityTypeResource, "Resources")
	return &ResourceHandler{base}
}

// Get retrieves all resources
func (h *ResourceHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// First try the standard approach
	resources, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return resources, nil
	}

	// If that fails, try the resource-specific method
	return h.client.GetResources(ctx)
}

// GetByID retrieves a single resource by ID
func (h *ResourceHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	resources, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(resources) > 0 {
		return resources[0], nil
	}

	return nil, err
}

// GetSince retrieves all resources modified since the given time
func (h *ResourceHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	resources, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return resources, nil
	}

	// If that fails, get all resources and filter client-side
	resources, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter resources based on last modified time
	var filtered []map[string]interface{}
	for _, resource := range resources {
		lastModified, err := parseTime(resource["lastActivityDate"])
		if err != nil {
			continue
		}
		if lastModified.After(since) {
			filtered = append(filtered, resource)
		}
	}

	return filtered, nil
}

// Validate validates a resource entity
func (h *ResourceHandler) Validate(resource map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "firstName", "lastName", "email", "resourceType"}
	for _, field := range requiredFields {
		if _, ok := resource[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeResource,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := resource["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeResource,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := resource["firstName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeResource,
			Field:      "firstName",
			Message:    "firstName must be a string",
		}
	}

	if _, ok := resource["lastName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeResource,
			Field:      "lastName",
			Message:    "lastName must be a string",
		}
	}

	if _, ok := resource["email"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeResource,
			Field:      "email",
			Message:    "email must be a string",
		}
	}

	if _, ok := resource["resourceType"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeResource,
			Field:      "resourceType",
			Message:    "resourceType must be a string",
		}
	}

	// Validate active status if present
	if active, ok := resource["active"]; ok {
		if _, ok := active.(bool); !ok {
			return &ValidationError{
				EntityType: EntityTypeResource,
				Field:      "active",
				Message:    "active must be a boolean",
			}
		}
	}

	return nil
}

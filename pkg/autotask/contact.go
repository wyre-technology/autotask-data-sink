package autotask

import (
	"context"
	"time"
)

// ContactHandler handles Contact entity operations
type ContactHandler struct {
	*BaseEntityHandler
}

// NewContactHandler creates a new ContactHandler
func NewContactHandler(client *Client) *ContactHandler {
	base := NewBaseEntityHandler(client, EntityTypeContact, "Contacts")
	return &ContactHandler{base}
}

// Get retrieves all contacts
func (h *ContactHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// First try the standard approach
	contacts, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return contacts, nil
	}

	// If that fails, try contact-specific approaches
	contacts, err = h.client.getContactsSequentially(ctx)
	if err == nil {
		return contacts, nil
	}

	// As a last resort, try getting contacts individually
	return h.client.getContactsIndividually(ctx)
}

// GetByID retrieves a single contact by ID
func (h *ContactHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// First try the standard approach
	contacts, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(contacts) > 0 {
		return contacts[0], nil
	}

	// If that fails, try the contact-specific approach
	return h.client.GetContact(ctx, id)
}

// GetSince retrieves all contacts modified since the given time
func (h *ContactHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	contacts, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return contacts, nil
	}

	// If that fails, get all contacts and filter client-side
	contacts, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter contacts based on last modified time
	var filtered []map[string]interface{}
	for _, contact := range contacts {
		lastModified, err := parseTime(contact["lastActivityDate"])
		if err != nil {
			continue
		}
		if lastModified.After(since) {
			filtered = append(filtered, contact)
		}
	}

	return filtered, nil
}

// Validate validates a contact entity
func (h *ContactHandler) Validate(contact map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "firstName", "lastName", "email"}
	for _, field := range requiredFields {
		if _, ok := contact[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeContact,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := contact["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeContact,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := contact["firstName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeContact,
			Field:      "firstName",
			Message:    "firstName must be a string",
		}
	}

	if _, ok := contact["lastName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeContact,
			Field:      "lastName",
			Message:    "lastName must be a string",
		}
	}

	if _, ok := contact["email"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeContact,
			Field:      "email",
			Message:    "email must be a string",
		}
	}

	return nil
}

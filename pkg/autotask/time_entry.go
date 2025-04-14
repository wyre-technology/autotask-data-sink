package autotask

import (
	"context"
	"time"
)

// TimeEntryHandler handles TimeEntry entity operations
type TimeEntryHandler struct {
	*BaseEntityHandler
}

// NewTimeEntryHandler creates a new TimeEntryHandler
func NewTimeEntryHandler(client *Client) *TimeEntryHandler {
	base := NewBaseEntityHandler(client, EntityTypeTimeEntry, "TimeEntries")
	return &TimeEntryHandler{base}
}

// Get retrieves all time entries
func (h *TimeEntryHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// For time entries, we always want to use GetSince with a reasonable default time
	// to avoid retrieving the entire history
	defaultLookback := time.Now().AddDate(0, -1, 0) // 1 month ago
	return h.GetSince(ctx, defaultLookback)
}

// GetByID retrieves a single time entry by ID
func (h *TimeEntryHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	entries, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(entries) > 0 {
		return entries[0], nil
	}

	return nil, err
}

// GetSince retrieves all time entries modified since the given time
func (h *TimeEntryHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	entries, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return entries, nil
	}

	// If that fails, try the time entry-specific method
	return h.client.GetTimeEntries(ctx, since)
}

// Validate validates a time entry entity
func (h *TimeEntryHandler) Validate(entry map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "resourceID", "ticketID", "hoursWorked", "dateWorked"}
	for _, field := range requiredFields {
		if _, ok := entry[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeTimeEntry,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := entry["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := entry["resourceID"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "resourceID",
			Message:    "resourceID must be a number",
		}
	}

	if _, ok := entry["ticketID"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "ticketID",
			Message:    "ticketID must be a number",
		}
	}

	if hoursWorked, ok := entry["hoursWorked"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "hoursWorked",
			Message:    "hoursWorked must be a number",
		}
	} else if hoursWorked < 0 {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "hoursWorked",
			Message:    "hoursWorked must be non-negative",
		}
	}

	// Validate dateWorked is a valid time
	if dateWorkedStr, ok := entry["dateWorked"].(string); ok {
		if _, err := parseTime(dateWorkedStr); err != nil {
			return &ValidationError{
				EntityType: EntityTypeTimeEntry,
				Field:      "dateWorked",
				Message:    "dateWorked must be a valid date/time",
			}
		}
	} else {
		return &ValidationError{
			EntityType: EntityTypeTimeEntry,
			Field:      "dateWorked",
			Message:    "dateWorked must be a string",
		}
	}

	// Validate relationships if present
	if contractID, ok := entry["contractID"]; ok {
		if _, ok := contractID.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeTimeEntry,
				Field:      "contractID",
				Message:    "contractID must be a number",
			}
		}
	}

	return nil
}

package autotask

import (
	"context"
	"time"
)

// AppointmentHandler handles Appointment entity operations
type AppointmentHandler struct {
	*BaseEntityHandler
}

// NewAppointmentHandler creates a new AppointmentHandler
func NewAppointmentHandler(client entityClient) *AppointmentHandler {
	base := NewBaseEntityHandler(client, EntityTypeAppointment, "Appointments")
	return &AppointmentHandler{base}
}

// Get retrieves all appointments
func (h *AppointmentHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// For appointments, we always want to use GetSince with a reasonable default time
	// to avoid retrieving the entire appointment history
	defaultLookback := time.Now().AddDate(0, -1, 0) // 1 month ago
	return h.GetSince(ctx, defaultLookback)
}

// GetByID retrieves a single appointment by ID
func (h *AppointmentHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	appointments, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(appointments) > 0 {
		return appointments[0], nil
	}

	return nil, err
}

// GetSince retrieves all appointments modified since the given time
func (h *AppointmentHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	appointments, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return appointments, nil
	}

	// If that fails, try with startDateTime field
	appointments, err = h.client.queryWithDateFilter(ctx, h.entityEndpoint, "startDateTime", since)
	if err == nil {
		return appointments, nil
	}

	// If that also fails, get all appointments and filter client-side
	appointments, err = h.BaseEntityHandler.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter appointments based on start time and last modified time
	var filtered []map[string]interface{}
	for _, appointment := range appointments {
		// Check both startDateTime and lastActivityDate
		var include bool

		if startDateTime, ok := appointment["startDateTime"]; ok {
			if timeVal, err := parseTime(startDateTime); err == nil {
				if !timeVal.Before(since) {
					include = true
				}
			}
		}

		if !include {
			if lastModified, ok := appointment["lastActivityDate"]; ok {
				if timeVal, err := parseTime(lastModified); err == nil {
					if !timeVal.Before(since) {
						include = true
					}
				}
			}
		}

		if include {
			filtered = append(filtered, appointment)
		}
	}

	return filtered, nil
}

// Validate validates an appointment entity
func (h *AppointmentHandler) Validate(appointment map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "title", "startDateTime", "endDateTime"}
	for _, field := range requiredFields {
		if _, ok := appointment[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeAppointment,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := appointment["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := appointment["title"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "title",
			Message:    "title must be a string",
		}
	}

	// Validate dates
	startDateStr, ok := appointment["startDateTime"].(string)
	if !ok {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "startDateTime",
			Message:    "startDateTime must be a string",
		}
	}
	startDate, err := parseTime(startDateStr)
	if err != nil {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "startDateTime",
			Message:    "startDateTime must be a valid date/time",
		}
	}

	endDateStr, ok := appointment["endDateTime"].(string)
	if !ok {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "endDateTime",
			Message:    "endDateTime must be a string",
		}
	}
	endDate, err := parseTime(endDateStr)
	if err != nil {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "endDateTime",
			Message:    "endDateTime must be a valid date/time",
		}
	}

	// Validate date order
	if endDate.Before(startDate) {
		return &ValidationError{
			EntityType: EntityTypeAppointment,
			Field:      "endDateTime",
			Message:    "endDateTime must be after startDateTime",
		}
	}

	// Validate relationships if present
	if resourceID, ok := appointment["resourceID"]; ok {
		if _, ok := resourceID.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeAppointment,
				Field:      "resourceID",
				Message:    "resourceID must be a number",
			}
		}
	}

	if contactID, ok := appointment["contactID"]; ok {
		if _, ok := contactID.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeAppointment,
				Field:      "contactID",
				Message:    "contactID must be a number",
			}
		}
	}

	return nil
}

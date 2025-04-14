package autotask

import (
	"context"
	"time"
)

// TicketHandler handles Ticket entity operations
type TicketHandler struct {
	*BaseEntityHandler
}

// NewTicketHandler creates a new TicketHandler
func NewTicketHandler(client *Client) *TicketHandler {
	base := NewBaseEntityHandler(client, EntityTypeTicket, "Tickets")
	return &TicketHandler{base}
}

// Get retrieves all tickets
func (h *TicketHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// For tickets, we always want to use GetSince with a reasonable default time
	// to avoid retrieving the entire ticket history
	defaultLookback := time.Now().AddDate(0, -1, 0) // 1 month ago
	return h.GetSince(ctx, defaultLookback)
}

// GetByID retrieves a single ticket by ID
func (h *TicketHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// First try the standard approach
	tickets, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(tickets) > 0 {
		return tickets[0], nil
	}

	// If that fails, try the ticket-specific approach
	return h.client.GetTicket(ctx, id)
}

// GetSince retrieves all tickets modified since the given time
func (h *TicketHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	tickets, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return tickets, nil
	}

	// If that fails, try the ticket-specific method
	return h.client.GetTickets(ctx, since)
}

// Validate validates a ticket entity
func (h *TicketHandler) Validate(ticket map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "ticketNumber", "title", "status"}
	for _, field := range requiredFields {
		if _, ok := ticket[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeTicket,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := ticket["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeTicket,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := ticket["ticketNumber"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeTicket,
			Field:      "ticketNumber",
			Message:    "ticketNumber must be a string",
		}
	}

	if _, ok := ticket["title"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeTicket,
			Field:      "title",
			Message:    "title must be a string",
		}
	}

	if _, ok := ticket["status"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeTicket,
			Field:      "status",
			Message:    "status must be a string",
		}
	}

	// Validate relationships if present
	if companyID, ok := ticket["companyID"]; ok {
		if _, ok := companyID.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeTicket,
				Field:      "companyID",
				Message:    "companyID must be a number",
			}
		}
	}

	if contactID, ok := ticket["contactID"]; ok {
		if _, ok := contactID.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeTicket,
				Field:      "contactID",
				Message:    "contactID must be a number",
			}
		}
	}

	return nil
}

package autotask

import (
	"context"
	"time"
)

// CompanyHandler handles Company entity operations
type CompanyHandler struct {
	*BaseEntityHandler
}

// NewCompanyHandler creates a new CompanyHandler
func NewCompanyHandler(client *Client) *CompanyHandler {
	base := NewBaseEntityHandler(client, EntityTypeCompany, "Companies")
	return &CompanyHandler{base}
}

// Get retrieves all companies
func (h *CompanyHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// First try the standard approach
	companies, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return companies, nil
	}

	// If that fails, try company-specific approaches
	companies, err = h.client.getCompaniesSequentially(ctx)
	if err == nil {
		return companies, nil
	}

	// As a last resort, try getting companies individually
	return h.client.getCompaniesIndividually(ctx)
}

// GetByID retrieves a single company by ID
func (h *CompanyHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// First try the standard approach
	companies, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(companies) > 0 {
		return companies[0], nil
	}

	// If that fails, try the company-specific approach
	return h.client.GetCompany(ctx, id)
}

// GetSince retrieves all companies modified since the given time
func (h *CompanyHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Companies don't support the standard lastModifiedDateTime field
	// Instead, we'll get all companies and filter them client-side
	companies, err := h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter companies based on last modified time
	var filtered []map[string]interface{}
	for _, company := range companies {
		lastModified, err := parseTime(company["lastActivityDate"])
		if err != nil {
			continue
		}
		if lastModified.After(since) {
			filtered = append(filtered, company)
		}
	}

	return filtered, nil
}

// Validate validates a company entity
func (h *CompanyHandler) Validate(company map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "companyName"}
	for _, field := range requiredFields {
		if _, ok := company[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeCompany,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := company["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeCompany,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := company["companyName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeCompany,
			Field:      "companyName",
			Message:    "companyName must be a string",
		}
	}

	return nil
}

// ValidationError represents a validation error
type ValidationError struct {
	EntityType EntityType
	Field      string
	Message    string
}

func (e *ValidationError) Error() string {
	return "validation error for " + string(e.EntityType) + " field " + e.Field + ": " + e.Message
}

package autotask

import (
	"context"
	"time"
)

// ContractHandler handles Contract entity operations
type ContractHandler struct {
	*BaseEntityHandler
}

// NewContractHandler creates a new ContractHandler
func NewContractHandler(client *Client) *ContractHandler {
	base := NewBaseEntityHandler(client, EntityTypeContract, "Contracts")
	return &ContractHandler{base}
}

// Get retrieves all contracts
func (h *ContractHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// First try the standard approach
	contracts, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return contracts, nil
	}

	// If that fails, try the contract-specific method
	return h.client.GetContracts(ctx)
}

// GetByID retrieves a single contract by ID
func (h *ContractHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	contracts, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(contracts) > 0 {
		return contracts[0], nil
	}

	return nil, err
}

// GetSince retrieves all contracts modified since the given time
func (h *ContractHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	contracts, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return contracts, nil
	}

	// If that fails, get all contracts and filter client-side
	contracts, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter contracts based on last modified time
	var filtered []map[string]interface{}
	for _, contract := range contracts {
		lastModified, err := parseTime(contract["lastActivityDate"])
		if err != nil {
			continue
		}
		if lastModified.After(since) {
			filtered = append(filtered, contract)
		}
	}

	return filtered, nil
}

// Validate validates a contract entity
func (h *ContractHandler) Validate(contract map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "companyID", "contractName", "contractType", "startDate", "endDate"}
	for _, field := range requiredFields {
		if _, ok := contract[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeContract,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := contract["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := contract["companyID"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "companyID",
			Message:    "companyID must be a number",
		}
	}

	if _, ok := contract["contractName"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "contractName",
			Message:    "contractName must be a string",
		}
	}

	if _, ok := contract["contractType"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "contractType",
			Message:    "contractType must be a string",
		}
	}

	// Validate dates
	startDateStr, ok := contract["startDate"].(string)
	if !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "startDate",
			Message:    "startDate must be a string",
		}
	}
	startDate, err := parseTime(startDateStr)
	if err != nil {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "startDate",
			Message:    "startDate must be a valid date/time",
		}
	}

	endDateStr, ok := contract["endDate"].(string)
	if !ok {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "endDate",
			Message:    "endDate must be a string",
		}
	}
	endDate, err := parseTime(endDateStr)
	if err != nil {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "endDate",
			Message:    "endDate must be a valid date/time",
		}
	}

	// Validate date order
	if endDate.Before(startDate) {
		return &ValidationError{
			EntityType: EntityTypeContract,
			Field:      "endDate",
			Message:    "endDate must be after startDate",
		}
	}

	// Validate contract hours if present
	if contractHours, ok := contract["contractHours"]; ok {
		if hours, ok := contractHours.(float64); !ok {
			return &ValidationError{
				EntityType: EntityTypeContract,
				Field:      "contractHours",
				Message:    "contractHours must be a number",
			}
		} else if hours < 0 {
			return &ValidationError{
				EntityType: EntityTypeContract,
				Field:      "contractHours",
				Message:    "contractHours must be non-negative",
			}
		}
	}

	return nil
}

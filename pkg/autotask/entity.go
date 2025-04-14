package autotask

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// EntityType represents the type of Autotask entity
type EntityType string

// Known entity types
const (
	EntityTypeCompany           EntityType = "Company"
	EntityTypeContact           EntityType = "Contact"
	EntityTypeTicket            EntityType = "Ticket"
	EntityTypeTimeEntry         EntityType = "TimeEntry"
	EntityTypeResource          EntityType = "Resource"
	EntityTypeContract          EntityType = "Contract"
	EntityTypeActionType        EntityType = "ActionType"
	EntityTypeAppointment       EntityType = "Appointment"
	EntityTypeArticle           EntityType = "Article"
	EntityTypeArticleNote       EntityType = "ArticleNote"
	EntityTypeConfigurationItem EntityType = "ConfigurationItem"
)

// entityClient defines the interface for interacting with the Autotask API
type entityClient interface {
	// Entity retrieval methods
	batchGetEntities(ctx context.Context, endpoint string, ids []int64, batchSize int) ([]map[string]interface{}, error)
	getAllEntities(ctx context.Context, endpoint string) ([]map[string]interface{}, error)
	queryWithDateFilter(ctx context.Context, endpoint string, fieldName string, since time.Time) ([]map[string]interface{}, error)
	queryWithEmptyFilter(ctx context.Context, endpoint string) ([]map[string]interface{}, error)
	getCompaniesSequentially(ctx context.Context) ([]map[string]interface{}, error)
	getCompaniesIndividually(ctx context.Context) ([]map[string]interface{}, error)
	getContactsSequentially(ctx context.Context) ([]map[string]interface{}, error)
	getContactsIndividually(ctx context.Context) ([]map[string]interface{}, error)
	GetCompany(ctx context.Context, id int64) (map[string]interface{}, error)
	GetContact(ctx context.Context, id int64) (map[string]interface{}, error)
	GetTicket(ctx context.Context, id int64) (map[string]interface{}, error)
	GetTickets(ctx context.Context, since time.Time) ([]map[string]interface{}, error)
	GetTimeEntries(ctx context.Context, since time.Time) ([]map[string]interface{}, error)
	GetResources(ctx context.Context) ([]map[string]interface{}, error)
	GetProjects(ctx context.Context) ([]map[string]interface{}, error)
	GetTasks(ctx context.Context) ([]map[string]interface{}, error)
	GetConfigurationItems(ctx context.Context) ([]map[string]interface{}, error)
	GetContracts(ctx context.Context) ([]map[string]interface{}, error)

	// Request handling methods
	NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error)
	Do(req *http.Request, v interface{}) (*http.Response, error)
	ExecuteConcurrent(ctx context.Context, entityName string, fn func() error) error
}

// EntityHandler defines the standard interface for handling Autotask entities
type EntityHandler interface {
	// Get retrieves all entities of this type
	Get(ctx context.Context) ([]map[string]interface{}, error)

	// GetByID retrieves a single entity by its ID
	GetByID(ctx context.Context, id int64) (map[string]interface{}, error)

	// GetSince retrieves all entities modified since the given time
	GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error)
}

// AutotaskError represents an error from the Autotask API
type AutotaskError struct {
	StatusCode int
	Message    string
	RetryAfter time.Duration
}

func (e *AutotaskError) Error() string {
	return fmt.Sprintf("autotask API error (status %d): %s", e.StatusCode, e.Message)
}

// IsRateLimitError returns true if the error is a rate limit error
func (e *AutotaskError) IsRateLimitError() bool {
	return e.StatusCode == 429
}

// BaseEntityHandler provides common functionality for entity handlers
type BaseEntityHandler struct {
	client         entityClient
	entityType     EntityType
	entityEndpoint string
}

// NewBaseEntityHandler creates a new base entity handler
func NewBaseEntityHandler(client entityClient, entityType EntityType, endpoint string) *BaseEntityHandler {
	return &BaseEntityHandler{
		client:         client,
		entityType:     entityType,
		entityEndpoint: endpoint,
	}
}

// Get implements the base Get method for entities
func (h *BaseEntityHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	return h.client.getAllEntities(ctx, h.entityEndpoint)
}

// GetByID implements the base GetByID method for entities
func (h *BaseEntityHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	entities, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity %s with ID %d not found", h.entityType, id)
	}
	return entities[0], nil
}

// GetSince implements the base GetSince method for entities
func (h *BaseEntityHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	return h.client.queryWithDateFilter(ctx, h.entityEndpoint, "lastModifiedDateTime", since)
}

// RetryableError checks if an error should trigger a retry
func RetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for rate limit errors
	if autotaskErr, ok := err.(*AutotaskError); ok {
		return autotaskErr.IsRateLimitError()
	}

	// Add other retryable error conditions here
	return false
}

// GetRetryDelay returns the delay before retrying an operation
func GetRetryDelay(attempt int, err error) time.Duration {
	// If it's a rate limit error, use the RetryAfter value
	if autotaskErr, ok := err.(*AutotaskError); ok && autotaskErr.IsRateLimitError() {
		return autotaskErr.RetryAfter
	}

	// Otherwise use exponential backoff
	baseDelay := time.Second
	maxDelay := time.Minute * 5

	delay := baseDelay * time.Duration(1<<uint(attempt))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

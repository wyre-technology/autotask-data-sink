package autotask

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bytes"
	"io"

	"github.com/asachs01/autotask-data-sink/pkg/config"
	atapi "github.com/asachs01/autotask-go/pkg/autotask"
	"github.com/rs/zerolog/log"
)

// SimpleSemaphore is a simple implementation of a semaphore
type SimpleSemaphore struct {
	mu    sync.Mutex
	count int
	max   int
	cond  *sync.Cond
}

// NewSimpleSemaphore creates a new semaphore with the given maximum count
func NewSimpleSemaphore(max int) *SimpleSemaphore {
	s := &SimpleSemaphore{
		max: max,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Acquire acquires a permit, blocking if necessary
func (s *SimpleSemaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.count >= s.max {
		// Check if context is done
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Wait for a release
			s.cond.Wait()
		}
	}

	s.count++
	return nil
}

// Release releases a permit
func (s *SimpleSemaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count > 0 {
		s.count--
		s.cond.Signal()
	}
}

// ConcurrentRequestLimiter manages concurrent requests to the Autotask API
// respecting the thread limits per object endpoint
type ConcurrentRequestLimiter struct {
	// Map of entity name to semaphore
	limiters map[string]*SimpleSemaphore
	mu       sync.Mutex
	// Maximum concurrent requests per entity
	maxConcurrent int
}

// NewConcurrentRequestLimiter creates a new request limiter
func NewConcurrentRequestLimiter(maxConcurrentPerEntity int) *ConcurrentRequestLimiter {
	if maxConcurrentPerEntity <= 0 {
		maxConcurrentPerEntity = 3 // Default to 3 as per Autotask docs for new integrations
	}

	return &ConcurrentRequestLimiter{
		limiters:      make(map[string]*SimpleSemaphore),
		maxConcurrent: maxConcurrentPerEntity,
	}
}

// Acquire acquires a permit to make a request to the specified entity
// It blocks until a permit is available or the context is canceled
func (l *ConcurrentRequestLimiter) Acquire(ctx context.Context, entityName string) error {
	l.mu.Lock()
	sem, ok := l.limiters[entityName]
	if !ok {
		sem = NewSimpleSemaphore(l.maxConcurrent)
		l.limiters[entityName] = sem
	}
	l.mu.Unlock()

	// Acquire a permit
	return sem.Acquire(ctx)
}

// Release releases a permit for the specified entity
func (l *ConcurrentRequestLimiter) Release(entityName string) {
	l.mu.Lock()
	if sem, ok := l.limiters[entityName]; ok {
		l.mu.Unlock()
		sem.Release()
	} else {
		l.mu.Unlock()
	}
}

// ExecuteConcurrent executes a function with concurrency control
func (l *ConcurrentRequestLimiter) ExecuteConcurrent(ctx context.Context, entityName string, fn func() error) error {
	if err := l.Acquire(ctx, entityName); err != nil {
		return fmt.Errorf("failed to acquire permit for %s: %w", entityName, err)
	}
	defer l.Release(entityName)

	return fn()
}

// Client wraps the autotask-go client
type Client struct {
	client         atapi.Client
	requestLimiter *ConcurrentRequestLimiter
}

// New creates a new Autotask client
func New(cfg config.AutotaskConfig) (*Client, error) {
	// Check for direct environment variables first
	username := os.Getenv("AUTOTASK_USERNAME")
	if username == "" {
		username = cfg.Username
	}

	secret := os.Getenv("AUTOTASK_SECRET")
	if secret == "" {
		secret = cfg.Secret
	}

	integrationCode := os.Getenv("AUTOTASK_INTEGRATION_CODE")
	if integrationCode == "" {
		integrationCode = cfg.IntegrationCode
	}

	// Log the credentials being used (without the actual secret)
	log.Debug().
		Str("username", username).
		Str("integration_code", integrationCode).
		Msg("Creating Autotask client")

	client := atapi.NewClient(
		username,
		secret,
		integrationCode,
	)

	// Create a request limiter with a default of 3 concurrent requests per entity
	// as per Autotask documentation for newer integrations
	requestLimiter := NewConcurrentRequestLimiter(3)

	return &Client{
		client:         client,
		requestLimiter: requestLimiter,
	}, nil
}

// queryWithEmptyFilter is a helper method to query an entity with an empty filter array
func (c *Client) queryWithEmptyFilter(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Querying with empty filter")

	url := entityName + "/query"

	// Use the exact format from the Autotask documentation
	// The key is that "filter" must be an array, not an object
	reqBody := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "exist",
				"field": "id",
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Msg("Query with empty filter")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add special headers that might be required
	req.Header.Set("Content-Type", "application/json")

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entities using query with empty filter")

	return response.Items, nil
}

// tryAutotaskDocumentationFormatWithDate attempts to query entities using the format from the Autotask API documentation with a date filter
func (c *Client) tryAutotaskDocumentationFormatWithDate(ctx context.Context, entityName, fieldName string, since time.Time) ([]map[string]interface{}, error) {
	log.Info().
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Attempting to query entities using Autotask documentation format with date filter")

	// This format is based on the Autotask API documentation
	// https://ww2.autotask.net/help/developerhelp/Content/APIs/REST/API_Calls/REST_Basic_Query_Calls.htm

	url := entityName + "/query"

	// Create the request body according to the documentation
	reqBody := map[string]interface{}{
		"MaxRecords": 500,
		"filter": []interface{}{
			map[string]interface{}{
				"field": fieldName,
				"op":    "gt",
				"value": since.Format(time.RFC3339),
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("API request body using documentation format with date filter")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Retrieved entities using documentation format with date filter")

	return response.Items, nil
}

// queryWithDateFilter is a helper method to query an entity with a date-based filter
func (c *Client) queryWithDateFilter(ctx context.Context, entityName, fieldName string, since time.Time) ([]map[string]interface{}, error) {
	log.Info().
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Querying with date filter")

	url := entityName + "/query"

	// Format the date as ISO 8601 string
	sinceStr := since.UTC().Format(time.RFC3339)

	// Use the exact format from the Autotask documentation
	// The key is that "filter" must be an array, not an object
	reqBody := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "gte",
				"field": fieldName,
				"value": sinceStr,
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Query with date filter")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add special headers that might be required
	req.Header.Set("Content-Type", "application/json")

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Retrieved entities using query with date filter")

	return response.Items, nil
}

// GetCompanies retrieves companies from Autotask
func (c *Client) GetCompanies(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all companies using the proper filter format first
	companies, err := c.queryWithProperFilter(ctx, "Companies")
	if err == nil && len(companies) > 0 {
		log.Info().
			Int("count", len(companies)).
			Msg("Retrieved all companies from Autotask using proper filter format")
		return companies, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get companies with proper filter format, trying with different case")

		// Try with a different endpoint name (case sensitivity matters in some API versions)
		companies, err = c.queryWithProperFilter(ctx, "companies")
		if err == nil && len(companies) > 0 {
			log.Info().
				Int("count", len(companies)).
				Msg("Retrieved all companies from Autotask using proper filter format with lowercase")
			return companies, nil
		}

		log.Warn().
			Err(err).
			Msg("Failed to get companies with proper filter format, falling back to other methods")
	}

	// Try to get all companies in a single API call
	companies, err = c.getAllEntities(ctx, "Companies")
	if err == nil && len(companies) > 0 {
		log.Info().
			Int("count", len(companies)).
			Msg("Retrieved all companies from Autotask in a single API call")
		return companies, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all companies in a single API call, trying query endpoint")
	}

	// Try to use the query endpoint with empty filter
	companies, err = c.queryWithEmptyFilter(ctx, "Companies")
	if err != nil {
		// Try with a different endpoint name (case sensitivity matters in some API versions)
		log.Warn().
			Err(err).
			Msg("Failed to query companies with 'Companies', trying with 'companies'")

		companies, err = c.queryWithEmptyFilter(ctx, "companies")
		if err != nil {
			log.Warn().
				Err(err).
				Msg("Failed to query companies with empty filter, trying ID ranges approach")

			// Try the ID ranges approach
			companies, err = c.getEntitiesByIdRanges(ctx, "Companies", 50, 1000)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("Failed to get companies by ID ranges, falling back to individual GET requests")

				return c.getCompaniesIndividually(ctx)
			}

			return companies, nil
		}
	}

	log.Info().
		Int("count", len(companies)).
		Msg("Retrieved companies from Autotask using query endpoint")

	return companies, nil
}

// getCompaniesIndividually retrieves companies one by one using their IDs
// This is a fallback method in case the query endpoint doesn't work
func (c *Client) getCompaniesIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	// Try the ID ranges approach with smaller batches
	companies, err := c.getEntitiesByIdRanges(ctx, "Companies", 10, 500)
	if err == nil && len(companies) > 0 {
		return companies, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get companies by ID ranges with smaller batches, trying to get valid IDs")
	}

	// First, try to get a list of valid company IDs
	log.Info().Msg("Attempting to get list of valid company IDs")

	// Try different endpoints to get a list of IDs
	validIDs, err := c.getValidIDs(ctx, "Companies")
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get list of valid company IDs, falling back to sequential ID checks")
		return c.getCompaniesSequentially(ctx)
	}

	// Try to get companies in batches with concurrency
	companies, err = c.batchGetEntitiesConcurrent(ctx, "Companies", validIDs, 50)
	if err != nil || len(companies) == 0 {
		log.Warn().
			Err(err).
			Int("validIDs", len(validIDs)).
			Msg("Concurrent batch GET request failed or returned no companies, falling back to sequential batch requests")

		// Try non-concurrent batch requests
		companies, err = c.batchGetEntities(ctx, "Companies", validIDs, 50)
		if err != nil || len(companies) == 0 {
			log.Warn().
				Err(err).
				Int("validIDs", len(validIDs)).
				Msg("Batch GET request failed or returned no companies, falling back to individual GET requests")

			// Create a list to hold all companies
			companies = make([]map[string]interface{}, 0, len(validIDs))

			// Get each company by ID
			for _, id := range validIDs {
				company, err := c.GetCompany(ctx, id)
				if err != nil {
					log.Debug().Int64("id", id).Err(err).Msg("Failed to get company, skipping")
					continue
				}

				// Skip nil companies
				if company == nil {
					log.Debug().Int64("id", id).Msg("Company is nil, skipping")
					continue
				}

				// Ensure the company has an ID field
				if _, ok := company["id"]; !ok {
					company["id"] = id
				}

				companies = append(companies, company)
			}
		}
	}

	log.Info().
		Int("count", len(companies)).
		Msg("Retrieved companies from Autotask")

	return companies, nil
}

// getCompaniesSequentially retrieves companies by checking sequential IDs
// This is a last resort fallback method
func (c *Client) getCompaniesSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	// Create a list to hold all companies
	var companies []map[string]interface{}

	// Try to retrieve companies with sequential IDs
	// This is a temporary solution until we can get the query endpoint working
	for id := int64(1); id < 200; id++ {
		company, err := c.GetCompany(ctx, id)
		if err != nil {
			// Skip companies that don't exist
			log.Debug().Int64("id", id).Msg("Company not found, skipping")
			continue
		}

		// Skip nil companies
		if company == nil {
			log.Debug().Int64("id", id).Msg("Company is nil, skipping")
			continue
		}

		// Ensure the company has an ID field
		if _, ok := company["id"]; !ok {
			company["id"] = id
		}

		companies = append(companies, company)
	}

	log.Info().
		Int("count", len(companies)).
		Msg("Retrieved companies from Autotask using sequential ID checks")

	return companies, nil
}

// getValidIDs attempts to get a list of valid entity IDs
func (c *Client) getValidIDs(ctx context.Context, entityName string) ([]int64, error) {
	// Try to get all entities first using the exact format
	entities, err := c.tryExactAutotaskFormat(ctx, entityName)
	if err == nil && len(entities) > 0 {
		// Extract IDs from the entities
		ids := make([]int64, 0, len(entities))
		for _, entity := range entities {
			if idVal, ok := entity["id"]; ok {
				switch v := idVal.(type) {
				case float64:
					ids = append(ids, int64(v))
				case int64:
					ids = append(ids, v)
				case int:
					ids = append(ids, int64(v))
				default:
					log.Debug().
						Interface("id", idVal).
						Str("type", fmt.Sprintf("%T", idVal)).
						Msg("Unexpected ID type, skipping")
				}
			}
		}

		if len(ids) > 0 {
			log.Info().
				Int("count", len(ids)).
				Str("entity", entityName).
				Msg("Retrieved entity IDs from entities")

			return ids, nil
		}
	}

	// Try different approaches to get a list of IDs

	// First, try to get a minimal projection with just IDs
	url := entityName + "/query"

	// Try the simplest filter format based on Autotask documentation
	// Format 1: Simple "exist" filter on ID field
	reqBody1 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "exist",
				"field": "id",
			},
		},
		"MaxRecords":    1000,
		"IncludeFields": []string{"id"},
	}

	// Format 2: Simple "greater than 0" filter on ID field
	reqBody2 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "gt",
				"field": "id",
				"value": 0,
			},
		},
		"MaxRecords":    1000,
		"IncludeFields": []string{"id"},
	}

	// Format 3: Simple "equal to true" filter on isActive field
	reqBody3 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "eq",
				"field": "isActive",
				"value": true,
			},
		},
		"MaxRecords":    1000,
		"IncludeFields": []string{"id"},
	}

	// Format 4: No filter, just MaxRecords
	reqBody4 := map[string]interface{}{
		"MaxRecords":    1000,
		"IncludeFields": []string{"id"},
	}

	// Try each format in sequence
	formats := []map[string]interface{}{reqBody1, reqBody2, reqBody3, reqBody4}
	descriptions := []string{
		"exist id filter",
		"id > 0 filter",
		"isActive = true filter",
		"no filter, just MaxRecords",
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}
	var succeeded bool

	for i, reqBody := range formats {
		reqBytes, err := json.Marshal(reqBody)
		if err != nil {
			continue
		}

		log.Debug().
			Str("request_body", string(reqBytes)).
			Str("entity", entityName).
			Int("format", i+1).
			Str("description", descriptions[i]).
			Msg("API request for IDs")

		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			continue
		}

		// Add special headers that might be required
		req.Header.Set("Content-Type", "application/json")

		_, err = c.client.Do(req, &response)
		if err == nil {
			succeeded = true
			log.Info().
				Int("count", len(response.Items)).
				Str("entity", entityName).
				Int("format", i+1).
				Str("description", descriptions[i]).
				Msg("Successfully retrieved entity IDs")
			break
		}

		log.Debug().
			Err(err).
			Int("format", i+1).
			Str("description", descriptions[i]).
			Msg("Filter format failed for ID request")
	}

	if !succeeded {
		return nil, fmt.Errorf("failed to execute request for %s IDs: all formats failed", entityName)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entity IDs from Autotask")

	// Extract IDs from the response
	ids := make([]int64, 0, len(response.Items))
	for _, item := range response.Items {
		if idVal, ok := item["id"]; ok {
			switch v := idVal.(type) {
			case float64:
				ids = append(ids, int64(v))
			case int64:
				ids = append(ids, v)
			case int:
				ids = append(ids, int64(v))
			default:
				log.Debug().
					Interface("id", idVal).
					Str("type", fmt.Sprintf("%T", idVal)).
					Msg("Unexpected ID type, skipping")
			}
		}
	}

	return ids, nil
}

// GetCompany retrieves a single company by ID
func (c *Client) GetCompany(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Create a custom request to the API
	url := fmt.Sprintf("Companies/%d", id)

	req, err := c.client.NewRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Define a response structure for a single company
	var response struct {
		Item map[string]interface{} `json:"item"`
	}

	// Execute the request
	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get company %d: %w", id, err)
	}

	// Check if the response item is nil
	if response.Item == nil {
		return nil, fmt.Errorf("company %d not found", id)
	}

	return response.Item, nil
}

// GetContacts retrieves contacts from Autotask
func (c *Client) GetContacts(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all contacts using the proper filter format first
	contacts, err := c.queryWithProperFilter(ctx, "Contacts")
	if err == nil && len(contacts) > 0 {
		log.Info().
			Int("count", len(contacts)).
			Msg("Retrieved all contacts from Autotask using proper filter format")
		return contacts, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get contacts with proper filter format, trying with different case")

		// Try with a different endpoint name (case sensitivity matters in some API versions)
		contacts, err = c.queryWithProperFilter(ctx, "contacts")
		if err == nil && len(contacts) > 0 {
			log.Info().
				Int("count", len(contacts)).
				Msg("Retrieved all contacts from Autotask using proper filter format with lowercase")
			return contacts, nil
		}

		log.Warn().
			Err(err).
			Msg("Failed to get contacts with proper filter format, falling back to other methods")
	}

	// Try to get all contacts in a single API call
	contacts, err = c.getAllEntities(ctx, "Contacts")
	if err == nil && len(contacts) > 0 {
		log.Info().
			Int("count", len(contacts)).
			Msg("Retrieved all contacts from Autotask in a single API call")
		return contacts, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all contacts in a single API call, trying query endpoint")
	}

	// Try to use the query endpoint with empty filter
	contacts, err = c.queryWithEmptyFilter(ctx, "Contacts")
	if err != nil {
		// Try with a different endpoint name (case sensitivity matters in some API versions)
		log.Warn().
			Err(err).
			Msg("Failed to query contacts with 'Contacts', trying with 'contacts'")

		contacts, err = c.queryWithEmptyFilter(ctx, "contacts")
		if err != nil {
			log.Warn().
				Err(err).
				Msg("Failed to query contacts with empty filter, trying ID ranges approach")

			// Try the ID ranges approach
			contacts, err = c.getEntitiesByIdRanges(ctx, "Contacts", 50, 1000)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("Failed to get contacts by ID ranges, falling back to individual GET requests")

				return c.getContactsIndividually(ctx)
			}

			return contacts, nil
		}
	}

	log.Info().
		Int("count", len(contacts)).
		Msg("Retrieved contacts from Autotask using query endpoint")

	return contacts, nil
}

// getContactsIndividually retrieves contacts one by one using their IDs
// This is a fallback method in case the query endpoint doesn't work
func (c *Client) getContactsIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	// Try the ID ranges approach with smaller batches
	contacts, err := c.getEntitiesByIdRanges(ctx, "Contacts", 10, 500)
	if err == nil && len(contacts) > 0 {
		return contacts, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get contacts by ID ranges with smaller batches, trying to get valid IDs")
	}

	// First, try to get a list of valid contact IDs
	log.Info().Msg("Attempting to get list of valid contact IDs")

	// Try different endpoints to get a list of IDs
	validIDs, err := c.getValidIDs(ctx, "Contacts")
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get list of valid contact IDs, falling back to sequential ID checks")
		return c.getContactsSequentially(ctx)
	}

	// Try to get contacts in batches with concurrency
	contacts, err = c.batchGetEntitiesConcurrent(ctx, "Contacts", validIDs, 50)
	if err != nil || len(contacts) == 0 {
		log.Warn().
			Err(err).
			Int("validIDs", len(validIDs)).
			Msg("Concurrent batch GET request failed or returned no contacts, falling back to sequential batch requests")

		// Try non-concurrent batch requests
		contacts, err = c.batchGetEntities(ctx, "Contacts", validIDs, 50)
		if err != nil || len(contacts) == 0 {
			log.Warn().
				Err(err).
				Int("validIDs", len(validIDs)).
				Msg("Batch GET request failed or returned no contacts, falling back to individual GET requests")

			// Create a list to hold all contacts
			contacts = make([]map[string]interface{}, 0, len(validIDs))

			// Get each contact by ID
			for _, id := range validIDs {
				contact, err := c.GetContact(ctx, id)
				if err != nil {
					log.Debug().Int64("id", id).Err(err).Msg("Failed to get contact, skipping")
					continue
				}

				// Skip nil contacts
				if contact == nil {
					log.Debug().Int64("id", id).Msg("Contact is nil, skipping")
					continue
				}

				// Ensure the contact has an ID field
				if _, ok := contact["id"]; !ok {
					contact["id"] = id
				}

				contacts = append(contacts, contact)
			}
		}
	}

	log.Info().
		Int("count", len(contacts)).
		Msg("Retrieved contacts from Autotask")

	return contacts, nil
}

// getContactsSequentially retrieves contacts by checking sequential IDs
// This is a last resort fallback method
func (c *Client) getContactsSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	// Create a list to hold all contacts
	var contacts []map[string]interface{}

	// Try to retrieve contacts with sequential IDs
	// This is a temporary solution until we can get the query endpoint working
	for id := int64(1); id < 200; id++ {
		contact, err := c.GetContact(ctx, id)
		if err != nil {
			// Skip contacts that don't exist
			log.Debug().Int64("id", id).Msg("Contact not found, skipping")
			continue
		}

		// Skip nil contacts
		if contact == nil {
			log.Debug().Int64("id", id).Msg("Contact is nil, skipping")
			continue
		}

		// Ensure the contact has an ID field
		if _, ok := contact["id"]; !ok {
			contact["id"] = id
		}

		contacts = append(contacts, contact)
	}

	log.Info().
		Int("count", len(contacts)).
		Msg("Retrieved contacts from Autotask using sequential ID checks")

	return contacts, nil
}

// GetContact retrieves a single contact by ID
func (c *Client) GetContact(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Create a custom request to the API
	url := fmt.Sprintf("Contacts/%d", id)

	req, err := c.client.NewRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Define a response structure for a single contact
	var response struct {
		Item map[string]interface{} `json:"item"`
	}

	// Execute the request
	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get contact %d: %w", id, err)
	}

	// Check if the response item is nil
	if response.Item == nil {
		return nil, fmt.Errorf("contact %d not found", id)
	}

	return response.Item, nil
}

// GetTickets retrieves tickets from Autotask
func (c *Client) GetTickets(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try to get tickets using the proper filter format with date filter first
	tickets, err := c.queryWithProperFilterAndDate(ctx, "Tickets", "lastActivityDate", since)
	if err == nil && len(tickets) > 0 {
		log.Info().
			Int("count", len(tickets)).
			Time("since", since).
			Msg("Retrieved tickets from Autotask using proper filter format with lastActivityDate")
		return tickets, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get tickets with proper filter format and lastActivityDate, trying with LastActivityDate")

		// Try with a different field name (case sensitivity matters in some API versions)
		tickets, err = c.queryWithProperFilterAndDate(ctx, "Tickets", "LastActivityDate", since)
		if err == nil && len(tickets) > 0 {
			log.Info().
				Int("count", len(tickets)).
				Time("since", since).
				Msg("Retrieved tickets from Autotask using proper filter format with LastActivityDate")
			return tickets, nil
		}

		log.Warn().
			Err(err).
			Msg("Failed to get tickets with proper filter format, falling back to other methods")
	}

	// Try to get all tickets in a single API call
	allTickets, err := c.getAllEntities(ctx, "Tickets")
	if err == nil && len(allTickets) > 0 {
		// Filter tickets by the since date
		var filteredTickets []map[string]interface{}
		for _, ticket := range allTickets {
			if lastActivityDate, ok := ticket["lastActivityDate"]; ok {
				if timeVal, err := parseTime(lastActivityDate); err == nil {
					if !timeVal.Before(since) {
						filteredTickets = append(filteredTickets, ticket)
					}
				}
			}
		}

		log.Info().
			Int("total", len(allTickets)).
			Int("filtered", len(filteredTickets)).
			Time("since", since).
			Msg("Retrieved and filtered tickets from Autotask in a single API call")

		return filteredTickets, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all tickets in a single API call, trying query endpoint")
	}

	// Use the queryWithDateFilter method to get all tickets updated since the given time
	// Try with lastActivityDate first
	tickets, err = c.queryWithDateFilter(ctx, "Tickets", "lastActivityDate", since)
	if err != nil {
		// If that fails, try with LastActivityDate (case sensitivity matters in some API versions)
		log.Warn().
			Err(err).
			Time("since", since).
			Msg("Failed to query tickets with lastActivityDate, trying with LastActivityDate")

		tickets, err = c.queryWithDateFilter(ctx, "Tickets", "LastActivityDate", since)
		if err != nil {
			// If that also fails, try the ID ranges approach
			log.Warn().
				Err(err).
				Time("since", since).
				Msg("Failed to query tickets with date filter, trying ID ranges approach")

			// Try the ID ranges approach
			allTickets, err = c.getEntitiesByIdRanges(ctx, "Tickets", 50, 5000)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("Failed to get tickets by ID ranges, falling back to individual GET requests")

				return c.getTicketsIndividually(ctx, since)
			}

			// Filter tickets by the since date
			var filteredTickets []map[string]interface{}
			for _, ticket := range allTickets {
				if lastActivityDate, ok := ticket["lastActivityDate"]; ok {
					if timeVal, err := parseTime(lastActivityDate); err == nil {
						if !timeVal.Before(since) {
							filteredTickets = append(filteredTickets, ticket)
						}
					}
				}
			}

			log.Info().
				Int("total", len(allTickets)).
				Int("filtered", len(filteredTickets)).
				Time("since", since).
				Msg("Retrieved and filtered tickets from Autotask using ID ranges")

			return filteredTickets, nil
		}
	}

	log.Info().
		Int("count", len(tickets)).
		Time("since", since).
		Msg("Retrieved tickets from Autotask using date filter query")

	return tickets, nil
}

// getTicketsIndividually retrieves tickets one by one using their IDs
// This is a fallback method in case the query endpoint doesn't work
func (c *Client) getTicketsIndividually(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try the ID ranges approach with smaller batches
	allTickets, err := c.getEntitiesByIdRanges(ctx, "Tickets", 10, 1000)
	if err == nil && len(allTickets) > 0 {
		// Filter tickets by the since date
		var filteredTickets []map[string]interface{}
		for _, ticket := range allTickets {
			if lastActivityDate, ok := ticket["lastActivityDate"]; ok {
				if timeVal, err := parseTime(lastActivityDate); err == nil {
					if !timeVal.Before(since) {
						filteredTickets = append(filteredTickets, ticket)
					}
				}
			}
		}

		log.Info().
			Int("total", len(allTickets)).
			Int("filtered", len(filteredTickets)).
			Time("since", since).
			Msg("Retrieved and filtered tickets from Autotask using ID ranges with smaller batches")

		return filteredTickets, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get tickets by ID ranges with smaller batches, trying to get valid IDs")
	}

	// First, try to get a list of valid ticket IDs
	log.Info().Msg("Attempting to get list of valid ticket IDs")

	// Try different endpoints to get a list of IDs
	validIDs, err := c.getValidIDs(ctx, "Tickets")
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get list of valid ticket IDs, falling back to sequential ID checks")
		return c.getTicketsSequentially(ctx, since)
	}

	// Try to get tickets in batches with concurrency
	allTickets, err = c.batchGetEntitiesConcurrent(ctx, "Tickets", validIDs, 50)
	if err != nil || len(allTickets) == 0 {
		log.Warn().
			Err(err).
			Int("validIDs", len(validIDs)).
			Msg("Concurrent batch GET request failed or returned no tickets, falling back to sequential batch requests")

		// Try non-concurrent batch requests
		allTickets, err = c.batchGetEntities(ctx, "Tickets", validIDs, 50)
		if err != nil || len(allTickets) == 0 {
			log.Warn().
				Err(err).
				Int("validIDs", len(validIDs)).
				Msg("Batch GET request failed or returned no tickets, falling back to individual GET requests")

			// Create a list to hold all tickets
			allTickets = make([]map[string]interface{}, 0, len(validIDs))

			// Get each ticket by ID
			for _, id := range validIDs {
				ticket, err := c.GetTicket(ctx, id)
				if err != nil {
					log.Debug().Int64("id", id).Err(err).Msg("Failed to get ticket, skipping")
					continue
				}

				// Skip nil tickets
				if ticket == nil {
					log.Debug().Int64("id", id).Msg("Ticket is nil, skipping")
					continue
				}

				// Ensure the ticket has an ID field
				if _, ok := ticket["id"]; !ok {
					ticket["id"] = id
				}

				allTickets = append(allTickets, ticket)
			}
		}
	}

	// Filter tickets by the since date
	var tickets []map[string]interface{}
	for _, ticket := range allTickets {
		// Check if the ticket was updated since the given time
		if lastActivityDate, ok := ticket["lastActivityDate"]; ok {
			if timeVal, err := parseTime(lastActivityDate); err == nil {
				if !timeVal.Before(since) {
					tickets = append(tickets, ticket)
				}
			}
		}
	}

	log.Info().
		Int("total", len(allTickets)).
		Int("filtered", len(tickets)).
		Time("since", since).
		Msg("Retrieved and filtered tickets from Autotask")

	return tickets, nil
}

// getTicketsSequentially retrieves tickets by checking sequential IDs
// This is a last resort fallback method
func (c *Client) getTicketsSequentially(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Create a list to hold all tickets
	var tickets []map[string]interface{}

	// Try to retrieve tickets with sequential IDs
	// This is a temporary solution until we can get the query endpoint working
	for id := int64(1); id < 200; id++ {
		ticket, err := c.GetTicket(ctx, id)
		if err != nil {
			// Skip tickets that don't exist
			log.Debug().Int64("id", id).Msg("Ticket not found, skipping")
			continue
		}

		// Skip nil tickets
		if ticket == nil {
			log.Debug().Int64("id", id).Msg("Ticket is nil, skipping")
			continue
		}

		// Ensure the ticket has an ID field
		if _, ok := ticket["id"]; !ok {
			ticket["id"] = id
		}

		tickets = append(tickets, ticket)
	}

	log.Info().
		Int("count", len(tickets)).
		Time("since", since).
		Msg("Retrieved tickets from Autotask using sequential ID checks")

	return tickets, nil
}

// batchGetEntitiesConcurrent performs batch GET requests for multiple entities concurrently
// respecting the thread limits per object endpoint
func (c *Client) batchGetEntitiesConcurrent(ctx context.Context, entityName string, ids []int64, batchSize int) ([]map[string]interface{}, error) {
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	log.Info().
		Str("entity", entityName).
		Int("totalIds", len(ids)).
		Int("batchSize", batchSize).
		Msg("Performing concurrent batch GET requests")

	// Create a slice to hold all entities
	var allEntities []map[string]interface{}
	var mu sync.Mutex // Mutex to protect the allEntities slice

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a channel to collect errors
	errCh := make(chan error, len(ids)/batchSize+1)

	// Process IDs in batches
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batchIDs := ids[i:end]

		// Create a comma-separated list of IDs
		idStrings := make([]string, len(batchIDs))
		for j, id := range batchIDs {
			idStrings[j] = fmt.Sprintf("%d", id)
		}
		idList := strings.Join(idStrings, ",")

		// Create the URL with the ID list
		url := fmt.Sprintf("%s?ids=%s", entityName, idList)

		// Increment the wait group counter
		wg.Add(1)

		// Start a goroutine to process this batch
		go func(batchURL string, batchStart, batchEnd int, batchIDList string) {
			defer wg.Done()

			// Use the request limiter to control concurrency
			err := c.requestLimiter.ExecuteConcurrent(ctx, entityName, func() error {
				req, err := c.client.NewRequest(ctx, "GET", batchURL, nil)
				if err != nil {
					log.Warn().
						Str("entity", entityName).
						Str("ids", batchIDList).
						Err(err).
						Msg("Failed to create batch GET request")
					return err
				}

				var response struct {
					Items []map[string]interface{} `json:"items"`
				}

				// Execute the request
				resp, err := c.client.Do(req, &response)
				if err != nil {
					log.Warn().
						Str("entity", entityName).
						Str("ids", batchIDList).
						Err(err).
						Msg("Failed to execute batch GET request")
					return err
				}

				log.Info().
					Int("status", resp.StatusCode).
					Int("count", len(response.Items)).
					Int("batchSize", batchEnd-batchStart).
					Int("batchStart", batchStart).
					Int("batchEnd", batchEnd-1).
					Str("entity", entityName).
					Msg("Retrieved batch of entities")

				// Add the items to the result slice
				mu.Lock()
				allEntities = append(allEntities, response.Items...)
				mu.Unlock()

				return nil
			})

			if err != nil {
				errCh <- err
			}
		}(url, i, end, idList)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errCh)

	// Check if there were any errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		log.Warn().
			Int("errorCount", len(errs)).
			Msg("Some batch requests failed")
	}

	log.Info().
		Str("entity", entityName).
		Int("totalRetrieved", len(allEntities)).
		Int("totalRequested", len(ids)).
		Msg("Completed concurrent batch GET requests")

	return allEntities, nil
}

// GetTicket retrieves a single ticket by ID
func (c *Client) GetTicket(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Create a custom request to the API
	url := fmt.Sprintf("Tickets/%d", id)

	req, err := c.client.NewRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Define a response structure for a single ticket
	var response struct {
		Item map[string]interface{} `json:"item"`
	}

	// Execute the request
	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticket %d: %w", id, err)
	}

	// Check if the response item is nil
	if response.Item == nil {
		return nil, fmt.Errorf("ticket %d not found", id)
	}

	return response.Item, nil
}

// Helper function to parse time values
func parseTime(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case string:
		// Try parsing as RFC3339
		t, err := time.Parse(time.RFC3339, v)
		if err == nil {
			return t, nil
		}

		// Try parsing as ISO8601
		t, err = time.Parse("2006-01-02T15:04:05", v)
		if err == nil {
			return t, nil
		}

		// Try parsing as date only
		t, err = time.Parse("2006-01-02", v)
		if err == nil {
			return t, nil
		}

		return time.Time{}, fmt.Errorf("unable to parse time: %s", v)
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf("unexpected time type: %T", val)
	}
}

// getAllEntities attempts to get all entities of a specific type in a single API call
func (c *Client) getAllEntities(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Attempting to get all entities in a single API call")

	// Try the search endpoint first
	entities, err := c.tryAutotaskSearchEndpoint(ctx, entityName)
	if err == nil && len(entities) > 0 {
		return entities, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Str("entity", entityName).
			Msg("Search endpoint failed, trying direct API calls")
	}

	// Try direct API calls with different URL formats
	entities, err = c.tryDirectAPICall(ctx, entityName)
	if err == nil && len(entities) > 0 {
		return entities, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Str("entity", entityName).
			Msg("Direct API calls failed, trying different endpoints")
	}

	// Try different API endpoints
	return c.tryDifferentEndpoints(ctx, entityName)
}

// batchGetEntities performs batch GET requests for multiple entities at once
func (c *Client) batchGetEntities(ctx context.Context, entityName string, ids []int64, batchSize int) ([]map[string]interface{}, error) {
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	log.Info().
		Str("entity", entityName).
		Int("totalIds", len(ids)).
		Int("batchSize", batchSize).
		Msg("Performing batch entity requests")

	var allEntities []map[string]interface{}

	// Process IDs in batches
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batchIDs := ids[i:end]

		// Create a comma-separated list of IDs for logging
		idStrings := make([]string, len(batchIDs))
		for j, id := range batchIDs {
			idStrings[j] = fmt.Sprintf("%d", id)
		}
		idList := strings.Join(idStrings, ",")

		// Instead of trying to get all IDs at once, we'll use the getEntitiesByIdRanges approach
		// Find the min and max IDs in this batch
		minID := batchIDs[0]
		maxID := batchIDs[0]
		for _, id := range batchIDs {
			if id < minID {
				minID = id
			}
			if id > maxID {
				maxID = id
			}
		}

		// Create a query with a filter for the ID range
		url := entityName + "/query"

		// Create a filter for the ID range using the format that's known to work
		reqBody := map[string]interface{}{
			"MaxRecords": end - i,
			"filter": []map[string]interface{}{
				{
					"op":    "gte",
					"field": "id",
					"value": minID,
				},
				{
					"op":    "lte",
					"field": "id",
					"value": maxID,
				},
			},
		}

		reqBytes, err := json.Marshal(reqBody)
		if err != nil {
			log.Warn().
				Str("entity", entityName).
				Str("ids", idList).
				Err(err).
				Msg("Failed to marshal request body for batch query")
			continue
		}

		log.Debug().
			Str("request_body", string(reqBytes)).
			Str("entity", entityName).
			Str("ids", idList).
			Msg("Executing batch query")

		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			log.Warn().
				Str("entity", entityName).
				Str("ids", idList).
				Err(err).
				Msg("Failed to create batch query request")
			continue
		}

		// Add required headers
		req.Header.Set("Content-Type", "application/json")

		var response struct {
			Items []map[string]interface{} `json:"items"`
		}

		// Execute the request
		resp, err := c.client.Do(req, &response)
		if err != nil {
			log.Warn().
				Str("entity", entityName).
				Str("ids", idList).
				Err(err).
				Msg("Failed to execute batch query")
			continue
		}

		// Filter the results to only include the IDs we requested
		var filteredItems []map[string]interface{}
		idMap := make(map[int64]bool)
		for _, id := range batchIDs {
			idMap[id] = true
		}

		for _, item := range response.Items {
			if id, ok := item["id"].(float64); ok && idMap[int64(id)] {
				filteredItems = append(filteredItems, item)
			}
		}

		log.Info().
			Int("status", resp.StatusCode).
			Int("count", len(filteredItems)).
			Int("totalReturned", len(response.Items)).
			Int("batchSize", len(batchIDs)).
			Int("batchStart", i).
			Int("batchEnd", end-1).
			Str("entity", entityName).
			Msg("Retrieved batch of entities")

		allEntities = append(allEntities, filteredItems...)
	}

	log.Info().
		Str("entity", entityName).
		Int("totalRetrieved", len(allEntities)).
		Int("totalRequested", len(ids)).
		Msg("Completed batch entity requests")

	return allEntities, nil
}

// GetTimeEntries retrieves time entries from Autotask
func (c *Client) GetTimeEntries(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try to get time entries using the proper filter format with date filter first
	timeEntries, err := c.queryWithProperFilterAndDate(ctx, "TimeEntries", "lastModifiedDateTime", since)
	if err == nil && len(timeEntries) > 0 {
		log.Info().
			Int("count", len(timeEntries)).
			Time("since", since).
			Msg("Retrieved time entries from Autotask using proper filter format with lastModifiedDateTime")
		return timeEntries, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get time entries with proper filter format and lastModifiedDateTime, trying with LastModifiedDateTime")

		// Try with a different field name (case sensitivity matters in some API versions)
		timeEntries, err = c.queryWithProperFilterAndDate(ctx, "TimeEntries", "LastModifiedDateTime", since)
		if err == nil && len(timeEntries) > 0 {
			log.Info().
				Int("count", len(timeEntries)).
				Time("since", since).
				Msg("Retrieved time entries from Autotask using proper filter format with LastModifiedDateTime")
			return timeEntries, nil
		}

		log.Warn().
			Err(err).
			Msg("Failed to get time entries with proper filter format, falling back to other methods")
	}

	// Try to get all time entries in a single API call
	allTimeEntries, err := c.getAllEntities(ctx, "TimeEntries")
	if err == nil && len(allTimeEntries) > 0 {
		// Filter time entries by the since date
		var filteredTimeEntries []map[string]interface{}
		for _, entry := range allTimeEntries {
			if lastModifiedDate, ok := entry["lastModifiedDateTime"]; ok {
				if timeVal, err := parseTime(lastModifiedDate); err == nil {
					if !timeVal.Before(since) {
						filteredTimeEntries = append(filteredTimeEntries, entry)
					}
				}
			}
		}

		log.Info().
			Int("total", len(allTimeEntries)).
			Int("filtered", len(filteredTimeEntries)).
			Time("since", since).
			Msg("Retrieved and filtered time entries from Autotask in a single API call")

		return filteredTimeEntries, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all time entries in a single API call, trying query endpoint")
	}

	// Try with different field names for the date filter
	timeEntries, err = c.queryWithDateFilter(ctx, "TimeEntries", "LastModifiedDateTime", since)
	if err != nil {
		timeEntries, err = c.queryWithDateFilter(ctx, "TimeEntries", "lastModifiedDateTime", since)
		if err != nil {
			return nil, fmt.Errorf("failed to get time entries: %w", err)
		}
	}

	return timeEntries, nil
}

// GetResources retrieves resources from Autotask
func (c *Client) GetResources(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all resources in a single API call first
	resources, err := c.getAllEntities(ctx, "Resources")
	if err == nil && len(resources) > 0 {
		log.Info().
			Int("count", len(resources)).
			Msg("Retrieved all resources from Autotask in a single API call")
		return resources, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all resources in a single API call, trying query endpoint")
	}

	// Try with different endpoint names
	items, err := c.queryWithEmptyFilter(ctx, "Resources")
	if err != nil {
		items, err = c.queryWithEmptyFilter(ctx, "resources")
		if err != nil {
			return nil, fmt.Errorf("failed to get resources: %w", err)
		}
	}

	return items, nil
}

// GetContracts retrieves contracts from Autotask
func (c *Client) GetContracts(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all contracts in a single API call first
	contracts, err := c.getAllEntities(ctx, "Contracts")
	if err == nil && len(contracts) > 0 {
		log.Info().
			Int("count", len(contracts)).
			Msg("Retrieved all contracts from Autotask in a single API call")
		return contracts, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all contracts in a single API call, trying query endpoint")
	}

	// Try with different endpoint names
	items, err := c.queryWithEmptyFilter(ctx, "Contracts")
	if err != nil {
		items, err = c.queryWithEmptyFilter(ctx, "contracts")
		if err != nil {
			return nil, fmt.Errorf("failed to get contracts: %w", err)
		}
	}

	return items, nil
}

// GetProjects retrieves projects from Autotask
func (c *Client) GetProjects(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all projects in a single API call first
	projects, err := c.getAllEntities(ctx, "Projects")
	if err == nil && len(projects) > 0 {
		log.Info().
			Int("count", len(projects)).
			Msg("Retrieved all projects from Autotask in a single API call")
		return projects, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all projects in a single API call, trying query endpoint")
	}

	// Try with different endpoint names
	items, err := c.queryWithEmptyFilter(ctx, "Projects")
	if err != nil {
		items, err = c.queryWithEmptyFilter(ctx, "projects")
		if err != nil {
			return nil, fmt.Errorf("failed to get projects: %w", err)
		}
	}

	return items, nil
}

// GetTasks retrieves tasks from Autotask
func (c *Client) GetTasks(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all tasks in a single API call first
	tasks, err := c.getAllEntities(ctx, "Tasks")
	if err == nil && len(tasks) > 0 {
		log.Info().
			Int("count", len(tasks)).
			Msg("Retrieved all tasks from Autotask in a single API call")
		return tasks, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all tasks in a single API call, trying query endpoint")
	}

	// Try with different endpoint names
	items, err := c.queryWithEmptyFilter(ctx, "Tasks")
	if err != nil {
		items, err = c.queryWithEmptyFilter(ctx, "tasks")
		if err != nil {
			return nil, fmt.Errorf("failed to get tasks: %w", err)
		}
	}

	return items, nil
}

// GetConfigurationItems retrieves configuration items from Autotask
func (c *Client) GetConfigurationItems(ctx context.Context) ([]map[string]interface{}, error) {
	// Try to get all configuration items in a single API call first
	configItems, err := c.getAllEntities(ctx, "ConfigurationItems")
	if err == nil && len(configItems) > 0 {
		log.Info().
			Int("count", len(configItems)).
			Msg("Retrieved all configuration items from Autotask in a single API call")
		return configItems, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get all configuration items in a single API call, trying query endpoint")
	}

	// Try with different endpoint names
	items, err := c.queryWithEmptyFilter(ctx, "ConfigurationItems")
	if err != nil {
		items, err = c.queryWithEmptyFilter(ctx, "configurationItems")
		if err != nil {
			return nil, fmt.Errorf("failed to get configuration items: %w", err)
		}
	}

	return items, nil
}

// RegisterWebhook registers a new webhook in Autotask
func (c *Client) RegisterWebhook(ctx context.Context, url, description string, entityTypes []string) (map[string]interface{}, error) {
	// Create webhook payload
	payload := map[string]interface{}{
		"Active":          true,
		"DeactivationURL": url + "/deactivate",
		"Description":     description,
		"EntityTypes":     entityTypes,
		"IsActive":        true,
		"Name":            "Autotask Data Sink Webhook",
		"SecretKey":       "your-secret-key", // This should be configurable
		"SendThreshold":   10,                // Number of events to batch
		"URL":             url,
	}

	// Register webhook
	response, err := c.client.Webhooks().Create(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to register webhook: %w", err)
	}

	// Convert interface{} to map[string]interface{}
	webhookMap, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to convert webhook response to map: %v", response)
	}

	log.Info().
		Interface("webhook", webhookMap).
		Msg("Registered webhook in Autotask")

	return webhookMap, nil
}

// tryDirectAPICall attempts to get entities using different URL formats
func (c *Client) tryDirectAPICall(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Attempting direct API calls with different URL formats")

	// Try different URL formats
	urlFormats := []string{
		entityName,                          // e.g., "Companies"
		strings.ToLower(entityName),         // e.g., "companies"
		entityName + "s",                    // e.g., "Company" -> "Companys" (incorrect but worth trying)
		strings.TrimSuffix(entityName, "s"), // e.g., "Companies" -> "Companie" (incorrect but worth trying)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}
	var err error
	var succeeded bool

	for _, url := range urlFormats {
		req, err := c.client.NewRequest(ctx, "GET", url, nil)
		if err != nil {
			continue
		}

		log.Debug().
			Str("url", url).
			Msg("Trying direct API call")

		_, err = c.client.Do(req, &response)
		if err == nil {
			succeeded = true
			break
		}

		log.Debug().
			Err(err).
			Str("url", url).
			Msg("Direct API call failed")
	}

	if !succeeded {
		return nil, fmt.Errorf("all direct API calls failed for %s: %w", entityName, err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entities from Autotask using direct API call")

	return response.Items, nil
}

// tryDifferentEndpoints attempts to get entities using different API endpoints
func (c *Client) tryDifferentEndpoints(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Attempting to get entities using different API endpoints")

	// Try different endpoints
	endpoints := []struct {
		method string
		path   string
		body   interface{}
	}{
		// GET with no query parameters
		{
			method: "GET",
			path:   entityName,
			body:   nil,
		},
		// GET with query parameter
		{
			method: "GET",
			path:   entityName + "?search=",
			body:   nil,
		},
		// POST with empty body
		{
			method: "POST",
			path:   entityName,
			body:   map[string]interface{}{},
		},
		// POST with search query
		{
			method: "POST",
			path:   entityName + "/search",
			body:   map[string]interface{}{},
		},
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}
	var err error
	var succeeded bool

	for _, endpoint := range endpoints {
		var req *http.Request

		if endpoint.body != nil {
			bodyBytes, err := json.Marshal(endpoint.body)
			if err != nil {
				continue
			}

			req, err = c.client.NewRequest(ctx, endpoint.method, endpoint.path, bytes.NewBuffer(bodyBytes))
		} else {
			req, err = c.client.NewRequest(ctx, endpoint.method, endpoint.path, nil)
		}

		if err != nil {
			continue
		}

		log.Debug().
			Str("method", endpoint.method).
			Str("path", endpoint.path).
			Msg("Trying API endpoint")

		_, err = c.client.Do(req, &response)
		if err == nil {
			succeeded = true
			break
		}

		log.Debug().
			Err(err).
			Str("method", endpoint.method).
			Str("path", endpoint.path).
			Msg("API endpoint failed")
	}

	if !succeeded {
		return nil, fmt.Errorf("all API endpoints failed for %s: %w", entityName, err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entities from Autotask using alternative API endpoint")

	return response.Items, nil
}

// tryAutotaskDocumentationFormat attempts to query entities using the format from the Autotask API documentation
func (c *Client) tryAutotaskDocumentationFormat(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Attempting to query entities using Autotask documentation format")

	// This format is based on the Autotask API documentation
	// https://ww2.autotask.net/help/developerhelp/Content/APIs/REST/API_Calls/REST_Basic_Query_Calls.htm

	url := entityName + "/query"

	// Create the request body according to the documentation
	reqBody := map[string]interface{}{
		"MaxRecords": 500,
		"filter": []interface{}{
			map[string]interface{}{
				"field": "id",
				"op":    "gt",
				"value": 0,
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Msg("API request body using documentation format")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entities using documentation format")

	return response.Items, nil
}

// tryExactAutotaskFormat attempts to query entities using the exact format required by the Autotask API
func (c *Client) tryExactAutotaskFormat(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	url := entityName + "/query"

	// Try different formats based on the Autotask documentation examples
	// Format 1: Simple filter array with direct objects
	reqBody1 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "exist",
				"field": "id",
			},
		},
		"MaxRecords": 500,
	}

	// Format 2: Using the exact format from the documentation example
	reqBody2 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "eq",
				"field": "isActive",
				"value": true,
			},
		},
		"MaxRecords": 500,
	}

	// Format 3: Using a simple "greater than 0" filter on ID
	reqBody3 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "gt",
				"field": "id",
				"value": 0,
			},
		},
		"MaxRecords": 500,
	}

	// Format 4: Using a complex filter with AND/OR as shown in documentation
	reqBody4 := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "exist",
				"field": "id",
			},
			{
				"op": "or",
				"items": []map[string]interface{}{
					{
						"op":    "gt",
						"field": "id",
						"value": 0,
					},
				},
			},
		},
		"MaxRecords": 500,
	}

	// Try each format in sequence
	formats := []map[string]interface{}{reqBody1, reqBody2, reqBody3, reqBody4}
	descriptions := []string{
		"simple exist filter",
		"isActive filter from docs",
		"id > 0 filter",
		"complex filter with OR",
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	for i, reqBody := range formats {
		reqBytes, err := json.Marshal(reqBody)
		if err != nil {
			continue
		}

		log.Debug().
			Str("request_body", string(reqBytes)).
			Str("entity", entityName).
			Int("format", i+1).
			Str("description", descriptions[i]).
			Msg("Trying exact format")

		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			continue
		}

		// Add special headers that might be required
		req.Header.Set("Content-Type", "application/json")

		_, err = c.client.Do(req, &response)
		if err == nil {
			log.Info().
				Int("count", len(response.Items)).
				Str("entity", entityName).
				Int("format", i+1).
				Str("description", descriptions[i]).
				Msg("Successfully retrieved entities with format")
			return response.Items, nil
		}

		log.Debug().
			Err(err).
			Int("format", i+1).
			Str("description", descriptions[i]).
			Msg("Format failed")
	}

	return nil, fmt.Errorf("all exact formats failed for %s", entityName)
}

// tryExactAutotaskFormatWithDate attempts to query entities using the exact format required by the Autotask API with a date filter
func (c *Client) tryExactAutotaskFormatWithDate(ctx context.Context, entityName, fieldName string, since time.Time) ([]map[string]interface{}, error) {
	log.Info().
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Attempting to query entities using exact Autotask format with date filter")

	url := entityName + "/query"

	// Try alternative formats
	alternativeFormats := []struct {
		description string
		body        map[string]interface{}
	}{
		{
			description: "filters.filter array",
			body: map[string]interface{}{
				"filters": map[string]interface{}{
					"filter": []map[string]interface{}{
						{
							"op":    "gt",
							"field": fieldName,
							"value": since.Format(time.RFC3339),
						},
					},
				},
			},
		},
		{
			description: "filter array",
			body: map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
			},
		},
		{
			description: "filters array",
			body: map[string]interface{}{
				"filters": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
			},
		},
		{
			description: "filter and filters arrays",
			body: map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
				"filters": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
			},
		},
		{
			description: "filter with includeFields",
			body: map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
				"includeFields": []string{"id", "title", fieldName},
			},
		},
		{
			description: "filter as single object",
			body: map[string]interface{}{
				"filter": map[string]interface{}{
					"op":    "gt",
					"field": fieldName,
					"value": since.Format(time.RFC3339),
				},
			},
		},
		{
			description: "filters as single object",
			body: map[string]interface{}{
				"filters": map[string]interface{}{
					"op":    "gt",
					"field": fieldName,
					"value": since.Format(time.RFC3339),
				},
			},
		},
		{
			description: "filter with items array",
			body: map[string]interface{}{
				"filter": map[string]interface{}{
					"items": []map[string]interface{}{
						{
							"op":    "gt",
							"field": fieldName,
							"value": since.Format(time.RFC3339),
						},
					},
				},
			},
		},
		{
			description: "filters with items array",
			body: map[string]interface{}{
				"filters": map[string]interface{}{
					"items": []map[string]interface{}{
						{
							"op":    "gt",
							"field": fieldName,
							"value": since.Format(time.RFC3339),
						},
					},
				},
			},
		},
		{
			description: "different date format (ISO8601)",
			body: map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format("2006-01-02T15:04:05"),
					},
				},
			},
		},
		{
			description: "date only format",
			body: map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gt",
						"field": fieldName,
						"value": since.Format("2006-01-02"),
					},
				},
			},
		},
		{
			description: "empty filter object",
			body: map[string]interface{}{
				"filter": map[string]interface{}{},
			},
		},
		{
			description: "empty filters object",
			body: map[string]interface{}{
				"filters": map[string]interface{}{},
			},
		},
		{
			description: "no filter, just MaxRecords",
			body: map[string]interface{}{
				"MaxRecords": 500,
			},
		},
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}
	var err error
	var succeeded bool
	var successFormat string

	for i, format := range alternativeFormats {
		reqBytes, err := json.Marshal(format.body)
		if err != nil {
			continue
		}

		log.Debug().
			Str("request_body", string(reqBytes)).
			Str("entity", entityName).
			Str("field", fieldName).
			Time("since", since).
			Int("format", i+1).
			Str("description", format.description).
			Msg("Trying exact Autotask format with date filter")

		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			continue
		}

		// Add special headers that might be required
		req.Header.Set("Content-Type", "application/json")

		httpResp, err := c.client.Do(req, &response)
		if err == nil {
			succeeded = true
			successFormat = format.description

			// Log the raw response for debugging
			if httpResp != nil && httpResp.Body != nil {
				log.Info().
					Int("status", httpResp.StatusCode).
					Int("count", len(response.Items)).
					Str("format", format.description).
					Msg("Format succeeded")
			}

			break
		}

		// Log the error details
		log.Debug().
			Err(err).
			Int("format", i+1).
			Str("description", format.description).
			Msg("Format failed")

		// Log the raw response for debugging
		if httpResp != nil && httpResp.Body != nil {
			bodyBytes, _ := io.ReadAll(httpResp.Body)
			log.Debug().
				Str("response_body", string(bodyBytes)).
				Msg("Error response body")
		}
	}

	if !succeeded {
		return nil, fmt.Errorf("all exact formats with date filter failed for %s: %w", entityName, err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Str("format", successFormat).
		Msg("Retrieved entities using exact Autotask format with date filter")

	return response.Items, nil
}

// tryAutotaskSearchEndpoint attempts to use the Autotask API's search endpoint
func (c *Client) tryAutotaskSearchEndpoint(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Attempting to use Autotask search endpoint")

	// Try different search endpoint formats
	searchEndpoints := []struct {
		description string
		url         string
		method      string
		body        interface{}
	}{
		{
			description: "POST to search endpoint with empty body",
			url:         entityName + "/search",
			method:      "POST",
			body:        map[string]interface{}{},
		},
		{
			description: "POST to search endpoint with query parameter",
			url:         entityName + "/search?query=",
			method:      "POST",
			body:        map[string]interface{}{},
		},
		{
			description: "POST to search endpoint with search term in body",
			url:         entityName + "/search",
			method:      "POST",
			body: map[string]interface{}{
				"search": "",
			},
		},
		{
			description: "POST to query endpoint with search parameter",
			url:         entityName + "/query",
			method:      "POST",
			body: map[string]interface{}{
				"search": "",
			},
		},
		{
			description: "GET to base endpoint",
			url:         entityName,
			method:      "GET",
			body:        nil,
		},
		{
			description: "GET to base endpoint with limit",
			url:         entityName + "?limit=500",
			method:      "GET",
			body:        nil,
		},
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}
	var err error
	var succeeded bool
	var successEndpoint string

	for i, endpoint := range searchEndpoints {
		var req *http.Request

		if endpoint.body != nil {
			reqBytes, err := json.Marshal(endpoint.body)
			if err != nil {
				continue
			}

			log.Debug().
				Str("request_body", string(reqBytes)).
				Str("entity", entityName).
				Str("url", endpoint.url).
				Str("method", endpoint.method).
				Int("endpoint", i+1).
				Str("description", endpoint.description).
				Msg("Trying Autotask search endpoint")

			req, err = c.client.NewRequest(ctx, endpoint.method, endpoint.url, bytes.NewBuffer(reqBytes))
		} else {
			log.Debug().
				Str("entity", entityName).
				Str("url", endpoint.url).
				Str("method", endpoint.method).
				Int("endpoint", i+1).
				Str("description", endpoint.description).
				Msg("Trying Autotask search endpoint")

			req, err = c.client.NewRequest(ctx, endpoint.method, endpoint.url, nil)
		}

		if err != nil {
			continue
		}

		// Add special headers that might be required
		req.Header.Set("Content-Type", "application/json")

		httpResp, err := c.client.Do(req, &response)
		if err == nil {
			succeeded = true
			successEndpoint = endpoint.description

			// Log the raw response for debugging
			if httpResp != nil && httpResp.Body != nil {
				log.Info().
					Int("status", httpResp.StatusCode).
					Int("count", len(response.Items)).
					Str("endpoint", endpoint.description).
					Msg("Search endpoint succeeded")
			}

			break
		}

		// Log the error details
		log.Debug().
			Err(err).
			Int("endpoint", i+1).
			Str("description", endpoint.description).
			Msg("Search endpoint failed")

		// Log the raw response for debugging
		if httpResp != nil && httpResp.Body != nil {
			bodyBytes, _ := io.ReadAll(httpResp.Body)
			log.Debug().
				Str("response_body", string(bodyBytes)).
				Msg("Error response body")
		}
	}

	if !succeeded {
		return nil, fmt.Errorf("all search endpoints failed for %s: %w", entityName, err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Str("endpoint", successEndpoint).
		Msg("Retrieved entities using Autotask search endpoint")

	return response.Items, nil
}

// getEntitiesByIdRanges retrieves entities by querying for ranges of IDs
// This is a more reliable approach than trying to use filters
func (c *Client) getEntitiesByIdRanges(ctx context.Context, entityName string, batchSize int, maxId int64) ([]map[string]interface{}, error) {
	if batchSize <= 0 {
		batchSize = 50
	}

	if maxId <= 0 {
		maxId = 10000 // Default max ID to try
	}

	log.Info().
		Str("entity", entityName).
		Int("batchSize", batchSize).
		Int64("maxId", maxId).
		Msg("Retrieving entities by ID ranges")

	var allEntities []map[string]interface{}
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Create a semaphore to limit concurrent requests
	sem := make(chan struct{}, 3) // Limit to 3 concurrent requests per entity type

	// Process IDs in batches
	for startId := int64(1); startId <= maxId; startId += int64(batchSize) {
		endId := startId + int64(batchSize) - 1
		if endId > maxId {
			endId = maxId
		}

		wg.Add(1)

		// Start a goroutine to process this batch
		go func(start, end int64) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Try to get entities in this range using the query endpoint with proper filter format
			url := entityName + "/query"

			// Create the filter for ID range using the simplest possible format
			// Based on the Autotask documentation example
			reqBody := map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"op":    "gte",
						"field": "id",
						"value": start,
					},
					{
						"op":    "lte",
						"field": "id",
						"value": end,
					},
				},
				"MaxRecords": end - start + 1,
			}

			reqBytes, err := json.Marshal(reqBody)
			if err != nil {
				log.Warn().
					Err(err).
					Int64("startId", start).
					Int64("endId", end).
					Str("entity", entityName).
					Msg("Failed to marshal request body for ID range query")
				return
			}

			log.Debug().
				Str("request_body", string(reqBytes)).
				Int64("startId", start).
				Int64("endId", end).
				Str("entity", entityName).
				Msg("Querying ID range")

			req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
			if err != nil {
				log.Warn().
					Err(err).
					Int64("startId", start).
					Int64("endId", end).
					Str("entity", entityName).
					Msg("Failed to create request for ID range query")
				return
			}

			// Add special headers that might be required
			req.Header.Set("Content-Type", "application/json")

			var response struct {
				Items []map[string]interface{} `json:"items"`
			}

			_, err = c.client.Do(req, &response)
			if err != nil {
				log.Warn().
					Err(err).
					Int64("startId", start).
					Int64("endId", end).
					Str("entity", entityName).
					Msg("Failed to execute ID range query, falling back to batch GET")

				// Fall back to batch GET
				var ids []int64
				for id := start; id <= end; id++ {
					ids = append(ids, id)
				}

				// Try to get entities in this range using batch GET
				entities, err := c.batchGetEntities(ctx, entityName, ids, len(ids))
				if err != nil {
					log.Warn().
						Err(err).
						Int64("startId", start).
						Int64("endId", end).
						Str("entity", entityName).
						Msg("Failed to get entities in range")
					return
				}

				// Add the entities to the result
				mu.Lock()
				allEntities = append(allEntities, entities...)
				mu.Unlock()

				log.Info().
					Int("count", len(entities)).
					Int64("startId", start).
					Int64("endId", end).
					Str("entity", entityName).
					Msg("Retrieved entities in ID range using batch GET")
				return
			}

			// Add the entities to the result
			mu.Lock()
			allEntities = append(allEntities, response.Items...)
			mu.Unlock()

			log.Info().
				Int("count", len(response.Items)).
				Int64("startId", start).
				Int64("endId", end).
				Str("entity", entityName).
				Msg("Retrieved entities in ID range using query")
		}(startId, endId)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Info().
		Int("count", len(allEntities)).
		Str("entity", entityName).
		Msg("Retrieved entities by ID ranges")

	return allEntities, nil
}

// queryWithProperFilter executes a query against the Autotask API using the proper filter format
// This follows the exact format specified in the Autotask API documentation
func (c *Client) queryWithProperFilter(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Info().Str("entity", entityName).Msg("Querying with proper filter format")

	url := entityName + "/query"

	// Use the exact format from the Autotask API documentation
	// The filter must be an array with at least one condition
	reqBody := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "gt", // greater than
				"field": "id",
				"value": 0, // all valid IDs are greater than 0
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Msg("Query with proper filter")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add required headers
	req.Header.Set("Content-Type", "application/json")

	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails struct {
			Count        int    `json:"count"`
			RequestCount int    `json:"requestCount"`
			PrevPageUrl  string `json:"prevPageUrl"`
			NextPageUrl  string `json:"nextPageUrl"`
		} `json:"pageDetails"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Msg("Retrieved entities using query with proper filter")

	// If there are more pages, fetch them
	allItems := response.Items
	nextPageUrl := response.PageDetails.NextPageUrl

	for nextPageUrl != "" && nextPageUrl != "null" {
		log.Debug().
			Str("nextPageUrl", nextPageUrl).
			Str("entity", entityName).
			Msg("Fetching next page")

		// Extract the relative URL from the full URL
		parts := strings.Split(nextPageUrl, "/v1.0/")
		if len(parts) < 2 {
			log.Warn().
				Str("nextPageUrl", nextPageUrl).
				Msg("Invalid next page URL format, stopping pagination")
			break
		}
		relativeUrl := parts[1]

		req, err := c.client.NewRequest(ctx, "GET", relativeUrl, nil)
		if err != nil {
			log.Warn().
				Err(err).
				Str("nextPageUrl", nextPageUrl).
				Msg("Failed to create request for next page, stopping pagination")
			break
		}

		var pageResponse struct {
			Items       []map[string]interface{} `json:"items"`
			PageDetails struct {
				Count        int    `json:"count"`
				RequestCount int    `json:"requestCount"`
				PrevPageUrl  string `json:"prevPageUrl"`
				NextPageUrl  string `json:"nextPageUrl"`
			} `json:"pageDetails"`
		}

		_, err = c.client.Do(req, &pageResponse)
		if err != nil {
			log.Warn().
				Err(err).
				Str("nextPageUrl", nextPageUrl).
				Msg("Failed to fetch next page, stopping pagination")
			break
		}

		allItems = append(allItems, pageResponse.Items...)
		nextPageUrl = pageResponse.PageDetails.NextPageUrl

		log.Info().
			Int("page_count", len(pageResponse.Items)).
			Int("total_count", len(allItems)).
			Str("entity", entityName).
			Msg("Retrieved additional page of entities")
	}

	log.Info().
		Int("total_count", len(allItems)).
		Str("entity", entityName).
		Msg("Retrieved all entities using query with proper filter")

	return allItems, nil
}

// queryWithProperFilterAndDate executes a query against the Autotask API using the proper filter format with a date filter
// This follows the exact format specified in the Autotask API documentation
func (c *Client) queryWithProperFilterAndDate(ctx context.Context, entityName, dateField string, since time.Time) ([]map[string]interface{}, error) {
	log.Info().
		Str("entity", entityName).
		Str("dateField", dateField).
		Time("since", since).
		Msg("Querying with proper filter format and date filter")

	url := entityName + "/query"

	// Format the date as ISO 8601 string
	sinceStr := since.Format(time.RFC3339)

	// Use the exact format from the Autotask API documentation
	// The filter must be an array with at least one condition
	reqBody := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op":    "gt", // greater than
				"field": "id",
				"value": 0, // all valid IDs are greater than 0
			},
			{
				"op":    "gte", // greater than or equal
				"field": dateField,
				"value": sinceStr,
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Str("dateField", dateField).
		Time("since", since).
		Msg("Query with proper filter and date")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add required headers
	req.Header.Set("Content-Type", "application/json")

	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails struct {
			Count        int    `json:"count"`
			RequestCount int    `json:"requestCount"`
			PrevPageUrl  string `json:"prevPageUrl"`
			NextPageUrl  string `json:"nextPageUrl"`
		} `json:"pageDetails"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Str("entity", entityName).
		Str("dateField", dateField).
		Time("since", since).
		Msg("Retrieved entities using query with proper filter and date")

	// If there are more pages, fetch them
	allItems := response.Items
	nextPageUrl := response.PageDetails.NextPageUrl

	for nextPageUrl != "" && nextPageUrl != "null" {
		log.Debug().
			Str("nextPageUrl", nextPageUrl).
			Str("entity", entityName).
			Msg("Fetching next page")

		// Extract the relative URL from the full URL
		parts := strings.Split(nextPageUrl, "/v1.0/")
		if len(parts) < 2 {
			log.Warn().
				Str("nextPageUrl", nextPageUrl).
				Msg("Invalid next page URL format, stopping pagination")
			break
		}
		relativeUrl := parts[1]

		req, err := c.client.NewRequest(ctx, "GET", relativeUrl, nil)
		if err != nil {
			log.Warn().
				Err(err).
				Str("nextPageUrl", nextPageUrl).
				Msg("Failed to create request for next page, stopping pagination")
			break
		}

		var pageResponse struct {
			Items       []map[string]interface{} `json:"items"`
			PageDetails struct {
				Count        int    `json:"count"`
				RequestCount int    `json:"requestCount"`
				PrevPageUrl  string `json:"prevPageUrl"`
				NextPageUrl  string `json:"nextPageUrl"`
			} `json:"pageDetails"`
		}

		_, err = c.client.Do(req, &pageResponse)
		if err != nil {
			log.Warn().
				Err(err).
				Str("nextPageUrl", nextPageUrl).
				Msg("Failed to fetch next page, stopping pagination")
			break
		}

		allItems = append(allItems, pageResponse.Items...)
		nextPageUrl = pageResponse.PageDetails.NextPageUrl

		log.Info().
			Int("page_count", len(pageResponse.Items)).
			Int("total_count", len(allItems)).
			Str("entity", entityName).
			Msg("Retrieved additional page of entities")
	}

	log.Info().
		Int("total_count", len(allItems)).
		Str("entity", entityName).
		Str("dateField", dateField).
		Time("since", since).
		Msg("Retrieved all entities using query with proper filter and date")

	return allItems, nil
}

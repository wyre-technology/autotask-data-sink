package autotask

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bytes"

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

	// Entity handlers
	Companies          *CompanyHandler
	Contacts           *ContactHandler
	Tickets            *TicketHandler
	TimeEntries        *TimeEntryHandler
	Resources          *ResourceHandler
	Contracts          *ContractHandler
	ActionTypes        *ActionTypeHandler
	Appointments       *AppointmentHandler
	Articles           *ArticleHandler
	ArticleNotes       *ArticleNoteHandler
	ConfigurationItems *ConfigurationItemHandler
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

	c := &Client{
		client:         client,
		requestLimiter: requestLimiter,
	}

	// Initialize entity handlers
	c.Companies = NewCompanyHandler(c)
	c.Contacts = NewContactHandler(c)
	c.Tickets = NewTicketHandler(c)
	c.TimeEntries = NewTimeEntryHandler(c)
	c.Resources = NewResourceHandler(c)
	c.Contracts = NewContractHandler(c)
	c.ActionTypes = NewActionTypeHandler(c)
	c.Appointments = NewAppointmentHandler(c)
	c.Articles = NewArticleHandler(c)
	c.ArticleNotes = NewArticleNoteHandler(c)
	c.ConfigurationItems = NewConfigurationItemHandler(c)

	return c, nil
}

// queryWithEmptyFilter is a helper method to query an entity with an empty filter array
func (c *Client) queryWithEmptyFilter(ctx context.Context, entityName string) ([]map[string]interface{}, error) {
	log.Debug().
		Str("entity", entityName).
		Msg("Querying with empty filter")

	filter := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op": "and",
				"items": []map[string]interface{}{
					{
						"op":    "exist",
						"field": "id",
					},
				},
			},
		},
	}

	jsonFilter, err := json.Marshal(filter)
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Err(err).
			Msg("Failed to marshal filter")
		return nil, fmt.Errorf("failed to marshal filter: %w", err)
	}

	url := fmt.Sprintf("/ATServicesRest/V1.0/%s/query", entityName)
	req, err := c.NewRequest(ctx, http.MethodPost, url, bytes.NewBuffer(jsonFilter))
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Str("url", url).
			Err(err).
			Msg("Failed to create request")
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var result struct {
		Items []map[string]interface{} `json:"items"`
	}

	resp, err := c.Do(req, &result)
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Str("url", url).
			Err(err).
			Msg("Failed to execute request")
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Debug().
		Str("entity", entityName).
		Int("count", len(result.Items)).
		Int("status", resp.StatusCode).
		Msg("Query completed successfully")

	return result.Items, nil
}

// tryAutotaskDocumentationFormatWithDate attempts to query entities using the format from the Autotask API documentation with a date filter
func (c *Client) tryAutotaskDocumentationFormatWithDate(ctx context.Context, entityName, fieldName string, since time.Time) ([]map[string]interface{}, error) {
	log.Info().
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Attempting to query entities using Autotask documentation format with date filter")

	url := entityName + "/query"

	// Use the absolute simplest filter structure possible
	reqBody := map[string]interface{}{
		"MaxRecords": 500,
		"filter": []map[string]interface{}{
			{
				"op": "and",
				"items": []map[string]interface{}{
					{
						"field": "id",
						"op":    "gt",
						"value": 0,
					},
				},
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
		Msg("API request body using documentation format with basic filter")

	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request with basic filter: %w", err)
	}

	// If we get here, we can try the date filter
	reqBody = map[string]interface{}{
		"MaxRecords": 500,
		"filter": []map[string]interface{}{
			{
				"op": "and",
				"items": []map[string]interface{}{
					{
						"field": fieldName,
						"op":    "gt",
						"value": since.UTC().Format(time.RFC3339),
					},
				},
			},
		},
	}

	reqBytes, err = json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	log.Debug().
		Str("request_body", string(reqBytes)).
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("API request body using documentation format with date filter")

	req, err = c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	_, err = c.client.Do(req, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request with date filter: %w", err)
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
	log.Debug().
		Str("entity", entityName).
		Str("field", fieldName).
		Time("since", since).
		Msg("Querying with date filter")

	filter := map[string]interface{}{
		"filter": []map[string]interface{}{
			{
				"op": "and",
				"items": []map[string]interface{}{
					{
						"op":    "gte",
						"field": fieldName,
						"value": since.Format(time.RFC3339),
					},
				},
			},
		},
	}

	jsonFilter, err := json.Marshal(filter)
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Str("field", fieldName).
			Time("since", since).
			Err(err).
			Msg("Failed to marshal date filter")
		return nil, fmt.Errorf("failed to marshal filter: %w", err)
	}

	url := fmt.Sprintf("/ATServicesRest/V1.0/%s/query", entityName)
	req, err := c.NewRequest(ctx, http.MethodPost, url, bytes.NewBuffer(jsonFilter))
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Str("url", url).
			Err(err).
			Msg("Failed to create request")
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var result struct {
		Items []map[string]interface{} `json:"items"`
	}

	resp, err := c.Do(req, &result)
	if err != nil {
		log.Error().
			Str("entity", entityName).
			Str("url", url).
			Err(err).
			Msg("Failed to execute request")
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	log.Debug().
		Str("entity", entityName).
		Int("count", len(result.Items)).
		Int("status", resp.StatusCode).
		Msg("Date filter query completed successfully")

	return result.Items, nil
}

// GetCompanies retrieves companies from Autotask
func (c *Client) GetCompanies(ctx context.Context) ([]map[string]interface{}, error) {
	// First try using queryWithEmptyFilter
	companies, err := c.queryWithEmptyFilter(ctx, "Companies")
	if err == nil {
		return companies, nil
	}

	// If that fails, try sequential approach
	return c.getCompaniesSequentially(ctx)
}

// getCompaniesIndividually retrieves companies one by one
func (c *Client) getCompaniesIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	// First try to get a list of valid company IDs
	validIDs, err := c.getValidIDs(ctx, "Companies")
	if err != nil {
		return nil, fmt.Errorf("failed to get valid company IDs: %w", err)
	}

	validIDsList := make([]int64, 0, len(validIDs))
	for _, id := range validIDs {
		if id > 0 {
			validIDsList = append(validIDsList, id)
		}
	}
	if len(validIDsList) == 0 {
		return []map[string]interface{}{}, nil
	}

	var companies []map[string]interface{}
	for _, id := range validIDsList {
		url := fmt.Sprintf("Companies/%d", id)
		req, err := c.client.NewRequest(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		var response struct {
			Item map[string]interface{} `json:"item"`
		}

		if err := c.requestLimiter.ExecuteConcurrent(ctx, "Companies", func() error {
			_, err := c.client.Do(req, &response)
			return err
		}); err != nil {
			if strings.Contains(err.Error(), "404") {
				continue
			}
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}

		companies = append(companies, response.Item)
	}

	return companies, nil
}

// getCompaniesSequentially retrieves companies by checking sequential IDs
func (c *Client) getCompaniesSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	// Try different ID ranges to find companies
	var allCompanies []map[string]interface{}
	batchSize := 100
	maxID := int64(1000000) // Adjust this based on your data

	for startID := int64(1); startID < maxID; startID += int64(batchSize - 1) {
		endID := startID + int64(batchSize-1)
		companies, err := c.queryIDRange(ctx, "Companies", startID, endID)
		if err != nil {
			continue
		}
		allCompanies = append(allCompanies, companies...)
	}

	return allCompanies, nil
}

// getValidIDs retrieves a list of valid IDs for the given entity type
func (c *Client) getValidIDs(ctx context.Context, entityType string) ([]int64, error) {
	// Try to get a list of valid IDs
	validIDs := make([]int64, 0)

	// First try using queryWithEmptyFilter
	entities, err := c.queryWithEmptyFilter(ctx, entityType)
	if err == nil && len(entities) > 0 {
		for _, entity := range entities {
			if id, ok := entity["id"].(float64); ok {
				validIDs = append(validIDs, int64(id))
			}
		}
		return validIDs, nil
	}

	return validIDs, fmt.Errorf("failed to get valid IDs: %w", err)
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
	// First try using queryWithEmptyFilter
	contacts, err := c.queryWithEmptyFilter(ctx, "Contacts")
	if err == nil {
		return contacts, nil
	}

	// If that fails, try sequential approach
	return c.getContactsSequentially(ctx)
}

// getContactsIndividually retrieves contacts one by one
func (c *Client) getContactsIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	// First try to get a list of valid contact IDs
	validIDs, err := c.getValidIDs(ctx, "Contacts")
	if err != nil {
		return nil, fmt.Errorf("failed to get valid contact IDs: %w", err)
	}

	validIDsList := make([]int64, 0, len(validIDs))
	for _, id := range validIDs {
		if id > 0 {
			validIDsList = append(validIDsList, id)
		}
	}
	if len(validIDsList) == 0 {
		return []map[string]interface{}{}, nil
	}

	var contacts []map[string]interface{}
	for _, id := range validIDsList {
		url := fmt.Sprintf("Contacts/%d", id)
		req, err := c.client.NewRequest(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		var response struct {
			Item map[string]interface{} `json:"item"`
		}

		if err := c.requestLimiter.ExecuteConcurrent(ctx, "Contacts", func() error {
			_, err := c.client.Do(req, &response)
			return err
		}); err != nil {
			if strings.Contains(err.Error(), "404") {
				continue
			}
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}

		contacts = append(contacts, response.Item)
	}

	return contacts, nil
}

// getContactsSequentially retrieves contacts by checking sequential IDs
func (c *Client) getContactsSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	// Try different ID ranges to find contacts
	var allContacts []map[string]interface{}
	batchSize := 100
	maxID := int64(1000000) // Adjust this based on your data

	for startID := int64(1); startID < maxID; startID += int64(batchSize - 1) {
		endID := startID + int64(batchSize-1)
		contacts, err := c.queryIDRange(ctx, "Contacts", startID, endID)
		if err != nil {
			continue
		}
		allContacts = append(allContacts, contacts...)
	}

	return allContacts, nil
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
	// Try to get tickets using the documentation format with date filter
	tickets, err := c.tryAutotaskDocumentationFormatWithDate(ctx, "Tickets", "lastActivityDate", since)
	if err == nil && len(tickets) > 0 {
		log.Info().
			Int("count", len(tickets)).
			Time("since", since).
			Msg("Retrieved tickets from Autotask using documentation format with lastActivityDate")
		return tickets, nil
	}

	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get tickets with documentation format and lastActivityDate, trying with LastActivityDate")

		// Try with a different field name (case sensitivity matters in some API versions)
		tickets, err = c.tryAutotaskDocumentationFormatWithDate(ctx, "Tickets", "LastActivityDate", since)
		if err == nil && len(tickets) > 0 {
			log.Info().
				Int("count", len(tickets)).
				Time("since", since).
				Msg("Retrieved tickets from Autotask using documentation format with LastActivityDate")
			return tickets, nil
		}

		log.Warn().
			Err(err).
			Msg("Failed to get tickets with documentation format, falling back to ID ranges approach")

		// Try the ID ranges approach
		return c.getTicketsSequentially(ctx, since)
	}

	return tickets, nil
}

// getTicketsSequentially retrieves tickets by checking sequential IDs
func (c *Client) getTicketsSequentially(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try different ID ranges to find tickets
	var allTickets []map[string]interface{}
	batchSize := 100
	maxID := int64(1000000) // Adjust this based on your data

	for startID := int64(1); startID < maxID; startID += int64(batchSize - 1) {
		endID := startID + int64(batchSize-1)
		tickets, err := c.queryIDRange(ctx, "Tickets", startID, endID)
		if err != nil {
			continue
		}

		// Filter tickets by the since date
		for _, ticket := range tickets {
			if lastActivityDate, ok := ticket["lastActivityDate"]; ok {
				if timeVal, err := parseTime(lastActivityDate); err == nil {
					if !timeVal.Before(since) {
						allTickets = append(allTickets, ticket)
					}
				}
			}
		}
	}

	return allTickets, nil
}

// parseTime attempts to parse a time value from an interface{}
func parseTime(v interface{}) (time.Time, error) {
	switch t := v.(type) {
	case string:
		return time.Parse(time.RFC3339, t)
	case time.Time:
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported time format: %T", v)
	}
}

// GetTicket retrieves a single ticket by ID
func (c *Client) GetTicket(ctx context.Context, id int64) (map[string]interface{}, error) {
	url := fmt.Sprintf("Tickets/%d", id)
	req, err := c.client.NewRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Item map[string]interface{} `json:"item"`
	}

	if err := c.requestLimiter.ExecuteConcurrent(ctx, "Tickets", func() error {
		_, err := c.client.Do(req, &response)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return response.Item, nil
}

// batchGetEntities retrieves entities in batches using their IDs
func (c *Client) batchGetEntities(ctx context.Context, entityName string, ids []int64, batchSize int) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		// If no IDs provided, use exist operator to get all entities
		reqBody := map[string]interface{}{
			"MaxRecords": 500,
			"filter": []map[string]interface{}{
				{
					"field": "id",
					"op":    "exist",
				},
			},
		}

		reqBytes, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}

		url := entityName + "/query"
		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		var response struct {
			Items []map[string]interface{} `json:"items"`
		}

		if err := c.requestLimiter.ExecuteConcurrent(ctx, entityName, func() error {
			_, err := c.client.Do(req, &response)
			return err
		}); err != nil {
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}

		return response.Items, nil
	}

	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	var allEntities []map[string]interface{}
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[i:end]

		// Create the filter for this batch using "in" operator
		reqBody := map[string]interface{}{
			"MaxRecords": 500,
			"filter": []map[string]interface{}{
				{
					"op":    "in",
					"field": "id",
					"value": batch,
				},
			},
		}

		reqBytes, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}

		url := entityName + "/query"
		req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		var response struct {
			Items []map[string]interface{} `json:"items"`
		}

		if err := c.requestLimiter.ExecuteConcurrent(ctx, entityName, func() error {
			_, err := c.client.Do(req, &response)
			return err
		}); err != nil {
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}

		allEntities = append(allEntities, response.Items...)
	}

	return allEntities, nil
}

// queryIDRange retrieves entities in a specific ID range
func (c *Client) queryIDRange(ctx context.Context, entityName string, startID, endID int64) ([]map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"MaxRecords": 500,
		"filter": []map[string]interface{}{
			{
				"op": "and",
				"items": []map[string]interface{}{
					{
						"field": "id",
						"op":    "gte",
						"value": startID,
					},
					{
						"field": "id",
						"op":    "lte",
						"value": endID,
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := entityName + "/query"
	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	if err := c.requestLimiter.ExecuteConcurrent(ctx, entityName, func() error {
		_, err := c.client.Do(req, &response)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return response.Items, nil
}

// getAllEntities retrieves all entities of a given type
func (c *Client) getAllEntities(ctx context.Context, endpoint string) ([]map[string]interface{}, error) {
	// First try using queryWithEmptyFilter
	entities, err := c.queryWithEmptyFilter(ctx, endpoint)
	if err == nil {
		return entities, nil
	}

	// If that fails, try using the proper filter format with an "exist" operator
	reqBody := map[string]interface{}{
		"MaxRecords": 500,
		"filter": []map[string]interface{}{
			{
				"field": "id",
				"op":    "exist",
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := endpoint + "/query"
	req, err := c.client.NewRequest(ctx, "POST", url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	var response struct {
		Items []map[string]interface{} `json:"items"`
	}

	if err := c.requestLimiter.ExecuteConcurrent(ctx, endpoint, func() error {
		_, err := c.client.Do(req, &response)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return response.Items, nil
}

// NewRequest creates a new HTTP request
func (c *Client) NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	return c.client.NewRequest(ctx, method, url, body)
}

// Do executes an HTTP request
func (c *Client) Do(req *http.Request, v interface{}) (*http.Response, error) {
	return c.client.Do(req, v)
}

// ExecuteConcurrent executes a function with concurrency control
func (c *Client) ExecuteConcurrent(ctx context.Context, entityName string, fn func() error) error {
	log.Debug().
		Str("entity", entityName).
		Msg("Acquiring concurrent request permit")

	if err := c.requestLimiter.Acquire(ctx, entityName); err != nil {
		log.Error().
			Str("entity", entityName).
			Err(err).
			Msg("Failed to acquire concurrent request permit")
		return fmt.Errorf("failed to acquire permit for %s: %w", entityName, err)
	}
	defer func() {
		c.requestLimiter.Release(entityName)
		log.Debug().
			Str("entity", entityName).
			Msg("Released concurrent request permit")
	}()

	if err := fn(); err != nil {
		log.Error().
			Str("entity", entityName).
			Err(err).
			Msg("Concurrent operation failed")
		return err
	}

	return nil
}

// GetConfigurationItems retrieves configuration items from Autotask
func (c *Client) GetConfigurationItems(ctx context.Context) ([]map[string]interface{}, error) {
	return c.queryWithEmptyFilter(ctx, "ConfigurationItems")
}

// GetContracts retrieves contracts from Autotask
func (c *Client) GetContracts(ctx context.Context) ([]map[string]interface{}, error) {
	return c.queryWithEmptyFilter(ctx, "Contracts")
}

// GetProjects retrieves projects from Autotask
func (c *Client) GetProjects(ctx context.Context) ([]map[string]interface{}, error) {
	return c.queryWithEmptyFilter(ctx, "Projects")
}

// GetResources retrieves resources from Autotask
func (c *Client) GetResources(ctx context.Context) ([]map[string]interface{}, error) {
	return c.queryWithEmptyFilter(ctx, "Resources")
}

// GetTasks retrieves tasks from Autotask
func (c *Client) GetTasks(ctx context.Context) ([]map[string]interface{}, error) {
	return c.queryWithEmptyFilter(ctx, "Tasks")
}

// GetTimeEntries retrieves time entries from Autotask since a given time
func (c *Client) GetTimeEntries(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	entries, err := c.queryWithDateFilter(ctx, "TimeEntries", "lastActivityDate", since)
	if err == nil {
		return entries, nil
	}
	return c.queryWithEmptyFilter(ctx, "TimeEntries")
}

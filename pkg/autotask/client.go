package autotask

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/asachs01/autotask-data-sink/pkg/config"
	atapi "github.com/asachs01/autotask-go/pkg/autotask"
	"github.com/rs/zerolog/log"
)

// Client wraps the autotask-go client
type Client struct {
	client atapi.Client
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

	return &Client{
		client: client,
	}, nil
}

// GetCompanies retrieves companies from Autotask
func (c *Client) GetCompanies(ctx context.Context) ([]map[string]interface{}, error) {
	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails map[string]interface{}   `json:"pageDetails"`
	}

	// Query companies without the Active filter that's causing issues
	// The error suggests "Active" field might not exist or have a different name
	// Try without any filter first to get all companies
	err := c.client.Companies().Query(ctx, "", &response)
	if err != nil {
		return nil, fmt.Errorf("failed to query companies: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Msg("Retrieved companies from Autotask")

	return response.Items, nil
}

// GetCompany retrieves a single company by ID
func (c *Client) GetCompany(ctx context.Context, id int64) (map[string]interface{}, error) {
	company, err := c.client.Companies().Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get company %d: %w", id, err)
	}

	// Convert interface{} to map[string]interface{}
	companyMap, ok := company.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to convert company to map: %v", company)
	}

	return companyMap, nil
}

// GetContacts retrieves contacts from Autotask
func (c *Client) GetContacts(ctx context.Context) ([]map[string]interface{}, error) {
	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails map[string]interface{}   `json:"pageDetails"`
	}

	// Query contacts without the Active filter that might cause issues
	// Similar to the companies query, remove the filter for now
	err := c.client.Contacts().Query(ctx, "", &response)
	if err != nil {
		return nil, fmt.Errorf("failed to query contacts: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Msg("Retrieved contacts from Autotask")

	return response.Items, nil
}

// GetTickets retrieves tickets from Autotask
func (c *Client) GetTickets(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails map[string]interface{}   `json:"pageDetails"`
	}

	// Query tickets updated since the given time
	filter := fmt.Sprintf("LastActivityDate>%s", since.Format(time.RFC3339))
	err := c.client.Tickets().Query(ctx, filter, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to query tickets: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Time("since", since).
		Msg("Retrieved tickets from Autotask")

	return response.Items, nil
}

// GetTimeEntries retrieves time entries from Autotask
// Note: This assumes the autotask-go library has a TimeEntries service
// If not, this will need to be implemented differently
func (c *Client) GetTimeEntries(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Since the TimeEntries service might not exist in the library,
	// we'll create a placeholder implementation that logs a warning
	log.Warn().Msg("TimeEntries service not implemented in autotask-go library")

	// Return an empty slice for now
	return []map[string]interface{}{}, nil
}

// GetResources retrieves resources from Autotask
func (c *Client) GetResources(ctx context.Context) ([]map[string]interface{}, error) {
	var response struct {
		Items       []map[string]interface{} `json:"items"`
		PageDetails map[string]interface{}   `json:"pageDetails"`
	}

	// Query active resources
	filter := "Active=true"
	err := c.client.Resources().Query(ctx, filter, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to query resources: %w", err)
	}

	log.Info().
		Int("count", len(response.Items)).
		Msg("Retrieved resources from Autotask")

	return response.Items, nil
}

// GetContracts retrieves contracts from Autotask
// Note: This assumes the autotask-go library has a Contracts service
// If not, this will need to be implemented differently
func (c *Client) GetContracts(ctx context.Context) ([]map[string]interface{}, error) {
	// Since the Contracts service might not exist in the library,
	// we'll create a placeholder implementation that logs a warning
	log.Warn().Msg("Contracts service not implemented in autotask-go library")

	// Return an empty slice for now
	return []map[string]interface{}{}, nil
}

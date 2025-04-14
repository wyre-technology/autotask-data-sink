package autotask

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	neturl "net/url"
)

const (
	// DefaultPageSize is the default number of items per page
	DefaultPageSize = 100
)

// EntityService represents a service that can interact with an Autotask entity
type EntityService interface {
	GetClient() *Client
	GetEntityName() string
}

// PageDetails contains pagination information
type PageDetails struct {
	Count       int    `json:"count"`
	PageNumber  int    `json:"pageNumber"`
	NextPageUrl string `json:"nextPageUrl"`
	PrevPageUrl string `json:"prevPageUrl"`
}

// EntityQueryParams represents query parameters for entity requests
type EntityQueryParams struct {
	Filter     interface{} `json:"filter,omitempty"`
	MaxRecords int         `json:"MaxRecords,omitempty"`
	Page       int         `json:"page,omitempty"`
}

// NewEntityQueryParams creates a new EntityQueryParams instance
func NewEntityQueryParams(filter interface{}) *EntityQueryParams {
	return &EntityQueryParams{
		Filter: filter,
	}
}

// WithMaxRecords sets the maximum number of records to return
func (p *EntityQueryParams) WithMaxRecords(maxRecords int) *EntityQueryParams {
	p.MaxRecords = maxRecords
	return p
}

// WithPage sets the page number
func (p *EntityQueryParams) WithPage(page int) *EntityQueryParams {
	p.Page = page
	return p
}

// ParseFilterString parses a filter string into a filter object
func ParseFilterString(filter string) interface{} {
	// If the filter is empty, return nil
	if filter == "" {
		return nil
	}

	// Try to parse the filter as JSON
	var filterObj interface{}
	if err := json.Unmarshal([]byte(filter), &filterObj); err == nil {
		return filterObj
	}

	// If parsing fails, return the filter string as is
	return filter
}

// PaginationIterator provides an iterator pattern for paginated results
type PaginationIterator struct {
	service      EntityService
	filter       string
	currentPage  int
	pageSize     int
	totalCount   int
	hasMore      bool
	nextPageUrl  string
	prevPageUrl  string
	items        []interface{}
	currentIndex int
	ctx          context.Context
}

// NewPaginationIterator creates a new pagination iterator
func NewPaginationIterator(ctx context.Context, service EntityService, filter string, pageSize int) (*PaginationIterator, error) {
	iterator := &PaginationIterator{
		service:      service,
		filter:       filter,
		currentPage:  1,
		pageSize:     pageSize,
		currentIndex: -1,
		ctx:          ctx,
	}

	// Load the first page
	err := iterator.loadNextPage()
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

// Next advances the iterator to the next item
// Returns false when there are no more items
func (p *PaginationIterator) Next() bool {
	p.currentIndex++

	// If we've reached the end of the current page and there are more pages
	if p.currentIndex >= len(p.items) && p.hasMore {
		err := p.loadNextPage()
		if err != nil {
			return false
		}
		p.currentIndex = 0
	}

	return p.currentIndex < len(p.items)
}

// Item returns the current item
func (p *PaginationIterator) Item() interface{} {
	if p.currentIndex < 0 || p.currentIndex >= len(p.items) {
		return nil
	}
	return p.items[p.currentIndex]
}

// Error returns any error that occurred during iteration
func (p *PaginationIterator) Error() error {
	return nil
}

// TotalCount returns the total number of items across all pages
func (p *PaginationIterator) TotalCount() int {
	return p.totalCount
}

// CurrentPage returns the current page number
func (p *PaginationIterator) CurrentPage() int {
	return p.currentPage
}

// loadNextPage loads the next page of results
func (p *PaginationIterator) loadNextPage() error {
	var urlPath string
	var req *http.Request
	var err error

	// Create a response structure
	var response struct {
		Items       []interface{} `json:"items"`
		PageDetails PageDetails   `json:"pageDetails"`
	}

	// If this is the first page, use the regular query endpoint
	if p.currentPage == 1 {
		// Parse the filter string
		var queryFilter interface{}
		if p.filter != "" {
			queryFilter = ParseFilterString(p.filter)
		}

		// Create query parameters
		params := NewEntityQueryParams(queryFilter).
			WithMaxRecords(p.pageSize)

		// Use the correct endpoint structure according to the API docs
		urlPath = p.service.GetEntityName() + "/query"

		// Convert params to JSON string for URL parameter
		searchJSON, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("failed to marshal search params: %w", err)
		}

		// Add search parameter to URL
		searchJSONStr := string(searchJSON)
		escapedJSON := neturl.QueryEscape(searchJSONStr)
		urlPath = fmt.Sprintf("%s?search=%s", urlPath, escapedJSON)

		req, err = p.service.GetClient().NewRequest(p.ctx, http.MethodGet, urlPath, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
	} else if p.nextPageUrl != "" {
		// For subsequent pages, use the nextPageUrl from the previous response
		// The nextPageUrl already contains all necessary parameters
		req, err = p.service.GetClient().NewRequest(p.ctx, http.MethodGet, p.nextPageUrl, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for next page: %w", err)
		}
	} else {
		// No more pages
		return nil
	}

	_, err = p.service.GetClient().Do(req, &response)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	// Ensure the page number is correctly set
	// The API might not always return the correct page number
	response.PageDetails.PageNumber = p.currentPage

	// Update the iterator state
	p.items = response.Items
	p.totalCount = response.PageDetails.Count
	p.nextPageUrl = response.PageDetails.NextPageUrl
	p.prevPageUrl = response.PageDetails.PrevPageUrl
	p.hasMore = response.PageDetails.NextPageUrl != "" && len(response.Items) > 0
	p.currentPage++

	return nil
}

// PaginatedResults is a generic structure for paginated results
type PaginatedResults[T any] struct {
	Items       []T         `json:"items"`
	PageDetails PageDetails `json:"pageDetails"`
}

// PaginationOptions contains options for pagination
type PaginationOptions struct {
	PageSize int
	Page     int
}

// DefaultPaginationOptions returns default pagination options
func DefaultPaginationOptions() PaginationOptions {
	return PaginationOptions{
		PageSize: DefaultPageSize,
		Page:     1,
	}
}

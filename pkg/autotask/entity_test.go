package autotask

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClient is a mock implementation of the entityClient interface
type MockClient struct {
	mock.Mock
}

func (m *MockClient) batchGetEntities(ctx context.Context, endpoint string, ids []int64, batchSize int) ([]map[string]interface{}, error) {
	args := m.Called(ctx, endpoint, ids, batchSize)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) getAllEntities(ctx context.Context, endpoint string) ([]map[string]interface{}, error) {
	args := m.Called(ctx, endpoint)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) queryWithDateFilter(ctx context.Context, endpoint string, fieldName string, since time.Time) ([]map[string]interface{}, error) {
	args := m.Called(ctx, endpoint, fieldName, since)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) queryWithEmptyFilter(ctx context.Context, endpoint string) ([]map[string]interface{}, error) {
	args := m.Called(ctx, endpoint)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) getCompaniesSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) getCompaniesIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetCompany(ctx context.Context, id int64) (map[string]interface{}, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetContact(ctx context.Context, id int64) (map[string]interface{}, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetTicket(ctx context.Context, id int64) (map[string]interface{}, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetTimeEntries(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	args := m.Called(ctx, since)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetResources(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetProjects(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetTasks(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetConfigurationItems(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) getContactsSequentially(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) getContactsIndividually(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetTickets(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	args := m.Called(ctx, since)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) GetContracts(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) NewRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	args := m.Called(ctx, method, url, body)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Request), args.Error(1)
}

func (m *MockClient) Do(req *http.Request, v interface{}) (*http.Response, error) {
	args := m.Called(req, v)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Response), args.Error(1)
}

func (m *MockClient) ExecuteConcurrent(ctx context.Context, entityName string, fn func() error) error {
	args := m.Called(ctx, entityName, fn)
	return args.Error(0)
}

func TestBaseEntityHandler_GetByID(t *testing.T) {
	tests := []struct {
		name           string
		entityType     EntityType
		endpoint       string
		id             int64
		mockResponse   []map[string]interface{}
		mockError      error
		expectedResult map[string]interface{}
		expectedError  error
	}{
		{
			name:       "successful retrieval",
			entityType: EntityTypeCompany,
			endpoint:   "Companies",
			id:         123,
			mockResponse: []map[string]interface{}{
				{"id": float64(123), "name": "Test Company"},
			},
			mockError:      nil,
			expectedResult: map[string]interface{}{"id": float64(123), "name": "Test Company"},
			expectedError:  nil,
		},
		{
			name:           "entity not found",
			entityType:     EntityTypeCompany,
			endpoint:       "Companies",
			id:             456,
			mockResponse:   []map[string]interface{}{},
			mockError:      nil,
			expectedResult: nil,
			expectedError:  fmt.Errorf("entity Company with ID 456 not found"),
		},
		{
			name:           "api error",
			entityType:     EntityTypeCompany,
			endpoint:       "Companies",
			id:             789,
			mockResponse:   nil,
			mockError:      fmt.Errorf("API error"),
			expectedResult: nil,
			expectedError:  fmt.Errorf("API error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockClient)
			handler := NewBaseEntityHandler(mockClient, tt.entityType, tt.endpoint)

			// Set up mock expectations
			mockClient.On("batchGetEntities", mock.Anything, tt.endpoint, []int64{tt.id}, 1).
				Return(tt.mockResponse, tt.mockError)

			// Execute the method
			result, err := handler.GetByID(context.Background(), tt.id)

			// Verify the results
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedResult, result)

			// Verify that the mock was called as expected
			mockClient.AssertExpectations(t)
		})
	}
}

func TestRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name: "rate limit error",
			err: &AutotaskError{
				StatusCode: 429,
				Message:    "Rate limit exceeded",
			},
			expected: true,
		},
		{
			name: "non-rate limit error",
			err: &AutotaskError{
				StatusCode: 400,
				Message:    "Bad request",
			},
			expected: false,
		},
		{
			name:     "generic error",
			err:      fmt.Errorf("generic error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetRetryDelay(t *testing.T) {
	tests := []struct {
		name          string
		attempt       int
		err           error
		expectedDelay time.Duration
	}{
		{
			name:          "first attempt",
			attempt:       0,
			err:           fmt.Errorf("generic error"),
			expectedDelay: time.Second,
		},
		{
			name:          "second attempt",
			attempt:       1,
			err:           fmt.Errorf("generic error"),
			expectedDelay: 2 * time.Second,
		},
		{
			name:    "rate limit error with RetryAfter",
			attempt: 0,
			err: &AutotaskError{
				StatusCode: 429,
				Message:    "Rate limit exceeded",
				RetryAfter: 30 * time.Second,
			},
			expectedDelay: 30 * time.Second,
		},
		{
			name:          "max delay reached",
			attempt:       10, // This would normally result in a very large delay
			err:           fmt.Errorf("generic error"),
			expectedDelay: 5 * time.Minute, // Should be capped at maxDelay
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := GetRetryDelay(tt.attempt, tt.err)
			assert.Equal(t, tt.expectedDelay, delay)
		})
	}
}

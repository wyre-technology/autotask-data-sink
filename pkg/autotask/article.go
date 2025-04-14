package autotask

import (
	"context"
	"time"
)

// ArticleHandler handles Article entity operations
type ArticleHandler struct {
	*BaseEntityHandler
}

// NewArticleHandler creates a new ArticleHandler
func NewArticleHandler(client *Client) *ArticleHandler {
	base := NewBaseEntityHandler(client, EntityTypeArticle, "Articles")
	return &ArticleHandler{base}
}

// Get retrieves all articles
func (h *ArticleHandler) Get(ctx context.Context) ([]map[string]interface{}, error) {
	// Use the standard approach
	articles, err := h.BaseEntityHandler.Get(ctx)
	if err == nil {
		return articles, nil
	}

	// If that fails, try with empty filter
	return h.client.queryWithEmptyFilter(ctx, h.entityEndpoint)
}

// GetByID retrieves a single article by ID
func (h *ArticleHandler) GetByID(ctx context.Context, id int64) (map[string]interface{}, error) {
	// Use the standard approach
	articles, err := h.client.batchGetEntities(ctx, h.entityEndpoint, []int64{id}, 1)
	if err == nil && len(articles) > 0 {
		return articles[0], nil
	}

	return nil, err
}

// GetSince retrieves all articles modified since the given time
func (h *ArticleHandler) GetSince(ctx context.Context, since time.Time) ([]map[string]interface{}, error) {
	// Try using the standard lastModifiedDateTime field
	articles, err := h.BaseEntityHandler.GetSince(ctx, since)
	if err == nil {
		return articles, nil
	}

	// If that fails, try with publishDate field
	articles, err = h.client.queryWithDateFilter(ctx, h.entityEndpoint, "publishDate", since)
	if err == nil {
		return articles, nil
	}

	// If that also fails, get all articles and filter client-side
	articles, err = h.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Filter articles based on publish date and last modified time
	var filtered []map[string]interface{}
	for _, article := range articles {
		// Check both publishDate and lastActivityDate
		var include bool

		if publishDate, ok := article["publishDate"]; ok {
			if timeVal, err := parseTime(publishDate); err == nil {
				if !timeVal.Before(since) {
					include = true
				}
			}
		}

		if !include {
			if lastModified, ok := article["lastActivityDate"]; ok {
				if timeVal, err := parseTime(lastModified); err == nil {
					if !timeVal.Before(since) {
						include = true
					}
				}
			}
		}

		if include {
			filtered = append(filtered, article)
		}
	}

	return filtered, nil
}

// Validate validates an article entity
func (h *ArticleHandler) Validate(article map[string]interface{}) error {
	// Required fields
	requiredFields := []string{"id", "title", "content"}
	for _, field := range requiredFields {
		if _, ok := article[field]; !ok {
			return &ValidationError{
				EntityType: EntityTypeArticle,
				Field:      field,
				Message:    "required field is missing",
			}
		}
	}

	// Type validations
	if _, ok := article["id"].(float64); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticle,
			Field:      "id",
			Message:    "id must be a number",
		}
	}

	if _, ok := article["title"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticle,
			Field:      "title",
			Message:    "title must be a string",
		}
	}

	if _, ok := article["content"].(string); !ok {
		return &ValidationError{
			EntityType: EntityTypeArticle,
			Field:      "content",
			Message:    "content must be a string",
		}
	}

	// Validate publish date if present
	if publishDateStr, ok := article["publishDate"].(string); ok {
		if _, err := parseTime(publishDateStr); err != nil {
			return &ValidationError{
				EntityType: EntityTypeArticle,
				Field:      "publishDate",
				Message:    "publishDate must be a valid date/time",
			}
		}
	}

	// Validate published status if present
	if published, ok := article["published"]; ok {
		if _, ok := published.(bool); !ok {
			return &ValidationError{
				EntityType: EntityTypeArticle,
				Field:      "published",
				Message:    "published must be a boolean",
			}
		}
	}

	return nil
}

package sync

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/asachs01/autotask-data-sink/pkg/autotask"
	"github.com/asachs01/autotask-data-sink/pkg/config"
	"github.com/asachs01/autotask-data-sink/pkg/database"
	"github.com/asachs01/autotask-data-sink/pkg/models"
	"github.com/rs/zerolog/log"
)

// Service handles synchronization between Autotask API and the database
type Service struct {
	db     *database.DB
	client *autotask.Client
	config *config.Config
}

// New creates a new sync service
func New(db *database.DB, client *autotask.Client, config *config.Config) *Service {
	return &Service{
		db:     db,
		client: client,
		config: config,
	}
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries  int
	InitialWait time.Duration
	MaxWait     time.Duration
}

// DefaultRetryConfig returns the default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  3,
		InitialWait: time.Second,
		MaxWait:     time.Minute * 5,
	}
}

// withRetry executes the given function with retries
func (s *Service) withRetry(ctx context.Context, operation string, fn func() error) error {
	config := DefaultRetryConfig()
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if err := fn(); err != nil {
			lastErr = err

			// Check if we should retry
			if !autotask.RetryableError(err) || attempt == config.MaxRetries {
				return fmt.Errorf("%s failed after %d attempts: %w", operation, attempt+1, err)
			}

			// Calculate wait time using exponential backoff
			wait := config.InitialWait * time.Duration(1<<uint(attempt))
			if wait > config.MaxWait {
				wait = config.MaxWait
			}

			log.Warn().
				Err(err).
				Int("attempt", attempt+1).
				Int("max_retries", config.MaxRetries).
				Dur("wait", wait).
				Str("operation", operation).
				Msg("Operation failed, retrying")

			// Wait before retrying
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				continue
			}
		}

		return nil
	}

	return lastErr
}

// processBatch processes a batch of records within a transaction
func (s *Service) processBatch(ctx context.Context, fn func(context.Context, *database.Tx) error) error {
	return s.db.WithTransaction(ctx, func(tx *database.Tx) error {
		return fn(ctx, tx)
	})
}

// SyncCompanies synchronizes companies from Autotask to the database
func (s *Service) SyncCompanies(ctx context.Context) error {
	log.Info().Msg("Starting company sync")

	return s.withRetry(ctx, "company sync", func() error {
		// Get companies from Autotask
		companies, err := s.client.GetCompanies(ctx)
		if err != nil {
			return fmt.Errorf("failed to get companies: %w", err)
		}

		// Process all companies in memory first
		newCompanies := make([]models.Company, 0, len(companies))
		updateCompanies := make([]models.Company, 0, len(companies))

		// Get all existing company IDs
		existingCompanyIDs := make(map[int64]bool)
		rows, err := s.db.QueryxContext(ctx, "SELECT id FROM autotask.companies")
		if err != nil {
			return fmt.Errorf("failed to query existing companies: %w", err)
		}
		defer rows.Close()

		var companyID int64
		for rows.Next() {
			if err := rows.Scan(&companyID); err != nil {
				return fmt.Errorf("failed to scan company ID: %w", err)
			}
			existingCompanyIDs[companyID] = true
		}

		// Process each company in memory
		for _, companyData := range companies {
			// Extract company fields
			model := models.Company{
				ID:     getInt64Value(companyData, "id"),
				Name:   getStringValue(companyData, "companyName"),
				SyncAt: time.Now(),
			}

			// Set optional fields
			model.AddressLine1 = sql.NullString{
				String: getStringValue(companyData, "address1"),
				Valid:  hasValue(companyData, "address1"),
			}
			model.AddressLine2 = sql.NullString{
				String: getStringValue(companyData, "address2"),
				Valid:  hasValue(companyData, "address2"),
			}
			model.City = sql.NullString{
				String: getStringValue(companyData, "city"),
				Valid:  hasValue(companyData, "city"),
			}
			model.State = sql.NullString{
				String: getStringValue(companyData, "state"),
				Valid:  hasValue(companyData, "state"),
			}
			model.PostalCode = sql.NullString{
				String: getStringValue(companyData, "postalCode"),
				Valid:  hasValue(companyData, "postalCode"),
			}
			model.Country = sql.NullString{
				String: getStringValue(companyData, "country"),
				Valid:  hasValue(companyData, "country"),
			}
			model.Phone = sql.NullString{
				String: getStringValue(companyData, "phone"),
				Valid:  hasValue(companyData, "phone"),
			}
			model.Active = sql.NullBool{
				Bool:  getBoolValue(companyData, "active"),
				Valid: hasValue(companyData, "active"),
			}

			if existingCompanyIDs[model.ID] {
				updateCompanies = append(updateCompanies, model)
			} else {
				newCompanies = append(newCompanies, model)
			}
		}

		// Process new companies in batches
		if len(newCompanies) > 0 {
			for i := 0; i < len(newCompanies); i += s.config.Sync.BatchSize {
				end := i + s.config.Sync.BatchSize
				if end > len(newCompanies) {
					end = len(newCompanies)
				}
				batch := newCompanies[i:end]

				err := s.processBatch(ctx, func(ctx context.Context, tx *database.Tx) error {
					query := `
						INSERT INTO autotask.companies (
							id, name, address_line1, address_line2, city, state,
							postal_code, country, phone, active, sync_at
						) VALUES (
							:id, :name, :address_line1, :address_line2, :city, :state,
							:postal_code, :country, :phone, :active, :sync_at
						)`

					if _, err := tx.NamedExecContext(ctx, query, batch); err != nil {
						return fmt.Errorf("failed to insert companies: %w", err)
					}

					return nil
				})
				if err != nil {
					return fmt.Errorf("failed to process new companies batch %d-%d: %w", i, end, err)
				}

				log.Debug().
					Int("start", i).
					Int("end", end).
					Int("total", len(newCompanies)).
					Msg("Processed new companies batch")
			}

			log.Info().
				Int("count", len(newCompanies)).
				Msg("Inserted new companies")
		}

		// Process updated companies in batches
		if len(updateCompanies) > 0 {
			for i := 0; i < len(updateCompanies); i += s.config.Sync.BatchSize {
				end := i + s.config.Sync.BatchSize
				if end > len(updateCompanies) {
					end = len(updateCompanies)
				}
				batch := updateCompanies[i:end]

				err := s.processBatch(ctx, func(ctx context.Context, tx *database.Tx) error {
					query := `
						UPDATE autotask.companies SET
							name = :name,
							address_line1 = :address_line1,
							address_line2 = :address_line2,
							city = :city,
							state = :state,
							postal_code = :postal_code,
							country = :country,
							phone = :phone,
							active = :active,
							sync_at = :sync_at,
							updated_at = NOW()
						WHERE id = :id`

					if _, err := tx.NamedExecContext(ctx, query, batch); err != nil {
						return fmt.Errorf("failed to update companies: %w", err)
					}

					return nil
				})
				if err != nil {
					return fmt.Errorf("failed to process updated companies batch %d-%d: %w", i, end, err)
				}

				log.Debug().
					Int("start", i).
					Int("end", end).
					Int("total", len(updateCompanies)).
					Msg("Processed updated companies batch")
			}

			log.Info().
				Int("count", len(updateCompanies)).
				Msg("Updated existing companies")
		}

		return nil
	})
}

// SyncContacts synchronizes contacts from Autotask to the database
func (s *Service) SyncContacts(ctx context.Context) error {
	log.Info().Msg("Starting contact sync")

	// Get contacts from Autotask
	contacts, err := s.client.GetContacts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contacts: %w", err)
	}

	// Process all contacts in memory first
	newContacts := make([]models.Contact, 0, len(contacts))
	updateContacts := make([]models.Contact, 0, len(contacts))

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, get all existing contact IDs to determine which ones to insert vs update
	existingContactIDs := make(map[int64]bool)
	rows, err := tx.QueryxContext(ctx, "SELECT id FROM autotask.contacts")
	if err != nil {
		return fmt.Errorf("failed to query existing contacts: %w", err)
	}
	defer rows.Close()

	var contactID int64
	for rows.Next() {
		if err := rows.Scan(&contactID); err != nil {
			return fmt.Errorf("failed to scan contact ID: %w", err)
		}
		existingContactIDs[contactID] = true
	}

	// Process each contact in memory
	for _, contactData := range contacts {
		// Extract contact ID
		idVal, ok := contactData["id"]
		if !ok {
			log.Warn().Interface("contact", contactData).Msg("Contact data missing ID field, skipping")
			continue
		}

		// Convert ID to int64
		var id int64
		switch v := idVal.(type) {
		case float64:
			id = int64(v)
		case int64:
			id = v
		case int:
			id = int64(v)
		default:
			log.Warn().
				Interface("contact", contactData).
				Str("idType", fmt.Sprintf("%T", idVal)).
				Msg("Unexpected ID type, skipping")
			continue
		}

		// Extract contact fields
		contact := models.Contact{
			ID:     id,
			SyncAt: time.Now(),
		}

		// Set optional fields
		if val, ok := contactData["firstName"]; ok {
			contact.FirstName.String = fmt.Sprintf("%v", val)
			contact.FirstName.Valid = true
		}
		if val, ok := contactData["lastName"]; ok {
			contact.LastName.String = fmt.Sprintf("%v", val)
			contact.LastName.Valid = true
		}
		if val, ok := contactData["emailAddress"]; ok {
			contact.Email.String = fmt.Sprintf("%v", val)
			contact.Email.Valid = true
		}
		if val, ok := contactData["phoneNumber"]; ok {
			contact.Phone.String = fmt.Sprintf("%v", val)
			contact.Phone.Valid = true
		}
		if val, ok := contactData["mobilePhone"]; ok {
			contact.Mobile.String = fmt.Sprintf("%v", val)
			contact.Mobile.Valid = true
		}
		if val, ok := contactData["active"]; ok {
			if boolVal, ok := val.(bool); ok {
				contact.Active.Bool = boolVal
				contact.Active.Valid = true
			}
		}
		if val, ok := contactData["companyID"]; ok {
			if numVal, ok := val.(float64); ok {
				contact.CompanyID.Int64 = int64(numVal)
				contact.CompanyID.Valid = true
			}
		}
		if val, ok := contactData["createDate"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				contact.CreatedAt.Time = timeVal
				contact.CreatedAt.Valid = true
			}
		}
		if val, ok := contactData["lastActivityDate"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				contact.UpdatedAt.Time = timeVal
				contact.UpdatedAt.Valid = true
			}
		}

		// Add to appropriate slice based on whether it exists
		if existingContactIDs[id] {
			updateContacts = append(updateContacts, contact)
		} else {
			newContacts = append(newContacts, contact)
		}
	}

	// Batch insert new contacts
	if len(newContacts) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			INSERT INTO autotask.contacts (
				id, company_id, first_name, last_name, email, phone, mobile,
				active, created_at, updated_at, sync_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer stmt.Close()

		for _, contact := range newContacts {
			_, err = stmt.ExecContext(ctx,
				contact.ID,
				contact.CompanyID,
				contact.FirstName,
				contact.LastName,
				contact.Email,
				contact.Phone,
				contact.Mobile,
				contact.Active,
				contact.CreatedAt,
				contact.UpdatedAt,
				contact.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to insert contact %d: %w", contact.ID, err)
			}
		}

		log.Info().Int("count", len(newContacts)).Msg("Inserted new contacts")
	}

	// Batch update existing contacts
	if len(updateContacts) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			UPDATE autotask.contacts SET
				company_id = $2,
				first_name = $3,
				last_name = $4,
				email = $5,
				phone = $6,
				mobile = $7,
				active = $8,
				created_at = $9,
				updated_at = $10,
				sync_at = $11
			WHERE id = $1
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
		defer stmt.Close()

		for _, contact := range updateContacts {
			_, err = stmt.ExecContext(ctx,
				contact.ID,
				contact.CompanyID,
				contact.FirstName,
				contact.LastName,
				contact.Email,
				contact.Phone,
				contact.Mobile,
				contact.Active,
				contact.CreatedAt,
				contact.UpdatedAt,
				contact.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to update contact %d: %w", contact.ID, err)
			}
		}

		log.Info().Int("count", len(updateContacts)).Msg("Updated existing contacts")
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info().
		Int("count", len(contacts)).
		Msg("Contact sync completed")

	return nil
}

// SyncTickets synchronizes tickets from Autotask to the database
func (s *Service) SyncTickets(ctx context.Context) error {
	log.Info().Msg("Starting ticket sync")

	// Get last sync time for tickets
	var lastSync time.Time
	err := s.db.GetContext(ctx, &lastSync, `
		SELECT COALESCE(MAX(sync_at), '2000-01-01T00:00:00Z')
		FROM autotask.tickets
	`)
	if err != nil {
		return fmt.Errorf("failed to get last sync time: %w", err)
	}

	// Get tickets from Autotask
	tickets, err := s.client.GetTickets(ctx, lastSync)
	if err != nil {
		return fmt.Errorf("failed to get tickets: %w", err)
	}

	// Process all tickets in memory first
	newTickets := make([]models.Ticket, 0, len(tickets))
	updateTickets := make([]models.Ticket, 0, len(tickets))

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, get all existing ticket IDs to determine which ones to insert vs update
	existingTicketIDs := make(map[int64]bool)
	rows, err := tx.QueryxContext(ctx, "SELECT id FROM autotask.tickets")
	if err != nil {
		return fmt.Errorf("failed to query existing tickets: %w", err)
	}
	defer rows.Close()

	var ticketID int64
	for rows.Next() {
		if err := rows.Scan(&ticketID); err != nil {
			return fmt.Errorf("failed to scan ticket ID: %w", err)
		}
		existingTicketIDs[ticketID] = true
	}

	// Process each ticket in memory
	for _, ticketData := range tickets {
		// Extract ticket ID
		idVal, ok := ticketData["id"]
		if !ok {
			log.Warn().Interface("ticket", ticketData).Msg("Ticket data missing ID field, skipping")
			continue
		}

		// Convert ID to int64
		var id int64
		switch v := idVal.(type) {
		case float64:
			id = int64(v)
		case int64:
			id = v
		case int:
			id = int64(v)
		default:
			log.Warn().
				Interface("ticket", ticketData).
				Str("idType", fmt.Sprintf("%T", idVal)).
				Msg("Unexpected ID type, skipping")
			continue
		}

		// Extract ticket fields
		ticket := models.Ticket{
			ID:     id,
			Title:  getStringValue(ticketData, "title"),
			SyncAt: time.Now(),
		}

		// Set optional fields
		if val, ok := ticketData["description"]; ok {
			ticket.Description.String = fmt.Sprintf("%v", val)
			ticket.Description.Valid = true
		}

		// Add more fields as needed...
		// This is the same field extraction logic as in the original processTicket method
		if val, ok := ticketData["status"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.Status.Int64 = int64(numVal)
				ticket.Status.Valid = true
			}
		}
		if val, ok := ticketData["priority"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.Priority.Int64 = int64(numVal)
				ticket.Priority.Valid = true
			}
		}
		if val, ok := ticketData["queueID"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.QueueID.Int64 = int64(numVal)
				ticket.QueueID.Valid = true
			}
		}
		if val, ok := ticketData["companyID"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.CompanyID.Int64 = int64(numVal)
				ticket.CompanyID.Valid = true
			}
		}
		if val, ok := ticketData["contactID"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.ContactID.Int64 = int64(numVal)
				ticket.ContactID.Valid = true
			}
		}
		if val, ok := ticketData["assignedResourceID"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.AssignedResourceID.Int64 = int64(numVal)
				ticket.AssignedResourceID.Valid = true
			}
		}
		if val, ok := ticketData["createdByResourceID"]; ok {
			if numVal, ok := val.(float64); ok {
				ticket.CreatedByResourceID.Int64 = int64(numVal)
				ticket.CreatedByResourceID.Valid = true
			}
		}
		if val, ok := ticketData["createDate"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				ticket.CreatedAt.Time = timeVal
				ticket.CreatedAt.Valid = true
			}
		}
		if val, ok := ticketData["lastActivityDate"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				ticket.UpdatedAt.Time = timeVal
				ticket.UpdatedAt.Valid = true
			}
		}
		if val, ok := ticketData["completedDate"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				ticket.CompletedDate.Time = timeVal
				ticket.CompletedDate.Valid = true
			}
		}
		if val, ok := ticketData["dueDateTime"]; ok {
			if timeVal, err := parseTime(val); err == nil {
				ticket.DueDate.Time = timeVal
				ticket.DueDate.Valid = true
			}
		}

		// Add to appropriate slice based on whether it exists
		if existingTicketIDs[id] {
			updateTickets = append(updateTickets, ticket)
		} else {
			newTickets = append(newTickets, ticket)
		}
	}

	// Batch insert new tickets
	if len(newTickets) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			INSERT INTO autotask.tickets (
				id, title, description, status, priority, queue_id, company_id,
				contact_id, assigned_resource_id, created_by_resource_id, created_at,
				updated_at, completed_date, due_date, sync_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11,
				$12, $13, $14, $15
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer stmt.Close()

		for _, ticket := range newTickets {
			_, err = stmt.ExecContext(ctx,
				ticket.ID,
				ticket.Title,
				ticket.Description,
				ticket.Status,
				ticket.Priority,
				ticket.QueueID,
				ticket.CompanyID,
				ticket.ContactID,
				ticket.AssignedResourceID,
				ticket.CreatedByResourceID,
				ticket.CreatedAt,
				ticket.UpdatedAt,
				ticket.CompletedDate,
				ticket.DueDate,
				ticket.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to insert ticket %d: %w", ticket.ID, err)
			}
		}

		log.Info().Int("count", len(newTickets)).Msg("Inserted new tickets")
	}

	// Batch update existing tickets
	if len(updateTickets) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			UPDATE autotask.tickets SET
				title = $2,
				description = $3,
				status = $4,
				priority = $5,
				queue_id = $6,
				company_id = $7,
				contact_id = $8,
				assigned_resource_id = $9,
				created_by_resource_id = $10,
				created_at = $11,
				updated_at = $12,
				completed_date = $13,
				due_date = $14,
				sync_at = $15
			WHERE id = $1
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
		defer stmt.Close()

		for _, ticket := range updateTickets {
			_, err = stmt.ExecContext(ctx,
				ticket.ID,
				ticket.Title,
				ticket.Description,
				ticket.Status,
				ticket.Priority,
				ticket.QueueID,
				ticket.CompanyID,
				ticket.ContactID,
				ticket.AssignedResourceID,
				ticket.CreatedByResourceID,
				ticket.CreatedAt,
				ticket.UpdatedAt,
				ticket.CompletedDate,
				ticket.DueDate,
				ticket.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to update ticket %d: %w", ticket.ID, err)
			}
		}

		log.Info().Int("count", len(updateTickets)).Msg("Updated existing tickets")
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info().
		Int("count", len(tickets)).
		Time("since", lastSync).
		Msg("Ticket sync completed")

	return nil
}

// Helper functions

// getStringValue safely extracts a string value from a map
func getStringValue(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

// getInt64Value safely extracts an int64 value from a map
func getInt64Value(data map[string]interface{}, key string) int64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 0
}

// getBoolValue safely extracts a bool value from a map
func getBoolValue(data map[string]interface{}, key string) bool {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return v == "true" || v == "1" || v == "yes"
		case float64:
			return v != 0
		case int:
			return v != 0
		}
	}
	return false
}

// hasValue checks if a key exists in a map
func hasValue(data map[string]interface{}, key string) bool {
	_, ok := data[key]
	return ok
}

// parseTime attempts to parse a time value from various formats
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

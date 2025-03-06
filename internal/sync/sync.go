package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/asachs01/autotask-data-sink/pkg/autotask"
	"github.com/asachs01/autotask-data-sink/pkg/database"
	"github.com/asachs01/autotask-data-sink/pkg/models"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

// Service handles synchronization between Autotask API and the database
type Service struct {
	db     *database.DB
	client *autotask.Client
}

// New creates a new sync service
func New(db *database.DB, client *autotask.Client) *Service {
	return &Service{
		db:     db,
		client: client,
	}
}

// SyncCompanies synchronizes companies from Autotask to the database
func (s *Service) SyncCompanies(ctx context.Context) error {
	log.Info().Msg("Starting company sync")

	// Get companies from Autotask
	companies, err := s.client.GetCompanies(ctx)
	if err != nil {
		return fmt.Errorf("failed to get companies: %w", err)
	}

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Process each company
	for _, companyData := range companies {
		if err := s.processCompany(ctx, tx, companyData); err != nil {
			return fmt.Errorf("failed to process company: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Info().
		Int("count", len(companies)).
		Msg("Company sync completed")

	return nil
}

// processCompany processes a single company
func (s *Service) processCompany(ctx context.Context, tx *sqlx.Tx, companyData map[string]interface{}) error {
	// Extract company ID
	idVal, ok := companyData["id"]
	if !ok {
		return fmt.Errorf("company data missing ID field")
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
		return fmt.Errorf("unexpected ID type: %T", idVal)
	}

	// Check if company exists
	var exists bool
	err := tx.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM autotask.companies WHERE id = $1)", id)
	if err != nil {
		return fmt.Errorf("failed to check if company exists: %w", err)
	}

	// Extract company fields
	company := models.Company{
		ID:     id,
		Name:   getStringValue(companyData, "name"),
		SyncAt: time.Now(),
	}

	// Set optional fields
	if val, ok := companyData["addressLine1"]; ok {
		company.AddressLine1.String = fmt.Sprintf("%v", val)
		company.AddressLine1.Valid = true
	}
	if val, ok := companyData["addressLine2"]; ok {
		company.AddressLine2.String = fmt.Sprintf("%v", val)
		company.AddressLine2.Valid = true
	}
	if val, ok := companyData["city"]; ok {
		company.City.String = fmt.Sprintf("%v", val)
		company.City.Valid = true
	}
	if val, ok := companyData["state"]; ok {
		company.State.String = fmt.Sprintf("%v", val)
		company.State.Valid = true
	}
	if val, ok := companyData["postalCode"]; ok {
		company.PostalCode.String = fmt.Sprintf("%v", val)
		company.PostalCode.Valid = true
	}
	if val, ok := companyData["country"]; ok {
		company.Country.String = fmt.Sprintf("%v", val)
		company.Country.Valid = true
	}
	if val, ok := companyData["phone"]; ok {
		company.Phone.String = fmt.Sprintf("%v", val)
		company.Phone.Valid = true
	}
	if val, ok := companyData["active"]; ok {
		if boolVal, ok := val.(bool); ok {
			company.Active.Bool = boolVal
			company.Active.Valid = true
		}
	}
	if val, ok := companyData["createDate"]; ok {
		if timeVal, err := parseTime(val); err == nil {
			company.CreatedAt.Time = timeVal
			company.CreatedAt.Valid = true
		}
	}
	if val, ok := companyData["lastActivityDate"]; ok {
		if timeVal, err := parseTime(val); err == nil {
			company.UpdatedAt.Time = timeVal
			company.UpdatedAt.Valid = true
		}
	}

	if exists {
		// Update existing company
		_, err = tx.NamedExecContext(ctx, `
			UPDATE autotask.companies
			SET name = :name,
				address_line1 = :address_line1,
				address_line2 = :address_line2,
				city = :city,
				state = :state,
				postal_code = :postal_code,
				country = :country,
				phone = :phone,
				active = :active,
				created_at = :created_at,
				updated_at = :updated_at,
				sync_at = :sync_at
			WHERE id = :id
		`, company)
		if err != nil {
			return fmt.Errorf("failed to update company: %w", err)
		}

		log.Debug().
			Int64("id", company.ID).
			Str("name", company.Name).
			Msg("Updated company")
	} else {
		// Insert new company
		_, err = tx.NamedExecContext(ctx, `
			INSERT INTO autotask.companies (
				id, name, address_line1, address_line2, city, state, postal_code,
				country, phone, active, created_at, updated_at, sync_at
			) VALUES (
				:id, :name, :address_line1, :address_line2, :city, :state, :postal_code,
				:country, :phone, :active, :created_at, :updated_at, :sync_at
			)
		`, company)
		if err != nil {
			return fmt.Errorf("failed to insert company: %w", err)
		}

		log.Debug().
			Int64("id", company.ID).
			Str("name", company.Name).
			Msg("Inserted new company")
	}

	return nil
}

// SyncContacts synchronizes contacts from Autotask to the database
func (s *Service) SyncContacts(ctx context.Context) error {
	log.Info().Msg("Starting contact sync")

	// Get contacts from Autotask
	contacts, err := s.client.GetContacts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contacts: %w", err)
	}

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Process each contact
	for _, contactData := range contacts {
		if err := s.processContact(ctx, tx, contactData); err != nil {
			return fmt.Errorf("failed to process contact: %w", err)
		}
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

// processContact processes a single contact
func (s *Service) processContact(ctx context.Context, tx *sqlx.Tx, contactData map[string]interface{}) error {
	// Extract contact ID
	idVal, ok := contactData["id"]
	if !ok {
		return fmt.Errorf("contact data missing ID field")
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
		return fmt.Errorf("unexpected ID type: %T", idVal)
	}

	// Check if contact exists
	var exists bool
	err := tx.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM autotask.contacts WHERE id = $1)", id)
	if err != nil {
		return fmt.Errorf("failed to check if contact exists: %w", err)
	}

	// Extract contact fields
	contact := models.Contact{
		ID:     id,
		SyncAt: time.Now(),
	}

	// Set optional fields
	if val, ok := contactData["companyID"]; ok {
		if numVal, ok := val.(float64); ok {
			contact.CompanyID.Int64 = int64(numVal)
			contact.CompanyID.Valid = true
		}
	}
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
	if val, ok := contactData["phone"]; ok {
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

	if exists {
		// Update existing contact
		_, err = tx.NamedExecContext(ctx, `
			UPDATE autotask.contacts
			SET company_id = :company_id,
				first_name = :first_name,
				last_name = :last_name,
				email = :email,
				phone = :phone,
				mobile = :mobile,
				active = :active,
				created_at = :created_at,
				updated_at = :updated_at,
				sync_at = :sync_at
			WHERE id = :id
		`, contact)
		if err != nil {
			return fmt.Errorf("failed to update contact: %w", err)
		}

		log.Debug().
			Int64("id", contact.ID).
			Str("firstName", contact.FirstName.String).
			Str("lastName", contact.LastName.String).
			Msg("Updated contact")
	} else {
		// Insert new contact
		_, err = tx.NamedExecContext(ctx, `
			INSERT INTO autotask.contacts (
				id, company_id, first_name, last_name, email, phone, mobile,
				active, created_at, updated_at, sync_at
			) VALUES (
				:id, :company_id, :first_name, :last_name, :email, :phone, :mobile,
				:active, :created_at, :updated_at, :sync_at
			)
		`, contact)
		if err != nil {
			return fmt.Errorf("failed to insert contact: %w", err)
		}

		log.Debug().
			Int64("id", contact.ID).
			Str("firstName", contact.FirstName.String).
			Str("lastName", contact.LastName.String).
			Msg("Inserted new contact")
	}

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

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Process each ticket
	for _, ticketData := range tickets {
		if err := s.processTicket(ctx, tx, ticketData); err != nil {
			return fmt.Errorf("failed to process ticket: %w", err)
		}
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

// processTicket processes a single ticket
func (s *Service) processTicket(ctx context.Context, tx *sqlx.Tx, ticketData map[string]interface{}) error {
	// Extract ticket ID
	idVal, ok := ticketData["id"]
	if !ok {
		return fmt.Errorf("ticket data missing ID field")
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
		return fmt.Errorf("unexpected ID type: %T", idVal)
	}

	// Check if ticket exists
	var exists bool
	err := tx.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM autotask.tickets WHERE id = $1)", id)
	if err != nil {
		return fmt.Errorf("failed to check if ticket exists: %w", err)
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

	if exists {
		// Update existing ticket
		_, err = tx.NamedExecContext(ctx, `
			UPDATE autotask.tickets
			SET title = :title,
				description = :description,
				status = :status,
				priority = :priority,
				queue_id = :queue_id,
				company_id = :company_id,
				contact_id = :contact_id,
				assigned_resource_id = :assigned_resource_id,
				created_by_resource_id = :created_by_resource_id,
				created_at = :created_at,
				updated_at = :updated_at,
				completed_date = :completed_date,
				due_date = :due_date,
				sync_at = :sync_at
			WHERE id = :id
		`, ticket)
		if err != nil {
			return fmt.Errorf("failed to update ticket: %w", err)
		}

		log.Debug().
			Int64("id", ticket.ID).
			Str("title", ticket.Title).
			Msg("Updated ticket")
	} else {
		// Insert new ticket
		_, err = tx.NamedExecContext(ctx, `
			INSERT INTO autotask.tickets (
				id, title, description, status, priority, queue_id, company_id,
				contact_id, assigned_resource_id, created_by_resource_id, created_at,
				updated_at, completed_date, due_date, sync_at
			) VALUES (
				:id, :title, :description, :status, :priority, :queue_id, :company_id,
				:contact_id, :assigned_resource_id, :created_by_resource_id, :created_at,
				:updated_at, :completed_date, :due_date, :sync_at
			)
		`, ticket)
		if err != nil {
			return fmt.Errorf("failed to insert ticket: %w", err)
		}

		log.Debug().
			Int64("id", ticket.ID).
			Str("title", ticket.Title).
			Msg("Inserted new ticket")
	}

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

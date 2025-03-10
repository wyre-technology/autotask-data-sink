package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/asachs01/autotask-data-sink/pkg/autotask"
	"github.com/asachs01/autotask-data-sink/pkg/database"
	"github.com/asachs01/autotask-data-sink/pkg/models"
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

	// Process all companies in memory first
	newCompanies := make([]models.Company, 0, len(companies))
	updateCompanies := make([]models.Company, 0, len(companies))

	// Begin transaction
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, get all existing company IDs to determine which ones to insert vs update
	existingCompanyIDs := make(map[int64]bool)
	rows, err := tx.QueryxContext(ctx, "SELECT id FROM autotask.companies")
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
		// Extract company ID
		idVal, ok := companyData["id"]
		if !ok {
			log.Warn().Interface("company", companyData).Msg("Company data missing ID field, skipping")
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
				Interface("company", companyData).
				Str("idType", fmt.Sprintf("%T", idVal)).
				Msg("Unexpected ID type, skipping")
			continue
		}

		// Extract company fields
		company := models.Company{
			ID:     id,
			Name:   getStringValue(companyData, "companyName"),
			SyncAt: time.Now(),
		}

		// Set optional fields
		if val, ok := companyData["address1"]; ok {
			company.AddressLine1.String = fmt.Sprintf("%v", val)
			company.AddressLine1.Valid = true
		}
		if val, ok := companyData["address2"]; ok {
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

		// Add to appropriate slice based on whether it exists
		if existingCompanyIDs[id] {
			updateCompanies = append(updateCompanies, company)
		} else {
			newCompanies = append(newCompanies, company)
		}
	}

	// Batch insert new companies
	if len(newCompanies) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			INSERT INTO autotask.companies (
				id, name, address_line1, address_line2, city, state, postal_code,
				country, phone, active, created_at, updated_at, sync_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12, $13
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer stmt.Close()

		for _, company := range newCompanies {
			_, err = stmt.ExecContext(ctx,
				company.ID,
				company.Name,
				company.AddressLine1,
				company.AddressLine2,
				company.City,
				company.State,
				company.PostalCode,
				company.Country,
				company.Phone,
				company.Active,
				company.CreatedAt,
				company.UpdatedAt,
				company.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to insert company %d: %w", company.ID, err)
			}
		}

		log.Info().Int("count", len(newCompanies)).Msg("Inserted new companies")
	}

	// Batch update existing companies
	if len(updateCompanies) > 0 {
		// Use a prepared statement for better performance
		stmt, err := tx.PreparexContext(ctx, `
			UPDATE autotask.companies SET
				name = $2,
				address_line1 = $3,
				address_line2 = $4,
				city = $5,
				state = $6,
				postal_code = $7,
				country = $8,
				phone = $9,
				active = $10,
				created_at = $11,
				updated_at = $12,
				sync_at = $13
			WHERE id = $1
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
		defer stmt.Close()

		for _, company := range updateCompanies {
			_, err = stmt.ExecContext(ctx,
				company.ID,
				company.Name,
				company.AddressLine1,
				company.AddressLine2,
				company.City,
				company.State,
				company.PostalCode,
				company.Country,
				company.Phone,
				company.Active,
				company.CreatedAt,
				company.UpdatedAt,
				company.SyncAt,
			)
			if err != nil {
				return fmt.Errorf("failed to update company %d: %w", company.ID, err)
			}
		}

		log.Info().Int("count", len(updateCompanies)).Msg("Updated existing companies")
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

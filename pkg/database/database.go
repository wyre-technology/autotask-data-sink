package database

import (
	"fmt"
	"os"

	"github.com/asachs01/autotask-data-sink/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/rs/zerolog/log"
)

// DB represents a database connection
type DB struct {
	*sqlx.DB
}

// New creates a new database connection
func New(cfg config.DatabaseConfig) (*DB, error) {
	// Check if PGHOST is set, which indicates we're using environment variables
	pgHost := os.Getenv("PGHOST")
	if pgHost != "" {
		log.Info().Msg("Using libpq environment variables for database connection")

		// Log all environment variables related to PostgreSQL
		log.Debug().
			Str("PGHOST", os.Getenv("PGHOST")).
			Str("PGPORT", os.Getenv("PGPORT")).
			Str("PGDATABASE", os.Getenv("PGDATABASE")).
			Str("PGUSER", os.Getenv("PGUSER")).
			Str("PGSSLMODE", os.Getenv("PGSSLMODE")).
			Msg("PostgreSQL environment variables")

		// Connect to the database using the PostgreSQL driver's built-in support for environment variables
		// This is the recommended approach as it handles all the connection parameters correctly
		db, err := sqlx.Open("postgres", "")
		if err != nil {
			return nil, fmt.Errorf("failed to open database connection: %w", err)
		}

		// Test the connection
		if err := db.Ping(); err != nil {
			return nil, fmt.Errorf("failed to connect to database: %w", err)
		}

		// Set connection pool settings
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)

		return &DB{db}, nil
	}

	// Fall back to config values
	log.Info().Msg("Using config values for database connection")
	connStr := cfg.ConnectionString()
	log.Debug().Str("connection_string", connStr).Msg("Database connection string")

	// Connect to the database
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return &DB{db}, nil
}

// Setup initializes the database schema
func (db *DB) Setup() error {
	// Create the schema if it doesn't exist
	if err := db.createSchema(); err != nil {
		return err
	}

	// Create tables for each entity type
	if err := db.createTables(); err != nil {
		return err
	}

	return nil
}

// createSchema creates the database schema
func (db *DB) createSchema() error {
	_, err := db.Exec(`
		CREATE SCHEMA IF NOT EXISTS autotask;
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// createTables creates all required tables
func (db *DB) createTables() error {
	// Create companies table
	if err := db.createCompaniesTable(); err != nil {
		return err
	}

	// Create contacts table
	if err := db.createContactsTable(); err != nil {
		return err
	}

	// Create tickets table
	if err := db.createTicketsTable(); err != nil {
		return err
	}

	// Create time entries table
	if err := db.createTimeEntriesTable(); err != nil {
		return err
	}

	// Create resources table
	if err := db.createResourcesTable(); err != nil {
		return err
	}

	// Create contracts table
	if err := db.createContractsTable(); err != nil {
		return err
	}

	return nil
}

// createCompaniesTable creates the companies table
func (db *DB) createCompaniesTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.companies (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			address_line1 TEXT,
			address_line2 TEXT,
			city TEXT,
			state TEXT,
			postal_code TEXT,
			country TEXT,
			phone TEXT,
			active BOOLEAN,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.companies_history (
			history_id SERIAL PRIMARY KEY,
			company_id BIGINT NOT NULL REFERENCES autotask.companies(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_companies_history_company_id ON autotask.companies_history(company_id);
		CREATE INDEX IF NOT EXISTS idx_companies_history_changed_at ON autotask.companies_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create companies tables: %w", err)
	}
	return nil
}

// createContactsTable creates the contacts table
func (db *DB) createContactsTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.contacts (
			id BIGINT PRIMARY KEY,
			company_id BIGINT REFERENCES autotask.companies(id),
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			phone TEXT,
			mobile TEXT,
			active BOOLEAN,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.contacts_history (
			history_id SERIAL PRIMARY KEY,
			contact_id BIGINT NOT NULL REFERENCES autotask.contacts(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON autotask.contacts(company_id);
		CREATE INDEX IF NOT EXISTS idx_contacts_history_contact_id ON autotask.contacts_history(contact_id);
		CREATE INDEX IF NOT EXISTS idx_contacts_history_changed_at ON autotask.contacts_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create contacts tables: %w", err)
	}
	return nil
}

// createTicketsTable creates the tickets table
func (db *DB) createTicketsTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.tickets (
			id BIGINT PRIMARY KEY,
			title TEXT NOT NULL,
			description TEXT,
			status INT,
			priority INT,
			queue_id BIGINT,
			company_id BIGINT REFERENCES autotask.companies(id),
			contact_id BIGINT REFERENCES autotask.contacts(id),
			assigned_resource_id BIGINT,
			created_by_resource_id BIGINT,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			completed_date TIMESTAMP WITH TIME ZONE,
			due_date TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.tickets_history (
			history_id SERIAL PRIMARY KEY,
			ticket_id BIGINT NOT NULL REFERENCES autotask.tickets(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_tickets_company_id ON autotask.tickets(company_id);
		CREATE INDEX IF NOT EXISTS idx_tickets_contact_id ON autotask.tickets(contact_id);
		CREATE INDEX IF NOT EXISTS idx_tickets_assigned_resource_id ON autotask.tickets(assigned_resource_id);
		CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON autotask.tickets(created_at);
		CREATE INDEX IF NOT EXISTS idx_tickets_completed_date ON autotask.tickets(completed_date);
		CREATE INDEX IF NOT EXISTS idx_tickets_status ON autotask.tickets(status);
		CREATE INDEX IF NOT EXISTS idx_tickets_history_ticket_id ON autotask.tickets_history(ticket_id);
		CREATE INDEX IF NOT EXISTS idx_tickets_history_changed_at ON autotask.tickets_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create tickets tables: %w", err)
	}
	return nil
}

// createTimeEntriesTable creates the time entries table
func (db *DB) createTimeEntriesTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.time_entries (
			id BIGINT PRIMARY KEY,
			ticket_id BIGINT REFERENCES autotask.tickets(id),
			resource_id BIGINT,
			date_worked DATE,
			start_time TIMESTAMP WITH TIME ZONE,
			end_time TIMESTAMP WITH TIME ZONE,
			hours_worked DECIMAL(10,2),
			summary TEXT,
			internal_notes TEXT,
			non_billable BOOLEAN,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.time_entries_history (
			history_id SERIAL PRIMARY KEY,
			time_entry_id BIGINT NOT NULL REFERENCES autotask.time_entries(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_time_entries_ticket_id ON autotask.time_entries(ticket_id);
		CREATE INDEX IF NOT EXISTS idx_time_entries_resource_id ON autotask.time_entries(resource_id);
		CREATE INDEX IF NOT EXISTS idx_time_entries_date_worked ON autotask.time_entries(date_worked);
		CREATE INDEX IF NOT EXISTS idx_time_entries_history_time_entry_id ON autotask.time_entries_history(time_entry_id);
		CREATE INDEX IF NOT EXISTS idx_time_entries_history_changed_at ON autotask.time_entries_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create time entries tables: %w", err)
	}
	return nil
}

// createResourcesTable creates the resources table
func (db *DB) createResourcesTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.resources (
			id BIGINT PRIMARY KEY,
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			active BOOLEAN,
			resource_type INT,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.resources_history (
			history_id SERIAL PRIMARY KEY,
			resource_id BIGINT NOT NULL REFERENCES autotask.resources(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_resources_history_resource_id ON autotask.resources_history(resource_id);
		CREATE INDEX IF NOT EXISTS idx_resources_history_changed_at ON autotask.resources_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create resources tables: %w", err)
	}
	return nil
}

// createContractsTable creates the contracts table
func (db *DB) createContractsTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS autotask.contracts (
			id BIGINT PRIMARY KEY,
			company_id BIGINT REFERENCES autotask.companies(id),
			name TEXT NOT NULL,
			contract_type INT,
			status INT,
			start_date DATE,
			end_date DATE,
			time_remaining DECIMAL(10,2),
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS autotask.contracts_history (
			history_id SERIAL PRIMARY KEY,
			contract_id BIGINT NOT NULL REFERENCES autotask.contracts(id),
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_contracts_company_id ON autotask.contracts(company_id);
		CREATE INDEX IF NOT EXISTS idx_contracts_history_contract_id ON autotask.contracts_history(contract_id);
		CREATE INDEX IF NOT EXISTS idx_contracts_history_changed_at ON autotask.contracts_history(changed_at);
	`)
	if err != nil {
		return fmt.Errorf("failed to create contracts tables: %w", err)
	}
	return nil
}

// CreateViews creates database views for common queries
func (db *DB) CreateViews() error {
	// Create view for ticket metrics
	if err := db.createTicketMetricsView(); err != nil {
		return err
	}

	// Create view for resource utilization
	if err := db.createResourceUtilizationView(); err != nil {
		return err
	}

	// Create view for contract profitability
	if err := db.createContractProfitabilityView(); err != nil {
		return err
	}

	return nil
}

// createTicketMetricsView creates a view for ticket metrics
func (db *DB) createTicketMetricsView() error {
	_, err := db.Exec(`
		CREATE OR REPLACE VIEW autotask.ticket_metrics AS
		SELECT
			DATE_TRUNC('day', created_at) AS date,
			COUNT(*) AS tickets_created,
			COUNT(CASE WHEN completed_date IS NOT NULL THEN 1 END) AS tickets_completed,
			AVG(EXTRACT(EPOCH FROM (completed_date - created_at))/3600)::NUMERIC(10,2) AS avg_resolution_hours,
			assigned_resource_id,
			company_id
		FROM
			autotask.tickets
		GROUP BY
			DATE_TRUNC('day', created_at),
			assigned_resource_id,
			company_id;
	`)
	if err != nil {
		return fmt.Errorf("failed to create ticket metrics view: %w", err)
	}
	return nil
}

// createResourceUtilizationView creates a view for resource utilization
func (db *DB) createResourceUtilizationView() error {
	_, err := db.Exec(`
		CREATE OR REPLACE VIEW autotask.resource_utilization AS
		SELECT
			r.id AS resource_id,
			r.first_name,
			r.last_name,
			DATE_TRUNC('day', te.date_worked) AS date,
			SUM(te.hours_worked) AS total_hours,
			SUM(CASE WHEN te.non_billable = false THEN te.hours_worked ELSE 0 END) AS billable_hours,
			SUM(CASE WHEN te.non_billable = true THEN te.hours_worked ELSE 0 END) AS non_billable_hours
		FROM
			autotask.resources r
		LEFT JOIN
			autotask.time_entries te ON r.id = te.resource_id
		WHERE
			r.active = true
		GROUP BY
			r.id,
			r.first_name,
			r.last_name,
			DATE_TRUNC('day', te.date_worked);
	`)
	if err != nil {
		return fmt.Errorf("failed to create resource utilization view: %w", err)
	}
	return nil
}

// createContractProfitabilityView creates a view for contract profitability
func (db *DB) createContractProfitabilityView() error {
	_, err := db.Exec(`
		CREATE OR REPLACE VIEW autotask.contract_profitability AS
		SELECT
			c.id AS contract_id,
			c.name AS contract_name,
			co.id AS company_id,
			co.name AS company_name,
			SUM(te.hours_worked) AS total_hours_worked
		FROM
			autotask.contracts c
		JOIN
			autotask.companies co ON c.company_id = co.id
		LEFT JOIN
			autotask.tickets t ON t.company_id = co.id
		LEFT JOIN
			autotask.time_entries te ON te.ticket_id = t.id
		GROUP BY
			c.id,
			c.name,
			co.id,
			co.name;
	`)
	if err != nil {
		return fmt.Errorf("failed to create contract profitability view: %w", err)
	}
	return nil
}

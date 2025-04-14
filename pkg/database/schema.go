package database

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

// SchemaManager handles database schema operations
type SchemaManager struct {
	db *DB
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(db *DB) *SchemaManager {
	return &SchemaManager{db: db}
}

// Setup initializes the database schema
func (sm *SchemaManager) Setup(ctx context.Context) error {
	return sm.db.WithTransaction(ctx, func(tx *Tx) error {
		// Create schema
		if err := sm.CreateSchema(ctx, tx); err != nil {
			return err
		}

		// Create tables first
		if err := sm.CreateTables(ctx, tx); err != nil {
			return err
		}

		// Create history tables
		tables := []string{"companies", "contacts", "resources", "tickets", "time_entries", "contracts"}
		for _, table := range tables {
			if err := sm.CreateHistoryTable(ctx, tx, table); err != nil {
				return err
			}
		}

		// Create indexes after tables are created
		if err := sm.CreateIndexes(ctx, tx); err != nil {
			return err
		}

		// Create foreign key constraints last
		if err := sm.CreateConstraints(ctx, tx); err != nil {
			return err
		}

		return nil
	})
}

// CreateSchema creates the database schema
func (sm *SchemaManager) CreateSchema(ctx context.Context, tx *Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS autotask`)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}

// CreateTables creates all required tables
func (sm *SchemaManager) CreateTables(ctx context.Context, tx *Tx) error {
	// Companies table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.companies (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			company_number TEXT,
			address_line1 TEXT,
			address_line2 TEXT,
			city TEXT,
			state TEXT,
			postal_code TEXT,
			country TEXT,
			phone TEXT,
			active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create companies table: %w", err)
	}

	// Contacts table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.contacts (
			id BIGINT PRIMARY KEY,
			company_id BIGINT NOT NULL,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			email TEXT,
			phone TEXT,
			mobile TEXT,
			active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create contacts table: %w", err)
	}

	// Resources table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.resources (
			id BIGINT PRIMARY KEY,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			email TEXT NOT NULL,
			resource_type TEXT NOT NULL,
			active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create resources table: %w", err)
	}

	// Tickets table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.tickets (
			id BIGINT PRIMARY KEY,
			ticket_number TEXT NOT NULL,
			company_id BIGINT NOT NULL,
			contact_id BIGINT,
			resource_id BIGINT,
			title TEXT NOT NULL,
			description TEXT,
			status TEXT NOT NULL,
			priority TEXT,
			due_date TIMESTAMP WITH TIME ZONE,
			completed_date TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create tickets table: %w", err)
	}

	// Time entries table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.time_entries (
			id BIGINT PRIMARY KEY,
			resource_id BIGINT NOT NULL,
			ticket_id BIGINT NOT NULL,
			contract_id BIGINT,
			hours_worked DECIMAL(10,2) NOT NULL,
			notes TEXT,
			date_worked TIMESTAMP WITH TIME ZONE NOT NULL,
			non_billable BOOLEAN NOT NULL DEFAULT false,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create time_entries table: %w", err)
	}

	// Contracts table
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS autotask.contracts (
			id BIGINT PRIMARY KEY,
			company_id BIGINT NOT NULL,
			name TEXT NOT NULL,
			contract_type TEXT NOT NULL,
			start_date TIMESTAMP WITH TIME ZONE NOT NULL,
			end_date TIMESTAMP WITH TIME ZONE NOT NULL,
			contract_hours DECIMAL(10,2),
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			sync_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("failed to create contracts table: %w", err)
	}

	return nil
}

// CreateHistoryTable creates a history table for the given entity
func (sm *SchemaManager) CreateHistoryTable(ctx context.Context, tx *Tx, entityTable string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS autotask.%s_history (
			history_id BIGSERIAL PRIMARY KEY,
			entity_id BIGINT NOT NULL,
			field_name TEXT NOT NULL,
			old_value TEXT,
			new_value TEXT,
			changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)`, entityTable)

	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create history table for %s: %w", entityTable, err)
	}

	return nil
}

// CreateIndexes creates all required indexes
func (sm *SchemaManager) CreateIndexes(ctx context.Context, tx *Tx) error {
	indexes := []string{
		// Companies indexes
		`CREATE INDEX IF NOT EXISTS idx_companies_name ON autotask.companies (name)`,
		`CREATE INDEX IF NOT EXISTS idx_companies_company_number ON autotask.companies (company_number)`,
		`CREATE INDEX IF NOT EXISTS idx_companies_updated_at ON autotask.companies (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_companies_active ON autotask.companies (active)`,

		// Contacts indexes
		`CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON autotask.contacts (company_id)`,
		`CREATE INDEX IF NOT EXISTS idx_contacts_email ON autotask.contacts (email)`,
		`CREATE INDEX IF NOT EXISTS idx_contacts_name ON autotask.contacts (first_name, last_name)`,
		`CREATE INDEX IF NOT EXISTS idx_contacts_updated_at ON autotask.contacts (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_contacts_active ON autotask.contacts (active)`,

		// Resources indexes
		`CREATE INDEX IF NOT EXISTS idx_resources_email ON autotask.resources (email)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_name ON autotask.resources (first_name, last_name)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_type ON autotask.resources (resource_type)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_updated_at ON autotask.resources (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_resources_active ON autotask.resources (active)`,

		// Tickets indexes
		`CREATE INDEX IF NOT EXISTS idx_tickets_company_id ON autotask.tickets (company_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_contact_id ON autotask.tickets (contact_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_resource_id ON autotask.tickets (resource_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_status ON autotask.tickets (status)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_due_date ON autotask.tickets (due_date)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_completed_date ON autotask.tickets (completed_date)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_updated_at ON autotask.tickets (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON autotask.tickets (created_at)`,

		// Time entries indexes
		`CREATE INDEX IF NOT EXISTS idx_time_entries_resource_id ON autotask.time_entries (resource_id)`,
		`CREATE INDEX IF NOT EXISTS idx_time_entries_ticket_id ON autotask.time_entries (ticket_id)`,
		`CREATE INDEX IF NOT EXISTS idx_time_entries_contract_id ON autotask.time_entries (contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_time_entries_date_worked ON autotask.time_entries (date_worked)`,
		`CREATE INDEX IF NOT EXISTS idx_time_entries_updated_at ON autotask.time_entries (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_time_entries_non_billable ON autotask.time_entries (non_billable)`,

		// Contracts indexes
		`CREATE INDEX IF NOT EXISTS idx_contracts_company_id ON autotask.contracts (company_id)`,
		`CREATE INDEX IF NOT EXISTS idx_contracts_date_range ON autotask.contracts (start_date, end_date)`,
		`CREATE INDEX IF NOT EXISTS idx_contracts_updated_at ON autotask.contracts (updated_at)`,
		`CREATE INDEX IF NOT EXISTS idx_contracts_name ON autotask.contracts (name)`,
		`CREATE INDEX IF NOT EXISTS idx_contracts_type ON autotask.contracts (contract_type)`,
	}

	// Create indexes for history tables
	tables := []string{"companies", "contacts", "resources", "tickets", "time_entries", "contracts"}
	for _, table := range tables {
		historyIndexes := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_history_entity_id ON autotask.%s_history (entity_id)`, table, table),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_history_changed_at ON autotask.%s_history (changed_at)`, table, table),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_history_field_name ON autotask.%s_history (field_name)`, table, table),
		}
		indexes = append(indexes, historyIndexes...)
	}

	// Execute all index creation statements
	for _, idx := range indexes {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// CreateConstraints creates all required foreign key constraints
func (sm *SchemaManager) CreateConstraints(ctx context.Context, tx *Tx) error {
	constraints := []string{
		// Contacts foreign keys
		`ALTER TABLE autotask.contacts ADD CONSTRAINT fk_contacts_company
			FOREIGN KEY (company_id) REFERENCES autotask.companies(id) ON DELETE CASCADE`,

		// Tickets foreign keys
		`ALTER TABLE autotask.tickets ADD CONSTRAINT fk_tickets_company
			FOREIGN KEY (company_id) REFERENCES autotask.companies(id)`,
		`ALTER TABLE autotask.tickets ADD CONSTRAINT fk_tickets_contact
			FOREIGN KEY (contact_id) REFERENCES autotask.contacts(id)`,
		`ALTER TABLE autotask.tickets ADD CONSTRAINT fk_tickets_resource
			FOREIGN KEY (resource_id) REFERENCES autotask.resources(id)`,

		// Time entries foreign keys
		`ALTER TABLE autotask.time_entries ADD CONSTRAINT fk_time_entries_resource
			FOREIGN KEY (resource_id) REFERENCES autotask.resources(id)`,
		`ALTER TABLE autotask.time_entries ADD CONSTRAINT fk_time_entries_ticket
			FOREIGN KEY (ticket_id) REFERENCES autotask.tickets(id)`,
		`ALTER TABLE autotask.time_entries ADD CONSTRAINT fk_time_entries_contract
			FOREIGN KEY (contract_id) REFERENCES autotask.contracts(id)`,

		// Contracts foreign keys
		`ALTER TABLE autotask.contracts ADD CONSTRAINT fk_contracts_company
			FOREIGN KEY (company_id) REFERENCES autotask.companies(id)`,
	}

	// Execute all constraint creation statements
	for _, constraint := range constraints {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			DO $$
			BEGIN
				%s;
			EXCEPTION WHEN duplicate_object THEN
				NULL;
			END $$;
		`, constraint)); err != nil {
			return fmt.Errorf("failed to create constraint: %w", err)
		}
	}

	return nil
}

// OptimizeTables performs table optimization operations
func (sm *SchemaManager) OptimizeTables(ctx context.Context) error {
	return sm.db.WithTransaction(ctx, func(tx *Tx) error {
		// Get list of tables
		rows, err := tx.QueryContext(ctx, `
			SELECT schemaname || '.' || tablename
			FROM pg_tables
			WHERE schemaname = 'autotask'
		`)
		if err != nil {
			return fmt.Errorf("failed to get table list: %w", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("failed to scan table name: %w", err)
			}
			tables = append(tables, tableName)
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating tables: %w", err)
		}

		// Analyze and vacuum each table
		for _, table := range tables {
			log.Info().
				Str("table", table).
				Msg("Optimizing table")

			// Analyze the table
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("ANALYZE %s", table)); err != nil {
				log.Error().
					Err(err).
					Str("table", table).
					Msg("Failed to analyze table")
				continue
			}

			// Vacuum the table
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("VACUUM ANALYZE %s", table)); err != nil {
				log.Error().
					Err(err).
					Str("table", table).
					Msg("Failed to vacuum table")
				continue
			}

			log.Info().
				Str("table", table).
				Msg("Table optimization completed")
		}

		return nil
	})
}

// ValidateSchema validates the database schema
func (sm *SchemaManager) ValidateSchema(ctx context.Context) error {
	log.Info().Msg("Validating database schema...")

	return sm.db.WithTransaction(ctx, func(tx *Tx) error {
		// Check if all required tables exist
		tables := []string{
			"companies", "contacts", "resources", "tickets",
			"time_entries", "contracts",
		}

		for _, table := range tables {
			var exists bool
			if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM information_schema.tables
					WHERE table_schema = 'autotask'
					AND table_name = $1
				)
			`, table).Scan(&exists); err != nil {
				return fmt.Errorf("failed to check table existence: %w", err)
			}

			if !exists {
				return fmt.Errorf("required table %s does not exist", table)
			}

			// Check if history table exists
			historyTable := fmt.Sprintf("%s_history", table)
			if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM information_schema.tables
					WHERE table_schema = 'autotask'
					AND table_name = $1
				)
			`, historyTable).Scan(&exists); err != nil {
				return fmt.Errorf("failed to check history table existence: %w", err)
			}

			if !exists {
				return fmt.Errorf("required history table %s does not exist", historyTable)
			}
		}

		log.Info().Msg("Database schema validation completed successfully")
		return nil
	})
}

// CreateViews creates database views for common queries
func (sm *SchemaManager) CreateViews(ctx context.Context) error {
	// Create view for ticket metrics
	if err := sm.createTicketMetricsView(ctx); err != nil {
		return err
	}

	// Create view for resource utilization
	if err := sm.createResourceUtilizationView(ctx); err != nil {
		return err
	}

	// Create view for contract profitability
	if err := sm.createContractProfitabilityView(ctx); err != nil {
		return err
	}

	return nil
}

// createTicketMetricsView creates a view for ticket metrics
func (sm *SchemaManager) createTicketMetricsView(ctx context.Context) error {
	_, err := sm.db.ExecContext(ctx, `
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
func (sm *SchemaManager) createResourceUtilizationView(ctx context.Context) error {
	_, err := sm.db.ExecContext(ctx, `
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
func (sm *SchemaManager) createContractProfitabilityView(ctx context.Context) error {
	_, err := sm.db.ExecContext(ctx, `
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

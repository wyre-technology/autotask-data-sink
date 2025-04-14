package database

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/asachs01/autotask-data-sink/pkg/config"
	"github.com/asachs01/autotask-data-sink/pkg/models"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/rs/zerolog/log"
)

// DB represents a database connection
type DB struct {
	*sqlx.DB
	schema      *SchemaManager
	performance *PerformanceManager
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
		db.SetConnMaxLifetime(time.Hour)
		db.SetConnMaxIdleTime(time.Minute * 5)

		dbWrapper := &DB{DB: db}
		dbWrapper.schema = NewSchemaManager(dbWrapper)
		dbWrapper.performance = NewPerformanceManager(db.DB)

		return dbWrapper, nil
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
	db.SetMaxOpenConns(cfg.Pool.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Pool.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.Pool.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.Pool.ConnMaxIdleTime)

	log.Info().
		Int("max_open_conns", cfg.Pool.MaxOpenConns).
		Int("max_idle_conns", cfg.Pool.MaxIdleConns).
		Dur("conn_max_lifetime", cfg.Pool.ConnMaxLifetime).
		Dur("conn_max_idle_time", cfg.Pool.ConnMaxIdleTime).
		Msg("Configured database connection pool")

	dbWrapper := &DB{DB: db}
	dbWrapper.schema = NewSchemaManager(dbWrapper)
	dbWrapper.performance = NewPerformanceManager(db.DB)

	return dbWrapper, nil
}

// Schema returns the schema manager
func (db *DB) Schema() *SchemaManager {
	return db.schema
}

// Performance returns the performance manager
func (db *DB) Performance() *PerformanceManager {
	return db.performance
}

// Setup initializes the database schema
func (db *DB) Setup(ctx context.Context) error {
	return db.schema.Setup(ctx)
}

// ValidateSchema validates the database schema
func (db *DB) ValidateSchema(ctx context.Context) error {
	return db.schema.ValidateSchema(ctx)
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

// BatchProcessor is a function that processes a batch of records within a transaction
type BatchProcessor func(context.Context, *Tx) error

// ProcessBatch processes records in batches within transactions
func (db *DB) ProcessBatch(ctx context.Context, batchSize int, processor BatchProcessor) error {
	// Process batch in a transaction
	err := db.WithTransaction(ctx, func(tx *Tx) error {
		return processor(ctx, tx)
	})
	if err != nil {
		return fmt.Errorf("failed to process batch: %w", err)
	}

	return nil
}

// CompanyBatchProcessor is a function that processes a batch of companies within a transaction
type CompanyBatchProcessor func(context.Context, *Tx, []models.Company) error

// ProcessCompanyBatch processes a batch of companies within a transaction
func (db *DB) ProcessCompanyBatch(ctx context.Context, records []models.Company, batchSize int, processor CompanyBatchProcessor) error {
	length := len(records)
	for i := 0; i < length; i += batchSize {
		end := i + batchSize
		if end > length {
			end = length
		}

		// Get the batch
		batch := records[i:end]

		// Process batch in a transaction
		err := db.WithTransaction(ctx, func(tx *Tx) error {
			return processor(ctx, tx, batch)
		})
		if err != nil {
			return fmt.Errorf("failed to process batch %d-%d: %w", i, end, err)
		}

		log.Debug().
			Int("start", i).
			Int("end", end).
			Int("total", length).
			Msg("Processed batch successfully")
	}

	return nil
}

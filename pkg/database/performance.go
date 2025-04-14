package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// PerformanceManager handles database performance monitoring and optimization
type PerformanceManager struct {
	db *sql.DB
}

// NewPerformanceManager creates a new PerformanceManager instance
func NewPerformanceManager(db *sql.DB) *PerformanceManager {
	return &PerformanceManager{
		db: db,
	}
}

// PerformanceStats tracks database performance metrics
type PerformanceStats struct {
	QueryCount      int64
	SlowQueryCount  int64
	TotalQueryTime  time.Duration
	SlowQueryTime   time.Duration
	LastAnalyze     time.Time
	LastVacuum      time.Time
	ConnectionStats ConnectionStats
	TableStats      map[string]TableStats
}

// ConnectionStats tracks database connection metrics
type ConnectionStats struct {
	ActiveConnections int64
	IdleConnections   int64
	MaxConnections    int64
	ConnectionWaits   int64
	ConnectionTimeout int64
	CacheHitRatio     float64
}

// TableStats represents statistics about a database table
type TableStats struct {
	SchemaName  string
	TableName   string
	LiveTuples  int64
	DeadTuples  int64
	SeqScans    int64
	IdxScans    int64
	TotalTime   float64
	LastAnalyze time.Time
	LastVacuum  time.Time
}

// GetConnectionStats returns current database connection statistics
func (pm *PerformanceManager) GetConnectionStats(ctx context.Context) sql.DBStats {
	return pm.db.Stats()
}

// GetSlowQueries retrieves queries that took longer than the specified duration
func (pm *PerformanceManager) GetSlowQueries(ctx context.Context, threshold time.Duration) ([]SlowQuery, error) {
	rows, err := pm.db.QueryContext(ctx, `
		SELECT 
			query,
			calls,
			total_exec_time,
			mean_exec_time,
			rows_processed
		FROM 
			pg_stat_statements
		WHERE 
			mean_exec_time > $1
		ORDER BY 
			mean_exec_time DESC
		LIMIT 10;
	`, float64(threshold.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to get slow queries: %w", err)
	}
	defer rows.Close()

	var queries []SlowQuery
	for rows.Next() {
		var q SlowQuery
		err := rows.Scan(
			&q.Query,
			&q.Calls,
			&q.TotalExecTime,
			&q.MeanExecTime,
			&q.RowsProcessed,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan slow query row: %w", err)
		}
		queries = append(queries, q)
	}

	return queries, nil
}

// GetTableStats retrieves statistics about table usage and size
func (pm *PerformanceManager) GetTableStats(ctx context.Context) ([]TableStats, error) {
	rows, err := pm.db.QueryContext(ctx, `
		SELECT
			schemaname,
			relname,
			n_live_tup,
			n_dead_tup,
			seq_scan,
			idx_scan,
			total_time,
			last_analyze,
			last_vacuum
		FROM
			pg_stat_user_tables
		WHERE
			schemaname = 'autotask'
		ORDER BY
			total_time DESC;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get table stats: %w", err)
	}
	defer rows.Close()

	var stats []TableStats
	for rows.Next() {
		var s TableStats
		err := rows.Scan(
			&s.SchemaName,
			&s.TableName,
			&s.LiveTuples,
			&s.DeadTuples,
			&s.SeqScans,
			&s.IdxScans,
			&s.TotalTime,
			&s.LastAnalyze,
			&s.LastVacuum,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table stats row: %w", err)
		}
		stats = append(stats, s)
	}

	return stats, nil
}

// SlowQuery represents a slow-performing database query
type SlowQuery struct {
	Query         string
	Calls         int64
	TotalExecTime float64
	MeanExecTime  float64
	RowsProcessed int64
}

// GetStats retrieves current database performance statistics
func (pm *PerformanceManager) GetStats(ctx context.Context) (*PerformanceStats, error) {
	stats := &PerformanceStats{
		TableStats: make(map[string]TableStats),
	}

	// Get connection statistics
	if err := pm.db.QueryRowContext(ctx, `
		SELECT
			numbackends,
			sum(xact_commit + xact_rollback) as total_xacts,
			sum(blks_hit)::float / (sum(blks_hit) + sum(blks_read)) as cache_hit_ratio
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(&stats.ConnectionStats.ActiveConnections, &stats.QueryCount, &stats.ConnectionStats.CacheHitRatio); err != nil {
		return nil, fmt.Errorf("failed to get connection stats: %w", err)
	}

	// Get table statistics
	rows, err := pm.db.QueryContext(ctx, `
		SELECT
			schemaname || '.' || relname as table_name,
			n_live_tup as row_count,
			n_dead_tup as dead_tuples,
			last_analyze,
			last_vacuum,
			seq_scan,
			idx_scan,
			idx_scan::float / nullif(seq_scan + idx_scan, 0) as index_usage_ratio
		FROM pg_stat_user_tables
		WHERE schemaname = 'autotask'
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get table stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var tableStats TableStats
		var seqScan, idxScan int64
		var indexUsageRatio float64

		if err := rows.Scan(
			&tableName,
			&tableStats.LiveTuples,
			&tableStats.DeadTuples,
			&tableStats.LastAnalyze,
			&tableStats.LastVacuum,
			&seqScan,
			&idxScan,
			&indexUsageRatio,
		); err != nil {
			return nil, fmt.Errorf("failed to scan table stats: %w", err)
		}

		tableStats.SeqScans = seqScan
		tableStats.IdxScans = idxScan
		tableStats.TotalTime = float64(time.Since(tableStats.LastAnalyze))

		stats.TableStats[tableName] = tableStats
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table stats: %w", err)
	}

	return stats, nil
}

// OptimizeTables performs table optimization operations
func (db *DB) OptimizeTables(ctx context.Context) error {
	return db.WithTransaction(ctx, func(tx *Tx) error {
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

// CreateIndexes creates recommended indexes based on query patterns
func (db *DB) CreateIndexes(ctx context.Context) error {
	return db.WithTransaction(ctx, func(tx *Tx) error {
		// Get index recommendations
		rows, err := tx.QueryContext(ctx, `
			SELECT schemaname, tablename, indexdef
			FROM pg_get_missing_indexes()
			WHERE schemaname = 'autotask'
		`)
		if err != nil {
			return fmt.Errorf("failed to get index recommendations: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var schema, table, indexDef string
			if err := rows.Scan(&schema, &table, &indexDef); err != nil {
				return fmt.Errorf("failed to scan index recommendation: %w", err)
			}

			log.Info().
				Str("schema", schema).
				Str("table", table).
				Str("index", indexDef).
				Msg("Creating recommended index")

			if _, err := tx.ExecContext(ctx, indexDef); err != nil {
				log.Error().
					Err(err).
					Str("schema", schema).
					Str("table", table).
					Str("index", indexDef).
					Msg("Failed to create index")
				continue
			}
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating index recommendations: %w", err)
		}

		return nil
	})
}

// MonitorQueryPerformance monitors query performance and logs slow queries
func (pm *PerformanceManager) MonitorQueryPerformance(ctx context.Context, slowQueryThreshold time.Duration) error {
	query := `
		SELECT
			query,
			calls,
			total_time / 1000 as total_ms,
			mean_time / 1000 as mean_ms,
			rowcount
		FROM pg_stat_statements
		WHERE total_time / 1000 > $1
		ORDER BY total_time DESC
		LIMIT 10
	`

	queryRows, err := pm.db.QueryContext(ctx, query, slowQueryThreshold.Milliseconds())
	if err != nil {
		return fmt.Errorf("failed to get slow queries: %w", err)
	}
	defer queryRows.Close()

	for queryRows.Next() {
		var (
			queryText string
			calls     int64
			totalMs   float64
			meanMs    float64
			rowCount  int64
		)

		if err := queryRows.Scan(&queryText, &calls, &totalMs, &meanMs, &rowCount); err != nil {
			return fmt.Errorf("failed to scan slow query: %w", err)
		}

		log.Warn().
			Str("query", queryText).
			Int64("calls", calls).
			Float64("total_ms", totalMs).
			Float64("mean_ms", meanMs).
			Int64("rows", rowCount).
			Msg("Slow query detected")
	}

	if err := queryRows.Err(); err != nil {
		return fmt.Errorf("error iterating slow queries: %w", err)
	}

	return nil
}

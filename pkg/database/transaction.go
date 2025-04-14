package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

// TxFn is a function that executes within a transaction
type TxFn func(*Tx) error

// Tx wraps sqlx.Tx to add additional functionality
type Tx struct {
	*sqlx.Tx
}

// WithTransaction executes the given function within a transaction
func (db *DB) WithTransaction(ctx context.Context, fn TxFn) error {
	// Start a transaction
	sqlxTx, err := db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	tx := &Tx{Tx: sqlxTx}

	// Handle panic by rolling back
	defer func() {
		if p := recover(); p != nil {
			// Log the panic
			log.Error().
				Interface("panic", p).
				Msg("Panic in transaction, rolling back")

			// Attempt rollback
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error().
					Err(rbErr).
					Msg("Failed to rollback after panic")
			}
			// Re-panic after rollback
			panic(p)
		}
	}()

	// Execute the function
	if err := fn(tx); err != nil {
		// Attempt rollback on error
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().
				Err(rbErr).
				Msg("Failed to rollback transaction")
			// Return the original error as it's more relevant
			return fmt.Errorf("failed to execute transaction (rollback failed): %w", err)
		}
		return fmt.Errorf("failed to execute transaction (rolled back): %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// WithSavepoint executes the given function within a savepoint in an existing transaction
func (tx *Tx) WithSavepoint(ctx context.Context, name string, fn TxFn) error {
	// Create savepoint
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", name)); err != nil {
		return fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Execute the function
	if err := fn(tx); err != nil {
		// Rollback to savepoint on error
		if _, rbErr := tx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)); rbErr != nil {
			log.Error().
				Err(rbErr).
				Str("savepoint", name).
				Msg("Failed to rollback to savepoint")
			return fmt.Errorf("failed to execute savepoint (rollback failed): %w", err)
		}
		return fmt.Errorf("failed to execute savepoint (rolled back): %w", err)
	}

	// Release savepoint
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", name)); err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}

	return nil
}

// BatchOperation executes a batch of operations within a single transaction
func (db *DB) BatchOperation(ctx context.Context, batchSize int, items []interface{}, fn func(*Tx, []interface{}) error) error {
	return db.WithTransaction(ctx, func(tx *Tx) error {
		for i := 0; i < len(items); i += batchSize {
			end := i + batchSize
			if end > len(items) {
				end = len(items)
			}
			batch := items[i:end]

			// Create a savepoint for this batch
			savepointName := fmt.Sprintf("batch_%d", i/batchSize)
			if err := tx.WithSavepoint(ctx, savepointName, func(tx *Tx) error {
				return fn(tx, batch)
			}); err != nil {
				return fmt.Errorf("failed to process batch %d: %w", i/batchSize, err)
			}

			log.Debug().
				Int("batch", i/batchSize).
				Int("size", len(batch)).
				Int("total", len(items)).
				Msg("Processed batch successfully")
		}
		return nil
	})
}

// RetryableOperation executes an operation with retries on specific errors
func (db *DB) RetryableOperation(ctx context.Context, maxRetries int, isRetryable func(error) bool, op func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := op(); err != nil {
			if attempt == maxRetries || !isRetryable(err) {
				return fmt.Errorf("operation failed after %d attempts: %w", attempt+1, err)
			}
			lastErr = err
			log.Warn().
				Err(err).
				Int("attempt", attempt+1).
				Int("maxRetries", maxRetries).
				Msg("Operation failed, retrying")
			continue
		}
		return nil
	}
	return lastErr
}

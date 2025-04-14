package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// HistoryEntry represents a change in an entity
type HistoryEntry struct {
	ID         int64       `db:"history_id"`
	EntityID   int64       `db:"entity_id"`
	EntityType string      `db:"entity_type"`
	FieldName  string      `db:"field_name"`
	OldValue   interface{} `db:"old_value"`
	NewValue   interface{} `db:"new_value"`
	ChangedAt  time.Time   `db:"changed_at"`
}

// TrackChanges compares old and new versions of an entity and records any changes
func (tx *Tx) TrackChanges(ctx context.Context, entityType string, entityID int64, oldData, newData map[string]interface{}) error {
	// Get the table name for history tracking
	historyTable := fmt.Sprintf("autotask.%s_history", entityType)

	// Compare fields and track changes
	for field, newValue := range newData {
		oldValue, exists := oldData[field]
		if !exists {
			// Field is new
			if err := tx.insertHistoryEntry(ctx, historyTable, entityID, field, nil, newValue); err != nil {
				return fmt.Errorf("failed to track new field %s: %w", field, err)
			}
			continue
		}

		// Check if value has changed
		if !valueEquals(oldValue, newValue) {
			if err := tx.insertHistoryEntry(ctx, historyTable, entityID, field, oldValue, newValue); err != nil {
				return fmt.Errorf("failed to track change in field %s: %w", field, err)
			}
		}
	}

	// Check for deleted fields
	for field, oldValue := range oldData {
		if _, exists := newData[field]; !exists {
			// Field was deleted
			if err := tx.insertHistoryEntry(ctx, historyTable, entityID, field, oldValue, nil); err != nil {
				return fmt.Errorf("failed to track deleted field %s: %w", field, err)
			}
		}
	}

	return nil
}

// insertHistoryEntry inserts a single history entry
func (tx *Tx) insertHistoryEntry(ctx context.Context, historyTable string, entityID int64, fieldName string, oldValue, newValue interface{}) error {
	// Convert values to JSON strings for storage
	oldValueStr, err := jsonString(oldValue)
	if err != nil {
		return fmt.Errorf("failed to serialize old value: %w", err)
	}

	newValueStr, err := jsonString(newValue)
	if err != nil {
		return fmt.Errorf("failed to serialize new value: %w", err)
	}

	// Insert the history entry
	query := fmt.Sprintf(`
		INSERT INTO %s (
			entity_id,
			field_name,
			old_value,
			new_value,
			changed_at
		) VALUES (
			$1, $2, $3, $4, NOW()
		)`, historyTable)

	if _, err := tx.ExecContext(ctx, query, entityID, fieldName, oldValueStr, newValueStr); err != nil {
		return fmt.Errorf("failed to insert history entry: %w", err)
	}

	log.Debug().
		Int64("entityID", entityID).
		Str("field", fieldName).
		Str("oldValue", oldValueStr).
		Str("newValue", newValueStr).
		Msg("Recorded history entry")

	return nil
}

// GetHistory retrieves the history of changes for an entity
func (db *DB) GetHistory(ctx context.Context, entityType string, entityID int64, since time.Time) ([]HistoryEntry, error) {
	historyTable := fmt.Sprintf("autotask.%s_history", entityType)

	query := fmt.Sprintf(`
		SELECT
			history_id,
			entity_id,
			field_name,
			old_value,
			new_value,
			changed_at
		FROM %s
		WHERE
			entity_id = $1
			AND changed_at >= $2
		ORDER BY
			changed_at DESC`, historyTable)

	var entries []HistoryEntry
	if err := db.SelectContext(ctx, &entries, query, entityID, since); err != nil {
		return nil, fmt.Errorf("failed to get history: %w", err)
	}

	return entries, nil
}

// CleanupHistory removes history entries older than the specified retention period
func (db *DB) CleanupHistory(ctx context.Context, entityType string, retentionPeriod time.Duration) error {
	historyTable := fmt.Sprintf("autotask.%s_history", entityType)
	cutoff := time.Now().Add(-retentionPeriod)

	return db.WithTransaction(ctx, func(tx *Tx) error {
		// Delete old entries in batches to avoid long-running transactions
		for {
			query := fmt.Sprintf(`
				DELETE FROM %s
				WHERE changed_at < $1
				LIMIT 1000`, historyTable)

			result, err := tx.ExecContext(ctx, query, cutoff)
			if err != nil {
				return fmt.Errorf("failed to cleanup history: %w", err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			}

			log.Debug().
				Int64("rowsDeleted", rowsAffected).
				Str("entityType", entityType).
				Time("cutoff", cutoff).
				Msg("Cleaned up history entries")

			if rowsAffected < 1000 {
				break
			}
		}

		return nil
	})
}

// valueEquals compares two values for equality
func valueEquals(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Convert to JSON strings for comparison
	aStr, err := jsonString(a)
	if err != nil {
		return false
	}

	bStr, err := jsonString(b)
	if err != nil {
		return false
	}

	return aStr == bStr
}

// jsonString converts a value to its JSON string representation
func jsonString(v interface{}) (string, error) {
	if v == nil {
		return "null", nil
	}

	bytes, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

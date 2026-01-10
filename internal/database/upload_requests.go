package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	uploadRequestColumns  = 5
	maxSQLiteVariables    = 999
	maxUploadRequestBatch = maxSQLiteVariables / uploadRequestColumns
)

// UploadRequestEntry represents a queued upload request persisted in the database.
type UploadRequestEntry struct {
	ID        int64
	ScanID    *int64
	LocalPath string
	Priority  int
	TriggerID *int64
	CreatedAt time.Time
}

// CreateUploadRequest adds a new upload request to the durable queue.
func (db *db) CreateUploadRequest(req *UploadRequestEntry) error {
	now := time.Now()
	result, err := db.exec(`
		INSERT INTO upload_requests (scan_id, local_path, priority, trigger_id, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, req.ScanID, req.LocalPath, req.Priority, req.TriggerID, now)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get upload request id: %w", err)
	}
	req.ID = id
	req.CreatedAt = now
	return nil
}

// CreateUploadRequests adds multiple upload requests to the durable queue in batches.
func (db *db) CreateUploadRequests(reqs []*UploadRequestEntry) error {
	if len(reqs) == 0 {
		return nil
	}

	now := time.Now()
	for _, req := range reqs {
		req.CreatedAt = now
	}

	return db.transaction(func(tx *sql.Tx) error {
		for start := 0; start < len(reqs); start += maxUploadRequestBatch {
			end := start + maxUploadRequestBatch
			if end > len(reqs) {
				end = len(reqs)
			}
			batch := reqs[start:end]

			query := buildUploadRequestInsertQuery(len(batch))
			args := make([]any, 0, len(batch)*uploadRequestColumns)
			for _, req := range batch {
				args = append(args, req.ScanID, req.LocalPath, req.Priority, req.TriggerID, req.CreatedAt)
			}

			result, err := tx.Exec(query, args...)
			if err != nil {
				return fmt.Errorf("failed to create upload request batch: %w", err)
			}

			if lastID, err := result.LastInsertId(); err == nil {
				firstID := lastID - int64(len(batch)) + 1
				for i, req := range batch {
					req.ID = firstID + int64(i)
				}
			}
		}

		return nil
	})
}

// ListUploadRequests returns the oldest queued upload requests, up to the limit.
func (db *db) ListUploadRequests(limit int) ([]*UploadRequestEntry, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := db.query(`
		SELECT id, scan_id, local_path, priority, trigger_id, created_at
		FROM upload_requests
		ORDER BY id ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list upload requests: %w", err)
	}
	defer rows.Close()

	var requests []*UploadRequestEntry
	for rows.Next() {
		req := &UploadRequestEntry{}
		var scanID sql.NullInt64
		var triggerID sql.NullInt64
		if err := rows.Scan(&req.ID, &scanID, &req.LocalPath, &req.Priority, &triggerID, &req.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan upload request: %w", err)
		}
		req.ScanID = nullInt64ToPtr(scanID)
		req.TriggerID = nullInt64ToPtr(triggerID)
		requests = append(requests, req)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate upload requests: %w", err)
	}

	return requests, nil
}

func buildUploadRequestInsertQuery(count int) string {
	if count <= 0 {
		return ""
	}

	values := make([]string, count)
	for i := 0; i < count; i++ {
		values[i] = "(?, ?, ?, ?, ?)"
	}

	return fmt.Sprintf(
		"INSERT INTO upload_requests (scan_id, local_path, priority, trigger_id, created_at) VALUES %s",
		strings.Join(values, ","),
	)
}

// DeleteUploadRequests removes queued upload requests by id.
func (db *db) DeleteUploadRequests(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf("DELETE FROM upload_requests WHERE id IN (%s)", strings.Join(placeholders, ","))
	if _, err := db.exec(query, args...); err != nil {
		return fmt.Errorf("failed to delete upload requests: %w", err)
	}
	return nil
}

// CountUploadRequests returns the number of queued upload requests.
func (db *db) CountUploadRequests() (int, error) {
	var count int
	if err := db.queryRow("SELECT COUNT(*) FROM upload_requests").Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count upload requests: %w", err)
	}
	return count, nil
}

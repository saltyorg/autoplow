package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
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
func (db *DB) CreateUploadRequest(req *UploadRequestEntry) error {
	now := time.Now()
	result, err := db.Exec(`
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

// ListUploadRequests returns the oldest queued upload requests, up to the limit.
func (db *DB) ListUploadRequests(limit int) ([]*UploadRequestEntry, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := db.Query(`
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

// DeleteUploadRequests removes queued upload requests by id.
func (db *DB) DeleteUploadRequests(ids []int64) error {
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
	if _, err := db.Exec(query, args...); err != nil {
		return fmt.Errorf("failed to delete upload requests: %w", err)
	}
	return nil
}

// CountUploadRequests returns the number of queued upload requests.
func (db *DB) CountUploadRequests() (int, error) {
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM upload_requests").Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count upload requests: %w", err)
	}
	return count, nil
}

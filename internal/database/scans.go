package database

import (
	"database/sql"
	"fmt"
	"time"
)

// ScanStatus represents the status of a scan
type ScanStatus string

const (
	ScanStatusPending   ScanStatus = "pending"
	ScanStatusScanning  ScanStatus = "scanning"
	ScanStatusCompleted ScanStatus = "completed"
	ScanStatusRetry     ScanStatus = "retry"
	ScanStatusFailed    ScanStatus = "failed"
)

// Scan represents a pending or completed scan request
type Scan struct {
	ID          int64      `json:"id"`
	Path        string     `json:"path"`
	TriggerID   *int64     `json:"trigger_id,omitempty"`
	Priority    int        `json:"priority"`
	Status      ScanStatus `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	RetryCount  int        `json:"retry_count"`
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	TargetID    *int64     `json:"target_id,omitempty"`
	EventType   string     `json:"event_type,omitempty"`
}

// CreateScan creates a new scan record
func (db *DB) CreateScan(scan *Scan) error {
	result, err := db.Exec(`
		INSERT INTO scans (path, trigger_id, priority, status, created_at, retry_count, event_type)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, scan.Path, scan.TriggerID, scan.Priority, scan.Status, time.Now(), 0, scan.EventType)
	if err != nil {
		return fmt.Errorf("failed to create scan: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	scan.ID = id
	scan.CreatedAt = time.Now()

	return nil
}

// GetScan retrieves a scan by ID
func (db *DB) GetScan(id int64) (*Scan, error) {
	scan := &Scan{}
	var triggerID, targetID sql.NullInt64
	var startedAt, completedAt, nextRetryAt sql.NullTime
	var lastError, eventType sql.NullString

	err := db.QueryRow(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
		FROM scans WHERE id = ?
	`, id).Scan(&scan.ID, &scan.Path, &triggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get scan: %w", err)
	}

	scan.TriggerID = nullInt64ToPtr(triggerID)
	scan.TargetID = nullInt64ToPtr(targetID)
	scan.StartedAt = nullTimeToPtr(startedAt)
	scan.CompletedAt = nullTimeToPtr(completedAt)
	scan.NextRetryAt = nullTimeToPtr(nextRetryAt)
	scan.LastError = nullStringValue(lastError)
	scan.EventType = nullStringValue(eventType)

	return scan, nil
}

// ListPendingScans returns all pending scans and retry scans that are ready, ordered by priority and age
func (db *DB) ListPendingScans() ([]*Scan, error) {
	now := time.Now()
	rows, err := db.Query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
		FROM scans
		WHERE (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
		   OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
		ORDER BY priority DESC, created_at ASC
	`, ScanStatusPending, now, ScanStatusRetry, now)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending scans: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToScans(rows)
}

// ListPendingScansOlderThan returns pending scans older than the given duration
func (db *DB) ListPendingScansOlderThan(age time.Duration) ([]*Scan, error) {
	cutoff := time.Now().Add(-age)
	rows, err := db.Query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
		FROM scans
		WHERE status = ? AND created_at < ?
		ORDER BY priority DESC, created_at ASC
	`, ScanStatusPending, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending scans: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToScans(rows)
}

// ListRecentScans returns the most recent scans
func (db *DB) ListRecentScans(limit int) ([]*Scan, error) {
	rows, err := db.Query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
		FROM scans
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list recent scans: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToScans(rows)
}

// scanRowsToScans converts sql.Rows to a slice of Scan
func (db *DB) scanRowsToScans(rows *sql.Rows) ([]*Scan, error) {
	var scans []*Scan
	for rows.Next() {
		scan := &Scan{}
		var triggerID, targetID sql.NullInt64
		var startedAt, completedAt, nextRetryAt sql.NullTime
		var lastError, eventType sql.NullString

		if err := rows.Scan(&scan.ID, &scan.Path, &triggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		scan.TriggerID = nullInt64ToPtr(triggerID)
		scan.TargetID = nullInt64ToPtr(targetID)
		scan.StartedAt = nullTimeToPtr(startedAt)
		scan.CompletedAt = nullTimeToPtr(completedAt)
		scan.NextRetryAt = nullTimeToPtr(nextRetryAt)
		scan.LastError = nullStringValue(lastError)
		scan.EventType = nullStringValue(eventType)

		scans = append(scans, scan)
	}

	return scans, nil
}

// UpdateScanStatus updates the status of a scan
func (db *DB) UpdateScanStatus(id int64, status ScanStatus) error {
	var err error
	switch status {
	case ScanStatusScanning:
		_, err = db.Exec(`UPDATE scans SET status = ?, started_at = ? WHERE id = ?`, status, time.Now(), id)
	case ScanStatusCompleted:
		_, err = db.Exec(`UPDATE scans SET status = ?, completed_at = ? WHERE id = ?`, status, time.Now(), id)
	case ScanStatusFailed:
		_, err = db.Exec(`UPDATE scans SET status = ?, retry_count = retry_count + 1 WHERE id = ?`, status, id)
	default:
		_, err = db.Exec(`UPDATE scans SET status = ? WHERE id = ?`, status, id)
	}
	if err != nil {
		return fmt.Errorf("failed to update scan status: %w", err)
	}
	return nil
}

// UpdateScanPriority updates the priority of a pending scan
func (db *DB) UpdateScanPriority(id int64, priority int) error {
	_, err := db.Exec(`UPDATE scans SET priority = ? WHERE id = ? AND status = ?`, priority, id, ScanStatusPending)
	if err != nil {
		return fmt.Errorf("failed to update scan priority: %w", err)
	}
	return nil
}

// UpdateScanError updates the error message for a scan
func (db *DB) UpdateScanError(id int64, errMsg string) error {
	_, err := db.Exec(`UPDATE scans SET last_error = ?, status = ?, retry_count = retry_count + 1 WHERE id = ?`, errMsg, ScanStatusFailed, id)
	if err != nil {
		return fmt.Errorf("failed to update scan error: %w", err)
	}
	return nil
}

// ScheduleScanRetry schedules a scan for retry with exponential backoff
// Returns true if the scan was scheduled for retry, false if max retries exceeded
func (db *DB) ScheduleScanRetry(id int64, errMsg string, maxRetries int) (bool, error) {
	// Get current retry count
	scan, err := db.GetScan(id)
	if err != nil {
		return false, fmt.Errorf("failed to get scan for retry: %w", err)
	}
	if scan == nil {
		return false, fmt.Errorf("scan not found: %d", id)
	}

	newRetryCount := scan.RetryCount + 1

	// Check if max retries exceeded
	if newRetryCount >= maxRetries {
		_, err := db.Exec(`UPDATE scans SET status = ?, last_error = ?, retry_count = ? WHERE id = ?`,
			ScanStatusFailed, errMsg, newRetryCount, id)
		if err != nil {
			return false, fmt.Errorf("failed to mark scan as failed: %w", err)
		}
		return false, nil
	}

	// Calculate exponential backoff: 2^(retry_count+1) seconds
	// retry 0 -> 2s, retry 1 -> 4s, retry 2 -> 8s, retry 3 -> 16s, retry 4 -> 32s
	backoffSeconds := 1 << (newRetryCount + 1) // 2^(n+1)
	nextRetryAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	_, err = db.Exec(`UPDATE scans SET status = ?, last_error = ?, retry_count = ?, next_retry_at = ? WHERE id = ?`,
		ScanStatusRetry, errMsg, newRetryCount, nextRetryAt, id)
	if err != nil {
		return false, fmt.Errorf("failed to schedule scan retry: %w", err)
	}

	return true, nil
}

// SetScanPathNotFoundRetry updates retry count and schedules next check for path-not-found errors
// The scan stays in pending status but won't be picked up until next_retry_at
func (db *DB) SetScanPathNotFoundRetry(id int64, retryCount int, nextRetryAt time.Time) error {
	_, err := db.Exec(`UPDATE scans SET retry_count = ?, next_retry_at = ?, last_error = ? WHERE id = ?`,
		retryCount, nextRetryAt, "path does not exist", id)
	if err != nil {
		return fmt.Errorf("failed to set path not found retry: %w", err)
	}
	return nil
}

// SetScanTarget assigns a target to a scan
func (db *DB) SetScanTarget(id int64, targetID int64) error {
	_, err := db.Exec(`UPDATE scans SET target_id = ? WHERE id = ?`, targetID, id)
	if err != nil {
		return fmt.Errorf("failed to set scan target: %w", err)
	}
	return nil
}

// FindDuplicatePendingScan checks if a pending or retry scan for the same path exists
func (db *DB) FindDuplicatePendingScan(path string) (*Scan, error) {
	scan := &Scan{}
	var triggerID, targetID sql.NullInt64
	var startedAt, completedAt, nextRetryAt sql.NullTime
	var lastError, eventType sql.NullString

	err := db.QueryRow(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
		FROM scans
		WHERE path = ? AND status IN (?, ?)
		ORDER BY created_at DESC
		LIMIT 1
	`, path, ScanStatusPending, ScanStatusRetry).Scan(&scan.ID, &scan.Path, &triggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find duplicate scan: %w", err)
	}

	scan.TriggerID = nullInt64ToPtr(triggerID)
	scan.TargetID = nullInt64ToPtr(targetID)
	scan.StartedAt = nullTimeToPtr(startedAt)
	scan.CompletedAt = nullTimeToPtr(completedAt)
	scan.NextRetryAt = nullTimeToPtr(nextRetryAt)
	scan.LastError = nullStringValue(lastError)
	scan.EventType = nullStringValue(eventType)

	return scan, nil
}

// CountPendingScans returns the number of pending and retry scans
func (db *DB) CountPendingScans() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM scans WHERE status IN (?, ?)", ScanStatusPending, ScanStatusRetry).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count pending scans: %w", err)
	}
	return count, nil
}

// DeleteOldScans deletes scans older than the given duration
func (db *DB) DeleteOldScans(age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	result, err := db.Exec(`DELETE FROM scans WHERE created_at < ? AND status IN (?, ?)`, cutoff, ScanStatusCompleted, ScanStatusFailed)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old scans: %w", err)
	}
	return result.RowsAffected()
}

// ListScansFiltered returns scans with filtering and pagination
func (db *DB) ListScansFiltered(status string, limit, offset int) ([]*Scan, error) {
	var rows *sql.Rows
	var err error

	if status != "" && status != "all" {
		rows, err = db.Query(`
			SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
			FROM scans
			WHERE status = ?
			ORDER BY created_at DESC
			LIMIT ? OFFSET ?
		`, status, limit, offset)
	} else {
		rows, err = db.Query(`
			SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type
			FROM scans
			ORDER BY created_at DESC
			LIMIT ? OFFSET ?
		`, limit, offset)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list scans: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToScans(rows)
}

// CountScansFiltered returns the total count of scans with optional status filter
func (db *DB) CountScansFiltered(status string) (int, error) {
	var count int
	var err error

	if status != "" && status != "all" {
		err = db.QueryRow("SELECT COUNT(*) FROM scans WHERE status = ?", status).Scan(&count)
	} else {
		err = db.QueryRow("SELECT COUNT(*) FROM scans").Scan(&count)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to count scans: %w", err)
	}
	return count, nil
}

// ResetScanningScans resets any scans with 'scanning' status back to 'pending'
// This is used on startup to recover scans that were interrupted during shutdown
func (db *DB) ResetScanningScans() (int64, error) {
	result, err := db.Exec(`UPDATE scans SET status = ?, started_at = NULL WHERE status = ?`,
		ScanStatusPending, ScanStatusScanning)
	if err != nil {
		return 0, fmt.Errorf("failed to reset scanning scans: %w", err)
	}
	return result.RowsAffected()
}

// GetScanStatsByStatus returns count of scans grouped by status
func (db *DB) GetScanStatsByStatus() (map[string]int, error) {
	stats := make(map[string]int)

	rows, err := db.Query(`SELECT status, COUNT(*) FROM scans GROUP BY status`)
	if err != nil {
		return stats, fmt.Errorf("failed to get scan stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err == nil {
			stats[status] = count
		}
	}
	return stats, nil
}

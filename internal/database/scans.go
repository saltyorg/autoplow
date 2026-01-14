package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// ScanStatus represents the status of a scan
type ScanStatus string

const (
	ScanStatusPending   ScanStatus = "pending"
	ScanStatusScanning  ScanStatus = "scanning"
	ScanStatusCompleted ScanStatus = "completed"
	ScanStatusSkipped   ScanStatus = "skipped"
	ScanStatusRetry     ScanStatus = "retry" // legacy status from removed retry scheduling
	ScanStatusFailed    ScanStatus = "failed"
)

// Scan represents a pending or completed scan request
type Scan struct {
	ID          int64      `json:"id"`
	Path        string     `json:"path"`
	TriggerPath string     `json:"trigger_path,omitempty"`
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
	FilePaths   []string   `json:"file_paths,omitempty"` // Original file paths that triggered this scan (for upload queuing)
}

// CreateScan creates a new scan record
func (db *db) CreateScan(scan *Scan) error {
	var filePathsJSON *string
	if len(scan.FilePaths) > 0 {
		data, err := json.Marshal(scan.FilePaths)
		if err != nil {
			return fmt.Errorf("failed to marshal file paths: %w", err)
		}
		s := string(data)
		filePathsJSON = &s
	}

	triggerPath := scan.TriggerPath
	if triggerPath == "" {
		triggerPath = scan.Path
	}

	result, err := db.exec(`
		INSERT INTO scans (path, trigger_path, trigger_id, priority, status, created_at, retry_count, event_type, file_paths)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, scan.Path, triggerPath, scan.TriggerID, scan.Priority, scan.Status, time.Now(), 0, scan.EventType, filePathsJSON)
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
func (db *db) GetScan(id int64) (*Scan, error) {
	scan := &Scan{}
	var triggerID, targetID sql.NullInt64
	var startedAt, completedAt, nextRetryAt sql.NullTime
	var lastError, eventType, filePathsJSON, triggerPath sql.NullString

	err := db.queryRow(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
		FROM scans WHERE id = ?
	`, id).Scan(&scan.ID, &scan.Path, &triggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType, &filePathsJSON, &triggerPath)
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
	scan.TriggerPath = nullStringValue(triggerPath)
	if filePathsJSON.Valid && filePathsJSON.String != "" {
		_ = json.Unmarshal([]byte(filePathsJSON.String), &scan.FilePaths)
	}

	return scan, nil
}

// ListPendingScans returns all pending scans ordered by priority and age
func (db *db) ListPendingScans() ([]*Scan, error) {
	rows, err := db.query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
		FROM scans
		WHERE status = ?
		ORDER BY priority DESC, created_at ASC
	`, ScanStatusPending)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending scans: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToScans(rows)
}

// ListPendingScansOlderThan returns pending scans older than the given duration
func (db *db) ListPendingScansOlderThan(age time.Duration) ([]*Scan, error) {
	cutoff := time.Now().Add(-age)
	rows, err := db.query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
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
func (db *db) ListRecentScans(limit int) ([]*Scan, error) {
	rows, err := db.query(`
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
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
func (db *db) scanRowsToScans(rows *sql.Rows) ([]*Scan, error) {
	var scans []*Scan
	for rows.Next() {
		scan := &Scan{}
		var triggerID, targetID sql.NullInt64
		var startedAt, completedAt, nextRetryAt sql.NullTime
		var lastError, eventType, filePathsJSON, triggerPath sql.NullString

		if err := rows.Scan(&scan.ID, &scan.Path, &triggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType, &filePathsJSON, &triggerPath); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		scan.TriggerID = nullInt64ToPtr(triggerID)
		scan.TargetID = nullInt64ToPtr(targetID)
		scan.StartedAt = nullTimeToPtr(startedAt)
		scan.CompletedAt = nullTimeToPtr(completedAt)
		scan.NextRetryAt = nullTimeToPtr(nextRetryAt)
		scan.LastError = nullStringValue(lastError)
		scan.EventType = nullStringValue(eventType)
		scan.TriggerPath = nullStringValue(triggerPath)
		if filePathsJSON.Valid && filePathsJSON.String != "" {
			_ = json.Unmarshal([]byte(filePathsJSON.String), &scan.FilePaths)
		}

		scans = append(scans, scan)
	}

	return scans, nil
}

// UpdateScanStatus updates the status of a scan
func (db *db) UpdateScanStatus(id int64, status ScanStatus) error {
	var err error
	switch status {
	case ScanStatusScanning:
		_, err = db.exec(`UPDATE scans SET status = ?, started_at = ? WHERE id = ?`, status, time.Now(), id)
	case ScanStatusCompleted, ScanStatusSkipped:
		_, err = db.exec(`UPDATE scans SET status = ?, completed_at = ? WHERE id = ?`, status, time.Now(), id)
	default:
		_, err = db.exec(`UPDATE scans SET status = ? WHERE id = ?`, status, id)
	}
	if err != nil {
		return fmt.Errorf("failed to update scan status: %w", err)
	}
	return nil
}

// UpdateScanPriority updates the priority of a pending scan
func (db *db) UpdateScanPriority(id int64, priority int) error {
	_, err := db.exec(`UPDATE scans SET priority = ? WHERE id = ? AND status = ?`, priority, id, ScanStatusPending)
	if err != nil {
		return fmt.Errorf("failed to update scan priority: %w", err)
	}
	return nil
}

// UpdateScanTriggerPath updates the stored trigger path for a scan.
func (db *db) UpdateScanTriggerPath(id int64, triggerPath string) error {
	_, err := db.exec(`UPDATE scans SET trigger_path = ? WHERE id = ?`, triggerPath, id)
	if err != nil {
		return fmt.Errorf("failed to update scan trigger path: %w", err)
	}
	return nil
}

// UpdateScanError updates the error message for a scan
func (db *db) UpdateScanError(id int64, errMsg string) error {
	_, err := db.exec(`UPDATE scans SET last_error = ?, status = ? WHERE id = ?`, errMsg, ScanStatusFailed, id)
	if err != nil {
		return fmt.Errorf("failed to update scan error: %w", err)
	}
	return nil
}

// SetScanTarget assigns a target to a scan
func (db *db) SetScanTarget(id int64, targetID int64) error {
	_, err := db.exec(`UPDATE scans SET target_id = ? WHERE id = ?`, targetID, id)
	if err != nil {
		return fmt.Errorf("failed to set scan target: %w", err)
	}
	return nil
}

// FindDuplicatePendingScan checks if a pending scan exists for the same path and trigger.
func (db *db) FindDuplicatePendingScan(path string, triggerID *int64) (*Scan, error) {
	scan := &Scan{}
	var scanTriggerID, targetID sql.NullInt64
	var startedAt, completedAt, nextRetryAt sql.NullTime
	var lastError, eventType, filePathsJSON sql.NullString

	query := `
		SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
		FROM scans
		WHERE path = ? AND status = ? AND `
	args := []any{path, ScanStatusPending}
	if triggerID == nil {
		query += "trigger_id IS NULL"
	} else {
		query += "trigger_id = ?"
		args = append(args, *triggerID)
	}
	query += `
		ORDER BY created_at DESC
		LIMIT 1
	`
	var triggerPath sql.NullString
	err := db.queryRow(query, args...).Scan(&scan.ID, &scan.Path, &scanTriggerID, &scan.Priority, &scan.Status, &scan.CreatedAt, &startedAt, &completedAt, &scan.RetryCount, &nextRetryAt, &lastError, &targetID, &eventType, &filePathsJSON, &triggerPath)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find duplicate scan: %w", err)
	}

	scan.TriggerID = nullInt64ToPtr(scanTriggerID)
	scan.TargetID = nullInt64ToPtr(targetID)
	scan.StartedAt = nullTimeToPtr(startedAt)
	scan.CompletedAt = nullTimeToPtr(completedAt)
	scan.NextRetryAt = nullTimeToPtr(nextRetryAt)
	scan.LastError = nullStringValue(lastError)
	scan.EventType = nullStringValue(eventType)
	scan.TriggerPath = nullStringValue(triggerPath)
	if filePathsJSON.Valid && filePathsJSON.String != "" {
		_ = json.Unmarshal([]byte(filePathsJSON.String), &scan.FilePaths)
	}

	return scan, nil
}

// CountPendingScans returns the number of pending scans
func (db *db) CountPendingScans() (int, error) {
	var count int
	err := db.queryRow("SELECT COUNT(*) FROM scans WHERE status = ?", ScanStatusPending).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count pending scans: %w", err)
	}
	return count, nil
}

// DeleteScan deletes a scan by ID.
func (db *db) DeleteScan(id int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM scans WHERE id = ?", id); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("scan not found")
		}
		return fmt.Errorf("failed to delete scan: %w", err)
	}
	return nil
}

// ClearScanHistory deletes all scan history records.
func (db *db) ClearScanHistory() (int64, error) {
	result, err := db.exec("DELETE FROM scans")
	if err != nil {
		return 0, fmt.Errorf("failed to clear scan history: %w", err)
	}
	return result.RowsAffected()
}

// DeleteOldScans deletes scans older than the given duration
func (db *db) DeleteOldScans(age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	result, err := db.exec(`DELETE FROM scans WHERE created_at < ? AND status IN (?, ?, ?, ?)`, cutoff, ScanStatusCompleted, ScanStatusSkipped, ScanStatusFailed, ScanStatusRetry)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old scans: %w", err)
	}
	return result.RowsAffected()
}

// ListScansFiltered returns scans with filtering and pagination
func (db *db) ListScansFiltered(status string, limit, offset int) ([]*Scan, error) {
	var rows *sql.Rows
	var err error

	if status != "" && status != "all" {
		switch status {
		case string(ScanStatusPending):
			rows, err = db.query(`
				SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
				FROM scans
				WHERE status = ?
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
			`, ScanStatusPending, limit, offset)
		case string(ScanStatusFailed):
			rows, err = db.query(`
				SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
				FROM scans
				WHERE status IN (?, ?)
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
			`, ScanStatusFailed, ScanStatusRetry, limit, offset)
		default:
			rows, err = db.query(`
				SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
				FROM scans
				WHERE status = ?
				ORDER BY created_at DESC
				LIMIT ? OFFSET ?
			`, status, limit, offset)
		}
	} else {
		rows, err = db.query(`
			SELECT id, path, trigger_id, priority, status, created_at, started_at, completed_at, retry_count, next_retry_at, last_error, target_id, event_type, file_paths, trigger_path
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
func (db *db) CountScansFiltered(status string) (int, error) {
	var count int
	var err error

	if status != "" && status != "all" {
		switch status {
		case string(ScanStatusPending):
			err = db.queryRow("SELECT COUNT(*) FROM scans WHERE status = ?", ScanStatusPending).Scan(&count)
		case string(ScanStatusFailed):
			err = db.queryRow("SELECT COUNT(*) FROM scans WHERE status IN (?, ?)", ScanStatusFailed, ScanStatusRetry).Scan(&count)
		default:
			err = db.queryRow("SELECT COUNT(*) FROM scans WHERE status = ?", status).Scan(&count)
		}
	} else {
		err = db.queryRow("SELECT COUNT(*) FROM scans").Scan(&count)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to count scans: %w", err)
	}
	return count, nil
}

// MarkRetryScansFailed converts legacy retry scans into failed scans.
func (db *db) MarkRetryScansFailed() (int64, error) {
	result, err := db.exec(`UPDATE scans SET status = ?, next_retry_at = NULL WHERE status = ?`, ScanStatusFailed, ScanStatusRetry)
	if err != nil {
		return 0, fmt.Errorf("failed to mark retry scans as failed: %w", err)
	}
	return result.RowsAffected()
}

// ResetScanningScans resets any scans with 'scanning' status back to 'pending'
// This is used on startup to recover scans that were interrupted during shutdown
func (db *db) ResetScanningScans() (int64, error) {
	result, err := db.exec(`UPDATE scans SET status = ?, started_at = NULL WHERE status = ?`,
		ScanStatusPending, ScanStatusScanning)
	if err != nil {
		return 0, fmt.Errorf("failed to reset scanning scans: %w", err)
	}
	return result.RowsAffected()
}

// GetScanStatsByStatus returns count of scans grouped by status
func (db *db) GetScanStatsByStatus() (map[string]int, error) {
	stats := make(map[string]int)

	rows, err := db.query(`SELECT status, COUNT(*) FROM scans GROUP BY status`)
	if err != nil {
		return stats, fmt.Errorf("failed to get scan stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err == nil {
			if status == string(ScanStatusRetry) {
				stats[string(ScanStatusFailed)] += count
				continue
			}
			stats[status] = count
		}
	}
	return stats, nil
}

// AppendScanFilePaths appends new file paths to an existing scan's file_paths
// This is used when a duplicate scan request comes in with additional files
func (db *db) AppendScanFilePaths(scanID int64, newPaths []string) error {
	if len(newPaths) == 0 {
		return nil
	}

	// Get current file paths
	var filePathsJSON sql.NullString
	err := db.queryRow(`SELECT file_paths FROM scans WHERE id = ?`, scanID).Scan(&filePathsJSON)
	if err != nil {
		return fmt.Errorf("failed to get current file paths: %w", err)
	}

	// Parse existing paths
	var existingPaths []string
	if filePathsJSON.Valid && filePathsJSON.String != "" {
		if err := json.Unmarshal([]byte(filePathsJSON.String), &existingPaths); err != nil {
			return fmt.Errorf("failed to unmarshal existing file paths: %w", err)
		}
	}

	// Create set for deduplication
	pathSet := make(map[string]struct{})
	for _, p := range existingPaths {
		pathSet[p] = struct{}{}
	}

	// Add new paths that don't already exist
	for _, p := range newPaths {
		if _, exists := pathSet[p]; !exists {
			existingPaths = append(existingPaths, p)
			pathSet[p] = struct{}{}
		}
	}

	// Marshal and update
	data, err := json.Marshal(existingPaths)
	if err != nil {
		return fmt.Errorf("failed to marshal file paths: %w", err)
	}

	_, err = db.exec(`UPDATE scans SET file_paths = ? WHERE id = ?`, string(data), scanID)
	if err != nil {
		return fmt.Errorf("failed to update file paths: %w", err)
	}

	return nil
}

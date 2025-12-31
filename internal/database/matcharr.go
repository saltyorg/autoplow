package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// ArrType represents the type of Arr instance
type ArrType string

const (
	ArrTypeSonarr ArrType = "sonarr"
	ArrTypeRadarr ArrType = "radarr"
)

// MatcharrArr represents a Sonarr/Radarr instance configuration
type MatcharrArr struct {
	ID           int64                 `json:"id"`
	Name         string                `json:"name"`
	Type         ArrType               `json:"type"`
	URL          string                `json:"url"`
	APIKey       string                `json:"api_key"`
	Enabled      bool                  `json:"enabled"`
	PathMappings []MatcharrPathMapping `json:"path_mappings,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

// MatcharrPathMapping represents a path mapping between Arr and media server
type MatcharrPathMapping struct {
	ArrPath    string `json:"arr_path"`    // Path as seen by Arr
	ServerPath string `json:"server_path"` // Path as seen by media server
}

// MatcharrRunStatus represents the status of a matcharr run
type MatcharrRunStatus string

const (
	MatcharrRunStatusRunning   MatcharrRunStatus = "running"
	MatcharrRunStatusCompleted MatcharrRunStatus = "completed"
	MatcharrRunStatusFailed    MatcharrRunStatus = "failed"
)

// MatcharrRun represents a single run of the matcharr comparison
type MatcharrRun struct {
	ID              int64             `json:"id"`
	StartedAt       time.Time         `json:"started_at"`
	CompletedAt     *time.Time        `json:"completed_at,omitempty"`
	Status          MatcharrRunStatus `json:"status"`
	TotalCompared   int               `json:"total_compared"`
	MismatchesFound int               `json:"mismatches_found"`
	MismatchesFixed int               `json:"mismatches_fixed"`
	Error           string            `json:"error,omitempty"`
	TriggeredBy     string            `json:"triggered_by"` // 'manual' or 'schedule'
	Logs            string            `json:"logs,omitempty"`
}

// MatcharrMismatchStatus represents the status of a detected mismatch
type MatcharrMismatchStatus string

const (
	MatcharrMismatchStatusPending MatcharrMismatchStatus = "pending"
	MatcharrMismatchStatusFixed   MatcharrMismatchStatus = "fixed"
	MatcharrMismatchStatusSkipped MatcharrMismatchStatus = "skipped"
	MatcharrMismatchStatusFailed  MatcharrMismatchStatus = "failed"
)

// MatcharrMismatch represents a detected mismatch between Arr and media server
type MatcharrMismatch struct {
	ID               int64                  `json:"id"`
	RunID            int64                  `json:"run_id"`
	ArrID            int64                  `json:"arr_id"`
	TargetID         int64                  `json:"target_id"`
	ArrType          ArrType                `json:"arr_type"`
	ArrName          string                 `json:"arr_name"`
	TargetName       string                 `json:"target_name"`
	MediaTitle       string                 `json:"media_title"`
	MediaPath        string                 `json:"media_path"`
	ArrIDType        string                 `json:"arr_id_type"` // 'tmdb', 'tvdb', 'imdb'
	ArrIDValue       string                 `json:"arr_id_value"`
	TargetIDType     string                 `json:"target_id_type"`
	TargetIDValue    string                 `json:"target_id_value,omitempty"`
	TargetMetadataID string                 `json:"target_metadata_id"`
	Status           MatcharrMismatchStatus `json:"status"`
	FixedAt          *time.Time             `json:"fixed_at,omitempty"`
	Error            string                 `json:"error,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
}

// CreateMatcharrArr creates a new Arr instance
func (db *DB) CreateMatcharrArr(arr *MatcharrArr) error {
	pathMappingsJSON, err := json.Marshal(arr.PathMappings)
	if err != nil {
		return fmt.Errorf("failed to marshal path mappings: %w", err)
	}

	result, err := db.Exec(`
		INSERT INTO matcharr_arrs (name, type, url, api_key, enabled, path_mappings, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, arr.Name, arr.Type, arr.URL, arr.APIKey, arr.Enabled, string(pathMappingsJSON))
	if err != nil {
		return fmt.Errorf("failed to create matcharr arr: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	arr.ID = id

	return nil
}

// GetMatcharrArr retrieves an Arr instance by ID
func (db *DB) GetMatcharrArr(id int64) (*MatcharrArr, error) {
	var arr MatcharrArr
	var pathMappingsJSON string
	var enabled int

	err := db.QueryRow(`
		SELECT id, name, type, url, api_key, enabled, path_mappings, created_at, updated_at
		FROM matcharr_arrs WHERE id = ?
	`, id).Scan(
		&arr.ID, &arr.Name, &arr.Type, &arr.URL, &arr.APIKey,
		&enabled, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr arr: %w", err)
	}

	arr.Enabled = enabled != 0

	if pathMappingsJSON != "" {
		if err := json.Unmarshal([]byte(pathMappingsJSON), &arr.PathMappings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
		}
	}

	return &arr, nil
}

// ListMatcharrArrs retrieves all Arr instances
func (db *DB) ListMatcharrArrs() ([]*MatcharrArr, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, api_key, enabled, path_mappings, created_at, updated_at
		FROM matcharr_arrs ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr arrs: %w", err)
	}
	defer rows.Close()

	var arrs []*MatcharrArr
	for rows.Next() {
		var arr MatcharrArr
		var pathMappingsJSON string
		var enabled int

		if err := rows.Scan(
			&arr.ID, &arr.Name, &arr.Type, &arr.URL, &arr.APIKey,
			&enabled, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr arr: %w", err)
		}

		arr.Enabled = enabled != 0

		if pathMappingsJSON != "" {
			if err := json.Unmarshal([]byte(pathMappingsJSON), &arr.PathMappings); err != nil {
				return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
			}
		}

		arrs = append(arrs, &arr)
	}

	return arrs, rows.Err()
}

// ListEnabledMatcharrArrs retrieves all enabled Arr instances
func (db *DB) ListEnabledMatcharrArrs() ([]*MatcharrArr, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, api_key, enabled, path_mappings, created_at, updated_at
		FROM matcharr_arrs WHERE enabled = 1 ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled matcharr arrs: %w", err)
	}
	defer rows.Close()

	var arrs []*MatcharrArr
	for rows.Next() {
		var arr MatcharrArr
		var pathMappingsJSON string
		var enabled int

		if err := rows.Scan(
			&arr.ID, &arr.Name, &arr.Type, &arr.URL, &arr.APIKey,
			&enabled, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr arr: %w", err)
		}

		arr.Enabled = enabled != 0

		if pathMappingsJSON != "" {
			if err := json.Unmarshal([]byte(pathMappingsJSON), &arr.PathMappings); err != nil {
				return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
			}
		}

		arrs = append(arrs, &arr)
	}

	return arrs, rows.Err()
}

// UpdateMatcharrArr updates an Arr instance
func (db *DB) UpdateMatcharrArr(arr *MatcharrArr) error {
	pathMappingsJSON, err := json.Marshal(arr.PathMappings)
	if err != nil {
		return fmt.Errorf("failed to marshal path mappings: %w", err)
	}

	_, err = db.Exec(`
		UPDATE matcharr_arrs
		SET name = ?, type = ?, url = ?, api_key = ?, enabled = ?, path_mappings = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, arr.Name, arr.Type, arr.URL, arr.APIKey, arr.Enabled, string(pathMappingsJSON), arr.ID)
	if err != nil {
		return fmt.Errorf("failed to update matcharr arr: %w", err)
	}

	return nil
}

// DeleteMatcharrArr deletes an Arr instance
func (db *DB) DeleteMatcharrArr(id int64) error {
	_, err := db.Exec(`DELETE FROM matcharr_arrs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("failed to delete matcharr arr: %w", err)
	}
	return nil
}

// CreateMatcharrRun creates a new matcharr run
func (db *DB) CreateMatcharrRun(run *MatcharrRun) error {
	result, err := db.Exec(`
		INSERT INTO matcharr_runs (started_at, status, total_compared, mismatches_found, mismatches_fixed, triggered_by)
		VALUES (?, ?, ?, ?, ?, ?)
	`, run.StartedAt, run.Status, run.TotalCompared, run.MismatchesFound, run.MismatchesFixed, run.TriggeredBy)
	if err != nil {
		return fmt.Errorf("failed to create matcharr run: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	run.ID = id

	return nil
}

// UpdateMatcharrRun updates a matcharr run
func (db *DB) UpdateMatcharrRun(run *MatcharrRun) error {
	_, err := db.Exec(`
		UPDATE matcharr_runs
		SET completed_at = ?, status = ?, total_compared = ?, mismatches_found = ?, mismatches_fixed = ?, error = ?, logs = ?
		WHERE id = ?
	`, run.CompletedAt, run.Status, run.TotalCompared, run.MismatchesFound, run.MismatchesFixed, run.Error, run.Logs, run.ID)
	if err != nil {
		return fmt.Errorf("failed to update matcharr run: %w", err)
	}
	return nil
}

// GetMatcharrRun retrieves a matcharr run by ID
func (db *DB) GetMatcharrRun(id int64) (*MatcharrRun, error) {
	var run MatcharrRun
	var completedAt sql.NullTime
	var errorStr sql.NullString
	var logs sql.NullString

	err := db.QueryRow(`
		SELECT id, started_at, completed_at, status, total_compared, mismatches_found, mismatches_fixed, error, triggered_by, logs
		FROM matcharr_runs WHERE id = ?
	`, id).Scan(
		&run.ID, &run.StartedAt, &completedAt, &run.Status,
		&run.TotalCompared, &run.MismatchesFound, &run.MismatchesFixed,
		&errorStr, &run.TriggeredBy, &logs,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr run: %w", err)
	}

	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errorStr.Valid {
		run.Error = errorStr.String
	}
	if logs.Valid {
		run.Logs = logs.String
	}

	return &run, nil
}

// GetLatestMatcharrRun retrieves the most recent matcharr run
func (db *DB) GetLatestMatcharrRun() (*MatcharrRun, error) {
	var run MatcharrRun
	var completedAt sql.NullTime
	var errorStr sql.NullString

	err := db.QueryRow(`
		SELECT id, started_at, completed_at, status, total_compared, mismatches_found, mismatches_fixed, error, triggered_by
		FROM matcharr_runs ORDER BY started_at DESC LIMIT 1
	`).Scan(
		&run.ID, &run.StartedAt, &completedAt, &run.Status,
		&run.TotalCompared, &run.MismatchesFound, &run.MismatchesFixed,
		&errorStr, &run.TriggeredBy,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest matcharr run: %w", err)
	}

	if completedAt.Valid {
		run.CompletedAt = &completedAt.Time
	}
	if errorStr.Valid {
		run.Error = errorStr.String
	}

	return &run, nil
}

// ListMatcharrRuns retrieves matcharr runs with pagination
func (db *DB) ListMatcharrRuns(limit, offset int) ([]*MatcharrRun, error) {
	rows, err := db.Query(`
		SELECT id, started_at, completed_at, status, total_compared, mismatches_found, mismatches_fixed, error, triggered_by
		FROM matcharr_runs ORDER BY started_at DESC LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr runs: %w", err)
	}
	defer rows.Close()

	var runs []*MatcharrRun
	for rows.Next() {
		var run MatcharrRun
		var completedAt sql.NullTime
		var errorStr sql.NullString

		if err := rows.Scan(
			&run.ID, &run.StartedAt, &completedAt, &run.Status,
			&run.TotalCompared, &run.MismatchesFound, &run.MismatchesFixed,
			&errorStr, &run.TriggeredBy,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr run: %w", err)
		}

		if completedAt.Valid {
			run.CompletedAt = &completedAt.Time
		}
		if errorStr.Valid {
			run.Error = errorStr.String
		}

		runs = append(runs, &run)
	}

	return runs, rows.Err()
}

// CountMatcharrRuns returns the total number of matcharr runs
func (db *DB) CountMatcharrRuns() (int, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM matcharr_runs`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count matcharr runs: %w", err)
	}
	return count, nil
}

// CreateMatcharrMismatch creates a new mismatch record
func (db *DB) CreateMatcharrMismatch(mismatch *MatcharrMismatch) error {
	result, err := db.Exec(`
		INSERT INTO matcharr_mismatches (
			run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`,
		mismatch.RunID, mismatch.ArrID, mismatch.TargetID, mismatch.ArrType, mismatch.ArrName, mismatch.TargetName,
		mismatch.MediaTitle, mismatch.MediaPath, mismatch.ArrIDType, mismatch.ArrIDValue,
		mismatch.TargetIDType, mismatch.TargetIDValue, mismatch.TargetMetadataID, mismatch.Status,
	)
	if err != nil {
		return fmt.Errorf("failed to create matcharr mismatch: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	mismatch.ID = id

	return nil
}

// UpdateMatcharrMismatchStatus updates the status of a mismatch
func (db *DB) UpdateMatcharrMismatchStatus(id int64, status MatcharrMismatchStatus, errorMsg string) error {
	var fixedAt any
	if status == MatcharrMismatchStatusFixed {
		now := time.Now()
		fixedAt = now
	}

	_, err := db.Exec(`
		UPDATE matcharr_mismatches SET status = ?, fixed_at = ?, error = ? WHERE id = ?
	`, status, fixedAt, errorMsg, id)
	if err != nil {
		return fmt.Errorf("failed to update matcharr mismatch status: %w", err)
	}
	return nil
}

// GetMatcharrMismatch retrieves a mismatch by ID
func (db *DB) GetMatcharrMismatch(id int64) (*MatcharrMismatch, error) {
	var mismatch MatcharrMismatch
	var fixedAt sql.NullTime
	var errorStr sql.NullString
	var targetIDValue sql.NullString

	err := db.QueryRow(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE id = ?
	`, id).Scan(
		&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID,
		&mismatch.ArrType, &mismatch.ArrName, &mismatch.TargetName,
		&mismatch.MediaTitle, &mismatch.MediaPath, &mismatch.ArrIDType, &mismatch.ArrIDValue,
		&mismatch.TargetIDType, &targetIDValue, &mismatch.TargetMetadataID,
		&mismatch.Status, &fixedAt, &errorStr, &mismatch.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr mismatch: %w", err)
	}

	if fixedAt.Valid {
		mismatch.FixedAt = &fixedAt.Time
	}
	if errorStr.Valid {
		mismatch.Error = errorStr.String
	}
	if targetIDValue.Valid {
		mismatch.TargetIDValue = targetIDValue.String
	}

	return &mismatch, nil
}

// ListMatcharrMismatches retrieves mismatches for a run
func (db *DB) ListMatcharrMismatches(runID int64) ([]*MatcharrMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE run_id = ? ORDER BY media_title
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr mismatches: %w", err)
	}
	defer rows.Close()

	return scanMatcharrMismatches(rows)
}

// GetPendingMatcharrMismatches retrieves all pending mismatches for a run
func (db *DB) GetPendingMatcharrMismatches(runID int64) ([]*MatcharrMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE run_id = ? AND status = ? ORDER BY media_title
	`, runID, MatcharrMismatchStatusPending)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending matcharr mismatches: %w", err)
	}
	defer rows.Close()

	return scanMatcharrMismatches(rows)
}

// GetLatestPendingMatcharrMismatches retrieves pending mismatches from the latest run
func (db *DB) GetLatestPendingMatcharrMismatches() ([]*MatcharrMismatch, error) {
	latestRun, err := db.GetLatestMatcharrRun()
	if err != nil {
		return nil, err
	}
	if latestRun == nil {
		return nil, nil
	}
	return db.GetPendingMatcharrMismatches(latestRun.ID)
}

// GetActionableMatcharrMismatches retrieves pending and failed mismatches for a run
// (items that need attention - either pending action or failed and may need retry)
func (db *DB) GetActionableMatcharrMismatches(runID int64) ([]*MatcharrMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE run_id = ? AND status IN (?, ?) ORDER BY status DESC, media_title
	`, runID, MatcharrMismatchStatusPending, MatcharrMismatchStatusFailed)
	if err != nil {
		return nil, fmt.Errorf("failed to get actionable matcharr mismatches: %w", err)
	}
	defer rows.Close()

	return scanMatcharrMismatches(rows)
}

// GetLatestActionableMatcharrMismatches retrieves pending and failed mismatches from the latest run
func (db *DB) GetLatestActionableMatcharrMismatches() ([]*MatcharrMismatch, error) {
	latestRun, err := db.GetLatestMatcharrRun()
	if err != nil {
		return nil, err
	}
	if latestRun == nil {
		return nil, nil
	}
	return db.GetActionableMatcharrMismatches(latestRun.ID)
}

// GetMatcharrFixHistory retrieves fixed and skipped mismatches (most recent first)
func (db *DB) GetMatcharrFixHistory(limit, offset int) ([]*MatcharrMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			media_title, media_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches
		WHERE status IN (?, ?, ?)
		ORDER BY fixed_at DESC, created_at DESC
		LIMIT ? OFFSET ?
	`, MatcharrMismatchStatusFixed, MatcharrMismatchStatusSkipped, MatcharrMismatchStatusFailed, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr fix history: %w", err)
	}
	defer rows.Close()

	return scanMatcharrMismatches(rows)
}

// scanMatcharrMismatches is a helper to scan mismatch rows
func scanMatcharrMismatches(rows *sql.Rows) ([]*MatcharrMismatch, error) {
	var mismatches []*MatcharrMismatch
	for rows.Next() {
		var mismatch MatcharrMismatch
		var fixedAt sql.NullTime
		var errorStr sql.NullString
		var targetIDValue sql.NullString

		if err := rows.Scan(
			&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID,
			&mismatch.ArrType, &mismatch.ArrName, &mismatch.TargetName,
			&mismatch.MediaTitle, &mismatch.MediaPath, &mismatch.ArrIDType, &mismatch.ArrIDValue,
			&mismatch.TargetIDType, &targetIDValue, &mismatch.TargetMetadataID,
			&mismatch.Status, &fixedAt, &errorStr, &mismatch.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr mismatch: %w", err)
		}

		if fixedAt.Valid {
			mismatch.FixedAt = &fixedAt.Time
		}
		if errorStr.Valid {
			mismatch.Error = errorStr.String
		}
		if targetIDValue.Valid {
			mismatch.TargetIDValue = targetIDValue.String
		}

		mismatches = append(mismatches, &mismatch)
	}

	return mismatches, rows.Err()
}

// DeleteOldMatcharrRuns deletes runs older than the specified number of days
func (db *DB) DeleteOldMatcharrRuns(daysToKeep int) (int64, error) {
	result, err := db.Exec(`
		DELETE FROM matcharr_runs
		WHERE started_at < datetime('now', '-' || ? || ' days')
	`, daysToKeep)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old matcharr runs: %w", err)
	}
	return result.RowsAffected()
}

// IncrementMatcharrRunFixed increments the mismatches_fixed counter for a run
func (db *DB) IncrementMatcharrRunFixed(runID int64) error {
	_, err := db.Exec(`
		UPDATE matcharr_runs SET mismatches_fixed = mismatches_fixed + 1 WHERE id = ?
	`, runID)
	if err != nil {
		return fmt.Errorf("failed to increment matcharr run fixed count: %w", err)
	}
	return nil
}

// ClearMatcharrHistory deletes all matcharr runs and their associated mismatches
func (db *DB) ClearMatcharrHistory() error {
	// Delete mismatches first (foreign keys may not be enabled in SQLite)
	_, err := db.Exec(`DELETE FROM matcharr_mismatches`)
	if err != nil {
		return fmt.Errorf("failed to clear matcharr mismatches: %w", err)
	}

	// Then delete runs
	_, err = db.Exec(`DELETE FROM matcharr_runs`)
	if err != nil {
		return fmt.Errorf("failed to clear matcharr runs: %w", err)
	}
	return nil
}

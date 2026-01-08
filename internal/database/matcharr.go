package database

import (
	"database/sql"
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
	ID              int64                 `json:"id"`
	Name            string                `json:"name"`
	Type            ArrType               `json:"type"`
	URL             string                `json:"url"`
	APIKey          string                `json:"api_key"`
	Enabled         bool                  `json:"enabled"`
	FileConcurrency int                   `json:"file_concurrency"`
	PathMappings    []MatcharrPathMapping `json:"path_mappings,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
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
	TargetTitle      string                 `json:"target_title"`
	MediaTitle       string                 `json:"media_title"`
	MediaPath        string                 `json:"media_path"`
	TargetPath       string                 `json:"target_path"`
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

type MatcharrGapSource string

const (
	MatcharrGapSourceArr    MatcharrGapSource = "arr"    // Present in Arr, missing on server
	MatcharrGapSourceTarget MatcharrGapSource = "target" // Present on server, missing in Arr
)

// MatcharrGap represents a missing path detected during comparison
type MatcharrGap struct {
	ID         int64             `json:"id"`
	RunID      int64             `json:"run_id"`
	ArrID      int64             `json:"arr_id"`
	TargetID   int64             `json:"target_id"`
	Source     MatcharrGapSource `json:"source"`
	Title      string            `json:"title"`
	ArrName    string            `json:"arr_name"`
	TargetName string            `json:"target_name"`
	ArrPath    string            `json:"arr_path"`
	TargetPath string            `json:"target_path"`
	CreatedAt  time.Time         `json:"created_at"`
}

// MatcharrFileMismatch represents a filename mismatch between Arr and media servers.
type MatcharrFileMismatch struct {
	ID               int64     `json:"id"`
	RunID            int64     `json:"run_id"`
	ArrID            int64     `json:"arr_id"`
	TargetID         int64     `json:"target_id"`
	ArrType          ArrType   `json:"arr_type"`
	ArrName          string    `json:"arr_name"`
	TargetName       string    `json:"target_name"`
	MediaTitle       string    `json:"media_title"`
	ArrMediaID       int64     `json:"arr_media_id"`
	TargetMetadataID string    `json:"target_metadata_id"`
	SeasonNumber     int       `json:"season_number"`
	EpisodeNumber    int       `json:"episode_number"`
	ArrFileName      string    `json:"arr_file_name"`
	TargetFileNames  string    `json:"target_file_names"`
	ArrFilePath      string    `json:"arr_file_path"`
	TargetItemPath   string    `json:"target_item_path"`
	TargetFilePaths  string    `json:"target_file_paths"`
	CreatedAt        time.Time `json:"created_at"`
}

// MatcharrFileIgnore represents a persisted ignore rule for filename mismatches.
type MatcharrFileIgnore struct {
	ID            int64     `json:"id"`
	ArrID         int64     `json:"arr_id"`
	TargetID      int64     `json:"target_id"`
	ArrType       ArrType   `json:"arr_type"`
	ArrName       string    `json:"arr_name"`
	TargetName    string    `json:"target_name"`
	MediaTitle    string    `json:"media_title"`
	ArrMediaID    int64     `json:"arr_media_id"`
	SeasonNumber  int       `json:"season_number"`
	EpisodeNumber int       `json:"episode_number"`
	ArrFileName   string    `json:"arr_file_name"`
	ArrFilePath   string    `json:"arr_file_path"`
	CreatedAt     time.Time `json:"created_at"`
}

// CreateMatcharrArr creates a new Arr instance
func (db *DB) CreateMatcharrArr(arr *MatcharrArr) error {
	pathMappingsJSON, err := marshalToString(arr.PathMappings)
	if err != nil {
		return fmt.Errorf("failed to marshal path mappings: %w", err)
	}

	result, err := db.Exec(`
		INSERT INTO matcharr_arrs (name, type, url, api_key, enabled, file_concurrency, path_mappings, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, arr.Name, arr.Type, arr.URL, arr.APIKey, arr.Enabled, arr.FileConcurrency, pathMappingsJSON)
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
		SELECT id, name, type, url, api_key, enabled, file_concurrency, path_mappings, created_at, updated_at
		FROM matcharr_arrs WHERE id = ?
	`, id).Scan(
		&arr.ID, &arr.Name, &arr.Type, &arr.URL, &arr.APIKey,
		&enabled, &arr.FileConcurrency, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr arr: %w", err)
	}

	arr.Enabled = enabled != 0

	if err := unmarshalFromString(pathMappingsJSON, &arr.PathMappings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
	}

	return &arr, nil
}

// ListMatcharrArrs retrieves all Arr instances
func (db *DB) ListMatcharrArrs() ([]*MatcharrArr, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, api_key, enabled, file_concurrency, path_mappings, created_at, updated_at
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
			&enabled, &arr.FileConcurrency, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr arr: %w", err)
		}

		arr.Enabled = enabled != 0

		if err := unmarshalFromString(pathMappingsJSON, &arr.PathMappings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
		}

		arrs = append(arrs, &arr)
	}

	return arrs, rows.Err()
}

// ListEnabledMatcharrArrs retrieves all enabled Arr instances
func (db *DB) ListEnabledMatcharrArrs() ([]*MatcharrArr, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, api_key, enabled, file_concurrency, path_mappings, created_at, updated_at
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
			&enabled, &arr.FileConcurrency, &pathMappingsJSON, &arr.CreatedAt, &arr.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr arr: %w", err)
		}

		arr.Enabled = enabled != 0

		if err := unmarshalFromString(pathMappingsJSON, &arr.PathMappings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal path mappings: %w", err)
		}

		arrs = append(arrs, &arr)
	}

	return arrs, rows.Err()
}

// UpdateMatcharrArr updates an Arr instance
func (db *DB) UpdateMatcharrArr(arr *MatcharrArr) error {
	pathMappingsJSON, err := marshalToString(arr.PathMappings)
	if err != nil {
		return fmt.Errorf("failed to marshal path mappings: %w", err)
	}

	_, err = db.Exec(`
		UPDATE matcharr_arrs
		SET name = ?, type = ?, url = ?, api_key = ?, enabled = ?, file_concurrency = ?, path_mappings = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, arr.Name, arr.Type, arr.URL, arr.APIKey, arr.Enabled, arr.FileConcurrency, pathMappingsJSON, arr.ID)
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
			target_title, media_title, media_path, target_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`,
		mismatch.RunID, mismatch.ArrID, mismatch.TargetID, mismatch.ArrType, mismatch.ArrName, mismatch.TargetName,
		mismatch.TargetTitle, mismatch.MediaTitle, mismatch.MediaPath, mismatch.TargetPath, mismatch.ArrIDType, mismatch.ArrIDValue,
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

// CreateMatcharrGap creates a missing-path record
func (db *DB) CreateMatcharrGap(gap *MatcharrGap) error {
	result, err := db.Exec(`
		INSERT INTO matcharr_gaps (run_id, arr_id, target_id, source, title, arr_name, target_name, arr_path, target_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gap.RunID, gap.ArrID, gap.TargetID, gap.Source, gap.Title, gap.ArrName, gap.TargetName, gap.ArrPath, gap.TargetPath)
	if err != nil {
		return fmt.Errorf("failed to create matcharr gap: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	gap.ID = id

	return nil
}

// DeleteMatcharrGap removes a missing-path record by ID.
func (db *DB) DeleteMatcharrGap(id int64) error {
	_, err := db.Exec(`DELETE FROM matcharr_gaps WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("failed to delete matcharr gap: %w", err)
	}
	return nil
}

// CreateMatcharrFileMismatch creates a new filename mismatch record.
func (db *DB) CreateMatcharrFileMismatch(mismatch *MatcharrFileMismatch) error {
	result, err := db.Exec(`
		INSERT INTO matcharr_file_mismatches (
			run_id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, target_metadata_id, season_number, episode_number,
			arr_file_name, target_file_names, arr_file_path, target_item_path, target_file_paths
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, mismatch.RunID, mismatch.ArrID, mismatch.TargetID, mismatch.ArrType, mismatch.ArrName, mismatch.TargetName,
		mismatch.MediaTitle, mismatch.ArrMediaID, mismatch.TargetMetadataID, mismatch.SeasonNumber,
		mismatch.EpisodeNumber, mismatch.ArrFileName, mismatch.TargetFileNames, mismatch.ArrFilePath,
		mismatch.TargetItemPath, mismatch.TargetFilePaths)
	if err != nil {
		return fmt.Errorf("failed to create matcharr file mismatch: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	mismatch.ID = id
	return nil
}

// GetMatcharrFileMismatch retrieves a filename mismatch by ID.
func (db *DB) GetMatcharrFileMismatch(id int64) (*MatcharrFileMismatch, error) {
	var mismatch MatcharrFileMismatch
	err := db.QueryRow(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, target_metadata_id, season_number, episode_number,
			arr_file_name, target_file_names, arr_file_path, target_item_path, target_file_paths, created_at
		FROM matcharr_file_mismatches WHERE id = ?
	`, id).Scan(
		&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID, &mismatch.ArrType,
		&mismatch.ArrName, &mismatch.TargetName, &mismatch.MediaTitle, &mismatch.ArrMediaID,
		&mismatch.TargetMetadataID, &mismatch.SeasonNumber, &mismatch.EpisodeNumber,
		&mismatch.ArrFileName, &mismatch.TargetFileNames, &mismatch.ArrFilePath, &mismatch.TargetItemPath,
		&mismatch.TargetFilePaths,
		&mismatch.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr file mismatch: %w", err)
	}
	return &mismatch, nil
}

// ListMatcharrFileMismatches retrieves filename mismatches for a run.
func (db *DB) ListMatcharrFileMismatches(runID int64) ([]*MatcharrFileMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, target_metadata_id, season_number, episode_number,
			arr_file_name, target_file_names, arr_file_path, target_item_path, target_file_paths, created_at
		FROM matcharr_file_mismatches
		WHERE run_id = ?
		ORDER BY id
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr file mismatches: %w", err)
	}
	defer rows.Close()

	var mismatches []*MatcharrFileMismatch
	for rows.Next() {
		var mismatch MatcharrFileMismatch
		if err := rows.Scan(
			&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID, &mismatch.ArrType,
			&mismatch.ArrName, &mismatch.TargetName, &mismatch.MediaTitle, &mismatch.ArrMediaID,
			&mismatch.TargetMetadataID, &mismatch.SeasonNumber, &mismatch.EpisodeNumber,
			&mismatch.ArrFileName, &mismatch.TargetFileNames, &mismatch.ArrFilePath, &mismatch.TargetItemPath,
			&mismatch.TargetFilePaths, &mismatch.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr file mismatch: %w", err)
		}
		mismatches = append(mismatches, &mismatch)
	}
	return mismatches, rows.Err()
}

// ListMatcharrFileMismatchesForMedia retrieves filename mismatches for a single media item.
func (db *DB) ListMatcharrFileMismatchesForMedia(runID, arrID, targetID, arrMediaID int64) ([]*MatcharrFileMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, target_metadata_id, season_number, episode_number,
			arr_file_name, target_file_names, arr_file_path, target_item_path, target_file_paths, created_at
		FROM matcharr_file_mismatches
		WHERE run_id = ? AND arr_id = ? AND target_id = ? AND arr_media_id = ?
		ORDER BY id
	`, runID, arrID, targetID, arrMediaID)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr file mismatches for media: %w", err)
	}
	defer rows.Close()

	var mismatches []*MatcharrFileMismatch
	for rows.Next() {
		var mismatch MatcharrFileMismatch
		if err := rows.Scan(
			&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID, &mismatch.ArrType,
			&mismatch.ArrName, &mismatch.TargetName, &mismatch.MediaTitle, &mismatch.ArrMediaID,
			&mismatch.TargetMetadataID, &mismatch.SeasonNumber, &mismatch.EpisodeNumber,
			&mismatch.ArrFileName, &mismatch.TargetFileNames, &mismatch.ArrFilePath, &mismatch.TargetItemPath,
			&mismatch.TargetFilePaths, &mismatch.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr file mismatch: %w", err)
		}
		mismatches = append(mismatches, &mismatch)
	}
	return mismatches, rows.Err()
}

// UpdateMatcharrFileMismatch updates stored filename mismatch details.
func (db *DB) UpdateMatcharrFileMismatch(id int64, targetFileNames, arrFilePath, targetItemPath, targetFilePaths string) error {
	_, err := db.Exec(`
		UPDATE matcharr_file_mismatches
		SET target_file_names = ?, arr_file_path = ?, target_item_path = ?, target_file_paths = ?
		WHERE id = ?
	`, targetFileNames, arrFilePath, targetItemPath, targetFilePaths, id)
	if err != nil {
		return fmt.Errorf("failed to update matcharr file mismatch: %w", err)
	}
	return nil
}

// DeleteMatcharrFileMismatch deletes a filename mismatch by ID.
func (db *DB) DeleteMatcharrFileMismatch(id int64) error {
	if _, err := db.Exec(`DELETE FROM matcharr_file_mismatches WHERE id = ?`, id); err != nil {
		return fmt.Errorf("failed to delete matcharr file mismatch: %w", err)
	}
	return nil
}

// DeleteMatcharrFileMismatchesForMedia removes filename mismatches for a media item.
func (db *DB) DeleteMatcharrFileMismatchesForMedia(runID, arrID, targetID, arrMediaID int64) error {
	_, err := db.Exec(`
		DELETE FROM matcharr_file_mismatches
		WHERE run_id = ? AND arr_id = ? AND target_id = ? AND arr_media_id = ?
	`, runID, arrID, targetID, arrMediaID)
	if err != nil {
		return fmt.Errorf("failed to delete matcharr file mismatches for media: %w", err)
	}
	return nil
}

// CreateMatcharrFileIgnore creates a new filename ignore rule.
func (db *DB) CreateMatcharrFileIgnore(ignore *MatcharrFileIgnore) error {
	result, err := db.Exec(`
		INSERT INTO matcharr_file_ignores (
			arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, season_number, episode_number, arr_file_name, arr_file_path
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, ignore.ArrID, ignore.TargetID, ignore.ArrType, ignore.ArrName, ignore.TargetName,
		ignore.MediaTitle, ignore.ArrMediaID, ignore.SeasonNumber, ignore.EpisodeNumber,
		ignore.ArrFileName, ignore.ArrFilePath)
	if err != nil {
		return fmt.Errorf("failed to create matcharr file ignore: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	ignore.ID = id
	return nil
}

// GetMatcharrFileIgnore retrieves a filename ignore by ID.
func (db *DB) GetMatcharrFileIgnore(id int64) (*MatcharrFileIgnore, error) {
	var ignore MatcharrFileIgnore
	err := db.QueryRow(`
		SELECT id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, season_number, episode_number, arr_file_name, arr_file_path, created_at
		FROM matcharr_file_ignores WHERE id = ?
	`, id).Scan(
		&ignore.ID, &ignore.ArrID, &ignore.TargetID, &ignore.ArrType, &ignore.ArrName, &ignore.TargetName,
		&ignore.MediaTitle, &ignore.ArrMediaID, &ignore.SeasonNumber, &ignore.EpisodeNumber,
		&ignore.ArrFileName, &ignore.ArrFilePath, &ignore.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr file ignore: %w", err)
	}
	return &ignore, nil
}

// ListMatcharrFileIgnores retrieves all filename ignore rules.
func (db *DB) ListMatcharrFileIgnores() ([]*MatcharrFileIgnore, error) {
	rows, err := db.Query(`
		SELECT id, arr_id, target_id, arr_type, arr_name, target_name, media_title,
			arr_media_id, season_number, episode_number, arr_file_name, arr_file_path, created_at
		FROM matcharr_file_ignores
		ORDER BY media_title, season_number, episode_number
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr file ignores: %w", err)
	}
	defer rows.Close()

	var ignores []*MatcharrFileIgnore
	for rows.Next() {
		var ignore MatcharrFileIgnore
		if err := rows.Scan(
			&ignore.ID, &ignore.ArrID, &ignore.TargetID, &ignore.ArrType, &ignore.ArrName, &ignore.TargetName,
			&ignore.MediaTitle, &ignore.ArrMediaID, &ignore.SeasonNumber, &ignore.EpisodeNumber,
			&ignore.ArrFileName, &ignore.ArrFilePath, &ignore.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr file ignore: %w", err)
		}
		ignores = append(ignores, &ignore)
	}
	return ignores, rows.Err()
}

// DeleteMatcharrFileIgnore removes a filename ignore rule by ID.
func (db *DB) DeleteMatcharrFileIgnore(id int64) error {
	if _, err := db.Exec(`DELETE FROM matcharr_file_ignores WHERE id = ?`, id); err != nil {
		return fmt.Errorf("failed to delete matcharr file ignore: %w", err)
	}
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
			target_title, media_title, media_path, target_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE id = ?
	`, id).Scan(
		&mismatch.ID, &mismatch.RunID, &mismatch.ArrID, &mismatch.TargetID,
		&mismatch.ArrType, &mismatch.ArrName, &mismatch.TargetName,
		&mismatch.TargetTitle, &mismatch.MediaTitle, &mismatch.MediaPath, &mismatch.TargetPath, &mismatch.ArrIDType, &mismatch.ArrIDValue,
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

// GetMatcharrGap retrieves a gap by ID
func (db *DB) GetMatcharrGap(id int64) (*MatcharrGap, error) {
	var gap MatcharrGap

	err := db.QueryRow(`
		SELECT id, run_id, arr_id, target_id, source, title, arr_name, target_name, arr_path, target_path, created_at
		FROM matcharr_gaps WHERE id = ?
	`, id).Scan(
		&gap.ID, &gap.RunID, &gap.ArrID, &gap.TargetID, &gap.Source, &gap.Title,
		&gap.ArrName, &gap.TargetName, &gap.ArrPath, &gap.TargetPath, &gap.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get matcharr gap: %w", err)
	}

	return &gap, nil
}

// ListMatcharrMismatches retrieves mismatches for a run
func (db *DB) ListMatcharrMismatches(runID int64) ([]*MatcharrMismatch, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, arr_type, arr_name, target_name,
			target_title, media_title, media_path, target_path, arr_id_type, arr_id_value,
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
			target_title, media_title, media_path, target_path, arr_id_type, arr_id_value,
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
			target_title, media_title, media_path, target_path, arr_id_type, arr_id_value,
			target_id_type, target_id_value, target_metadata_id, status, fixed_at, error, created_at
		FROM matcharr_mismatches WHERE run_id = ? AND status IN (?, ?) ORDER BY status DESC, media_title
	`, runID, MatcharrMismatchStatusPending, MatcharrMismatchStatusFailed)
	if err != nil {
		return nil, fmt.Errorf("failed to get actionable matcharr mismatches: %w", err)
	}
	defer rows.Close()

	return scanMatcharrMismatches(rows)
}

// GetMatcharrGaps retrieves gaps for a run filtered by source
func (db *DB) GetMatcharrGaps(runID int64, source MatcharrGapSource) ([]*MatcharrGap, error) {
	rows, err := db.Query(`
		SELECT id, run_id, arr_id, target_id, source, title, arr_name, target_name, arr_path, target_path, created_at
		FROM matcharr_gaps
		WHERE run_id = ? AND source = ?
		ORDER BY title
	`, runID, source)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr gaps: %w", err)
	}
	defer rows.Close()

	var gaps []*MatcharrGap
	for rows.Next() {
		var gap MatcharrGap
		if err := rows.Scan(&gap.ID, &gap.RunID, &gap.ArrID, &gap.TargetID, &gap.Source, &gap.Title, &gap.ArrName, &gap.TargetName, &gap.ArrPath, &gap.TargetPath, &gap.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan matcharr gap: %w", err)
		}
		gaps = append(gaps, &gap)
	}

	return gaps, rows.Err()
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
			&mismatch.ArrType, &mismatch.ArrName, &mismatch.TargetName, &mismatch.TargetTitle,
			&mismatch.MediaTitle, &mismatch.MediaPath, &mismatch.TargetPath, &mismatch.ArrIDType, &mismatch.ArrIDValue,
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

	// Delete gaps
	if _, err := db.Exec(`DELETE FROM matcharr_gaps`); err != nil {
		return fmt.Errorf("failed to clear matcharr gaps: %w", err)
	}

	if _, err := db.Exec(`DELETE FROM matcharr_file_mismatches`); err != nil {
		return fmt.Errorf("failed to clear matcharr file mismatches: %w", err)
	}

	// Then delete runs
	_, err = db.Exec(`DELETE FROM matcharr_runs`)
	if err != nil {
		return fmt.Errorf("failed to clear matcharr runs: %w", err)
	}
	return nil
}

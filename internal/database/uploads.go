package database

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// TransferType represents whether to copy or move files
type TransferType string

const (
	TransferTypeCopy TransferType = "copy"
	TransferTypeMove TransferType = "move"
)

// UploadStatus represents the status of an upload
type UploadStatus string

const (
	UploadStatusQueued    UploadStatus = "queued"
	UploadStatusPending   UploadStatus = "pending" // Waiting for mode conditions
	UploadStatusUploading UploadStatus = "uploading"
	UploadStatusCompleted UploadStatus = "completed"
	UploadStatusFailed    UploadStatus = "failed"
)

// Remote represents an rclone remote destination
type Remote struct {
	ID              int64          `json:"id"`
	Name            string         `json:"name"`
	RcloneRemote    string         `json:"rclone_remote"` // e.g., "gdrive:" or "dropbox:backup"
	Enabled         bool           `json:"enabled"`
	TransferOptions map[string]any `json:"transfer_options,omitempty"` // Backend-specific options
	CreatedAt       time.Time      `json:"created_at"`
}

// Destination represents a local path configuration for uploads
type Destination struct {
	ID                int64                       `json:"id"`
	LocalPath         string                      `json:"local_path"`
	MinFileAgeMinutes int                         `json:"min_file_age_minutes"`
	MinFolderSizeGB   int                         `json:"min_folder_size_gb"`
	UsePlexTracking   bool                        `json:"use_plex_scan_tracking"`
	TransferType      TransferType                `json:"transfer_type"` // copy or move
	Enabled           bool                        `json:"enabled"`
	ExcludePaths      []string                    `json:"exclude_paths,omitempty"`
	ExcludeExtensions []string                    `json:"exclude_extensions,omitempty"`
	AdvancedFilters   *DestinationAdvancedFilters `json:"advanced_filters,omitempty"`
	IncludedTriggers  []int64                     `json:"included_triggers,omitempty"` // Inotify trigger IDs that can upload to this destination (empty = all allowed)
	CreatedAt         time.Time                   `json:"created_at"`
	Remotes           []*DestinationRemote        `json:"remotes,omitempty"`
	PlexTargets       []*DestinationPlexTarget    `json:"plex_targets,omitempty"`
}

// DestinationPlexTarget configures Plex scan tracking for a destination.
type DestinationPlexTarget struct {
	DestinationID        int64  `json:"destination_id"`
	TargetID             int64  `json:"target_id"`
	IdleThresholdSeconds int    `json:"idle_threshold_seconds"`
	TargetName           string `json:"target_name,omitempty"`
}

// DestinationAdvancedFilters provides regex-based include/exclude filtering for destinations
type DestinationAdvancedFilters struct {
	// IncludePatterns are regex patterns - path must match at least one (if any specified)
	IncludePatterns []string `json:"include_patterns,omitempty"`
	// ExcludePatterns are regex patterns - path matching any will be rejected
	ExcludePatterns []string `json:"exclude_patterns,omitempty"`

	// Compiled regex patterns (not serialized)
	compiledIncludes []*regexp.Regexp `json:"-"`
	compiledExcludes []*regexp.Regexp `json:"-"`
}

// Compile compiles the regex patterns for use in filtering
// Returns an error if any pattern is invalid
func (af *DestinationAdvancedFilters) Compile() error {
	if af == nil {
		return nil
	}

	af.compiledIncludes = make([]*regexp.Regexp, 0, len(af.IncludePatterns))
	for _, pattern := range af.IncludePatterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid include pattern %q: %w", pattern, err)
		}
		af.compiledIncludes = append(af.compiledIncludes, re)
	}

	af.compiledExcludes = make([]*regexp.Regexp, 0, len(af.ExcludePatterns))
	for _, pattern := range af.ExcludePatterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
		}
		af.compiledExcludes = append(af.compiledExcludes, re)
	}

	return nil
}

// Matches checks if a path matches the advanced filter rules
// Returns true if the path should be processed, false if it should be skipped
func (af *DestinationAdvancedFilters) Matches(path string) bool {
	if af == nil {
		return true
	}

	// Check include patterns - if any specified, path must match at least one
	if len(af.compiledIncludes) > 0 {
		matched := false
		for _, re := range af.compiledIncludes {
			if re.MatchString(path) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check exclude patterns - if path matches any, reject it
	for _, re := range af.compiledExcludes {
		if re.MatchString(path) {
			return false
		}
	}

	return true
}

// ShouldExcludePath checks if a file path should be excluded
// based on the configured exclude path prefixes
func (d *Destination) ShouldExcludePath(filePath string) bool {
	for _, prefix := range d.ExcludePaths {
		if strings.HasPrefix(filePath, prefix) {
			return true
		}
	}
	return false
}

// ShouldExcludeExtension checks if a file should be excluded based on its extension
func (d *Destination) ShouldExcludeExtension(filePath string) bool {
	ext := strings.ToLower(filepath.Ext(filePath))
	for _, excludeExt := range d.ExcludeExtensions {
		// Normalize the exclude extension (ensure it starts with .)
		normalizedExt := strings.ToLower(excludeExt)
		if !strings.HasPrefix(normalizedExt, ".") {
			normalizedExt = "." + normalizedExt
		}
		if ext == normalizedExt {
			return true
		}
	}
	return false
}

// ShouldExcludeByAdvancedFilters checks if a file should be excluded based on regex patterns
func (d *Destination) ShouldExcludeByAdvancedFilters(filePath string) bool {
	if d.AdvancedFilters == nil {
		return false
	}
	return !d.AdvancedFilters.Matches(filePath)
}

// ShouldExclude checks if a file should be excluded based on all filter rules
// (path prefixes, extensions, and advanced regex filters)
func (d *Destination) ShouldExclude(filePath string) bool {
	// Check path prefix exclusions
	if d.ShouldExcludePath(filePath) {
		return true
	}
	// Check extension exclusions
	if d.ShouldExcludeExtension(filePath) {
		return true
	}
	// Check advanced regex filters
	if d.ShouldExcludeByAdvancedFilters(filePath) {
		return true
	}
	return false
}

// CompileAdvancedFilters compiles the regex patterns in AdvancedFilters
// This should be called after loading a destination from the database
func (d *Destination) CompileAdvancedFilters() error {
	if d.AdvancedFilters != nil {
		return d.AdvancedFilters.Compile()
	}
	return nil
}

// ShouldAllowTrigger checks if a trigger ID is allowed to upload to this destination.
// If IncludedTriggers is empty, all triggers are allowed.
// If IncludedTriggers has entries, only those triggers are allowed.
func (d *Destination) ShouldAllowTrigger(triggerID int64) bool {
	// Empty list means all triggers are allowed
	if len(d.IncludedTriggers) == 0 {
		return true
	}
	// Check if trigger is in the include list
	return slices.Contains(d.IncludedTriggers, triggerID)
}

// DestinationRemote represents the mapping between destinations and remotes
type DestinationRemote struct {
	DestinationID int64  `json:"destination_id"`
	RemoteID      int64  `json:"remote_id"`
	Priority      int    `json:"priority"`
	RemotePath    string `json:"remote_path"`
	RemoteName    string `json:"remote_name,omitempty"`   // Populated from join
	RcloneRemote  string `json:"rclone_remote,omitempty"` // Populated from join
}

// Upload represents an item in the upload queue
type Upload struct {
	ID             int64        `json:"id"`
	ScanID         *int64       `json:"scan_id,omitempty"`
	LocalPath      string       `json:"local_path"`
	RemoteName     string       `json:"remote_name"`
	RemotePath     string       `json:"remote_path"`
	Status         UploadStatus `json:"status"`
	SizeBytes      *int64       `json:"size_bytes,omitempty"`
	CreatedAt      time.Time    `json:"created_at"`
	StartedAt      *time.Time   `json:"started_at,omitempty"`
	CompletedAt    *time.Time   `json:"completed_at,omitempty"`
	RcloneJobID    *int64       `json:"rclone_job_id,omitempty"`
	ProgressBytes  int64        `json:"progress_bytes"`
	RetryCount     int          `json:"retry_count"`
	LastError      string       `json:"last_error,omitempty"`
	RemotePriority int          `json:"remote_priority"`
	WaitState      string       `json:"wait_state,omitempty"`
	WaitChecks     string       `json:"wait_checks,omitempty"`
}

// UploadHistory stores completed upload analytics
type UploadHistory struct {
	ID          int64     `json:"id"`
	UploadID    *int64    `json:"upload_id,omitempty"`
	LocalPath   string    `json:"local_path"`
	RemoteName  string    `json:"remote_name"`
	RemotePath  string    `json:"remote_path"`
	SizeBytes   *int64    `json:"size_bytes,omitempty"`
	CompletedAt time.Time `json:"completed_at"`
}

// ========== Remote CRUD ==========

// CreateRemote creates a new remote
func (db *DB) CreateRemote(remote *Remote) error {
	optionsJSON, err := marshalToPtr(remote.TransferOptions)
	if err != nil {
		return fmt.Errorf("failed to marshal transfer options: %w", err)
	}

	result, err := db.Exec(`
		INSERT INTO remotes (name, rclone_remote, enabled, transfer_options, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, remote.Name, remote.RcloneRemote, remote.Enabled, optionsJSON, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create remote: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	remote.ID = id
	remote.CreatedAt = time.Now()

	return nil
}

// GetRemote retrieves a remote by ID
func (db *DB) GetRemote(id int64) (*Remote, error) {
	remote := &Remote{}
	var optionsJSON sql.NullString

	err := db.QueryRow(`
		SELECT id, name, rclone_remote, enabled, transfer_options, created_at
		FROM remotes WHERE id = ?
	`, id).Scan(&remote.ID, &remote.Name, &remote.RcloneRemote, &remote.Enabled, &optionsJSON, &remote.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get remote: %w", err)
	}

	if err := unmarshalFromNullString(optionsJSON, &remote.TransferOptions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transfer options: %w", err)
	}

	return remote, nil
}

// ListRemotes retrieves all remotes
func (db *DB) ListRemotes() ([]*Remote, error) {
	rows, err := db.Query(`
		SELECT id, name, rclone_remote, enabled, transfer_options, created_at
		FROM remotes ORDER BY name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list remotes: %w", err)
	}
	defer rows.Close()

	var remotes []*Remote
	for rows.Next() {
		remote := &Remote{}
		var optionsJSON sql.NullString
		if err := rows.Scan(&remote.ID, &remote.Name, &remote.RcloneRemote, &remote.Enabled, &optionsJSON, &remote.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan remote: %w", err)
		}
		if err := unmarshalFromNullString(optionsJSON, &remote.TransferOptions); err != nil {
			log.Warn().Err(err).Int64("remote_id", remote.ID).Msg("Failed to unmarshal remote transfer options")
		}
		remotes = append(remotes, remote)
	}

	return remotes, nil
}

// ListEnabledRemotes retrieves all enabled remotes
func (db *DB) ListEnabledRemotes() ([]*Remote, error) {
	rows, err := db.Query(`
		SELECT id, name, rclone_remote, enabled, transfer_options, created_at
		FROM remotes WHERE enabled = true ORDER BY name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled remotes: %w", err)
	}
	defer rows.Close()

	var remotes []*Remote
	for rows.Next() {
		remote := &Remote{}
		var optionsJSON sql.NullString
		if err := rows.Scan(&remote.ID, &remote.Name, &remote.RcloneRemote, &remote.Enabled, &optionsJSON, &remote.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan remote: %w", err)
		}
		if err := unmarshalFromNullString(optionsJSON, &remote.TransferOptions); err != nil {
			log.Warn().Err(err).Int64("remote_id", remote.ID).Msg("Failed to unmarshal remote transfer options")
		}
		remotes = append(remotes, remote)
	}

	return remotes, nil
}

// UpdateRemote updates an existing remote
func (db *DB) UpdateRemote(remote *Remote) error {
	optionsJSON, err := marshalToPtr(remote.TransferOptions)
	if err != nil {
		return fmt.Errorf("failed to marshal transfer options: %w", err)
	}

	if err := db.execAndVerifyAffected(`
		UPDATE remotes SET name = ?, rclone_remote = ?, enabled = ?, transfer_options = ?
		WHERE id = ?
	`, remote.Name, remote.RcloneRemote, remote.Enabled, optionsJSON, remote.ID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("remote not found")
		}
		return fmt.Errorf("failed to update remote: %w", err)
	}

	return nil
}

// DeleteRemote deletes a remote by ID
func (db *DB) DeleteRemote(id int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM remotes WHERE id = ?", id); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("remote not found")
		}
		return fmt.Errorf("failed to delete remote: %w", err)
	}
	return nil
}

// CountRemotes returns the total number of remotes
func (db *DB) CountRemotes() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM remotes").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count remotes: %w", err)
	}
	return count, nil
}

// ========== Destination CRUD ==========

// CreateDestination creates a new destination
func (db *DB) CreateDestination(dest *Destination) error {
	// Marshal exclude paths, extensions, and triggers to JSON
	excludePathsJSON, err := marshalToString(dest.ExcludePaths)
	if err != nil {
		return fmt.Errorf("failed to marshal exclude paths: %w", err)
	}
	excludeExtensionsJSON, err := marshalToString(dest.ExcludeExtensions)
	if err != nil {
		return fmt.Errorf("failed to marshal exclude extensions: %w", err)
	}
	includedTriggersJSON, err := marshalToString(dest.IncludedTriggers)
	if err != nil {
		return fmt.Errorf("failed to marshal included triggers: %w", err)
	}

	// Marshal advanced filters to JSON (may be nil)
	advancedFiltersJSON, err := marshalToPtr(dest.AdvancedFilters)
	if err != nil {
		return fmt.Errorf("failed to marshal advanced filters: %w", err)
	}

	// Default transfer type to move if not set
	if dest.TransferType == "" {
		dest.TransferType = TransferTypeMove
	}

	result, err := db.Exec(`
		INSERT INTO destinations (local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, dest.LocalPath, dest.MinFileAgeMinutes, dest.MinFolderSizeGB, dest.UsePlexTracking, dest.TransferType, dest.Enabled,
		string(excludePathsJSON), string(excludeExtensionsJSON), string(includedTriggersJSON), advancedFiltersJSON, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create destination: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	dest.ID = id
	dest.CreatedAt = time.Now()

	return nil
}

// GetDestination retrieves a destination by ID
func (db *DB) GetDestination(id int64) (*Destination, error) {
	dest := &Destination{}
	var excludePathsJSON, excludeExtensionsJSON, includedTriggersJSON, advancedFiltersJSON sql.NullString

	err := db.QueryRow(`
		SELECT id, local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at
		FROM destinations WHERE id = ?
	`, id).Scan(&dest.ID, &dest.LocalPath, &dest.MinFileAgeMinutes, &dest.MinFolderSizeGB, &dest.UsePlexTracking, &dest.TransferType, &dest.Enabled,
		&excludePathsJSON, &excludeExtensionsJSON, &includedTriggersJSON, &advancedFiltersJSON, &dest.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get destination: %w", err)
	}

	// Default transfer type if empty (for backward compatibility)
	if dest.TransferType == "" {
		dest.TransferType = TransferTypeMove
	}

	// Unmarshal JSON arrays
	if err := unmarshalFromNullString(excludePathsJSON, &dest.ExcludePaths); err != nil {
		log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination exclude paths")
	}
	if err := unmarshalFromNullString(excludeExtensionsJSON, &dest.ExcludeExtensions); err != nil {
		log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination exclude extensions")
	}
	if err := unmarshalFromNullString(includedTriggersJSON, &dest.IncludedTriggers); err != nil {
		log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination included triggers")
	}
	if advancedFiltersJSON.Valid && advancedFiltersJSON.String != "" {
		dest.AdvancedFilters = &DestinationAdvancedFilters{}
		if err := unmarshalFromNullString(advancedFiltersJSON, dest.AdvancedFilters); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination advanced filters")
		}
	}

	// Compile advanced filters
	if err := dest.CompileAdvancedFilters(); err != nil {
		return nil, fmt.Errorf("failed to compile advanced filters: %w", err)
	}

	// Load associated remotes
	remotes, err := db.GetDestinationRemotes(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination remotes: %w", err)
	}
	dest.Remotes = remotes

	// Load Plex target mappings
	plexTargets, err := db.GetDestinationPlexTargets(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination plex targets: %w", err)
	}
	dest.PlexTargets = plexTargets

	return dest, nil
}

// GetDestinationByPath retrieves a destination by its local path (prefix match)
func (db *DB) GetDestinationByPath(localPath string) (*Destination, error) {
	// Find the most specific matching destination
	rows, err := db.Query(`
		SELECT id, local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at
		FROM destinations WHERE enabled = true
		ORDER BY length(local_path) DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query destinations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		dest := &Destination{}
		var excludePathsJSON, excludeExtensionsJSON, includedTriggersJSON, advancedFiltersJSON sql.NullString

		if err := rows.Scan(&dest.ID, &dest.LocalPath, &dest.MinFileAgeMinutes, &dest.MinFolderSizeGB, &dest.UsePlexTracking, &dest.TransferType, &dest.Enabled,
			&excludePathsJSON, &excludeExtensionsJSON, &includedTriggersJSON, &advancedFiltersJSON, &dest.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan destination: %w", err)
		}

		// Default transfer type if empty (for backward compatibility)
		if dest.TransferType == "" {
			dest.TransferType = TransferTypeMove
		}

		// Unmarshal JSON arrays
		if err := unmarshalFromNullString(excludePathsJSON, &dest.ExcludePaths); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination exclude paths")
		}
		if err := unmarshalFromNullString(excludeExtensionsJSON, &dest.ExcludeExtensions); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination exclude extensions")
		}
		if err := unmarshalFromNullString(includedTriggersJSON, &dest.IncludedTriggers); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination included triggers")
		}
		if advancedFiltersJSON.Valid && advancedFiltersJSON.String != "" {
			dest.AdvancedFilters = &DestinationAdvancedFilters{}
			if err := unmarshalFromNullString(advancedFiltersJSON, dest.AdvancedFilters); err != nil {
				log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to unmarshal destination advanced filters")
			}
		}

		// Check if localPath starts with this destination
		if strings.HasPrefix(localPath, dest.LocalPath) {
			// Compile advanced filters
			if err := dest.CompileAdvancedFilters(); err != nil {
				return nil, fmt.Errorf("failed to compile advanced filters: %w", err)
			}
			// Load associated remotes
			remotes, err := db.GetDestinationRemotes(dest.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to get destination remotes: %w", err)
			}
			dest.Remotes = remotes
			// Load Plex target mappings
			plexTargets, err := db.GetDestinationPlexTargets(dest.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to get destination plex targets: %w", err)
			}
			dest.PlexTargets = plexTargets
			return dest, nil
		}
	}

	return nil, nil
}

// scanDestination scans a row into a Destination struct
func (db *DB) scanDestination(scanner interface {
	Scan(dest ...any) error
}) (*Destination, error) {
	d := &Destination{}
	var excludePathsJSON, excludeExtensionsJSON, includedTriggersJSON, advancedFiltersJSON sql.NullString

	if err := scanner.Scan(&d.ID, &d.LocalPath, &d.MinFileAgeMinutes, &d.MinFolderSizeGB, &d.UsePlexTracking, &d.TransferType, &d.Enabled,
		&excludePathsJSON, &excludeExtensionsJSON, &includedTriggersJSON, &advancedFiltersJSON, &d.CreatedAt); err != nil {
		return nil, err
	}

	// Default transfer type if empty (for backward compatibility)
	if d.TransferType == "" {
		d.TransferType = TransferTypeMove
	}

	// Unmarshal JSON arrays
	if err := unmarshalFromNullString(excludePathsJSON, &d.ExcludePaths); err != nil {
		log.Warn().Err(err).Int64("destination_id", d.ID).Msg("Failed to unmarshal destination exclude paths")
	}
	if err := unmarshalFromNullString(excludeExtensionsJSON, &d.ExcludeExtensions); err != nil {
		log.Warn().Err(err).Int64("destination_id", d.ID).Msg("Failed to unmarshal destination exclude extensions")
	}
	if err := unmarshalFromNullString(includedTriggersJSON, &d.IncludedTriggers); err != nil {
		log.Warn().Err(err).Int64("destination_id", d.ID).Msg("Failed to unmarshal destination included triggers")
	}
	if advancedFiltersJSON.Valid && advancedFiltersJSON.String != "" {
		d.AdvancedFilters = &DestinationAdvancedFilters{}
		if err := unmarshalFromNullString(advancedFiltersJSON, d.AdvancedFilters); err != nil {
			log.Warn().Err(err).Int64("destination_id", d.ID).Msg("Failed to unmarshal destination advanced filters")
		}
	}

	// Compile advanced filters
	if err := d.CompileAdvancedFilters(); err != nil {
		return nil, fmt.Errorf("failed to compile advanced filters: %w", err)
	}

	return d, nil
}

// ListDestinations retrieves all destinations
func (db *DB) ListDestinations() ([]*Destination, error) {
	rows, err := db.Query(`
		SELECT id, local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at
		FROM destinations ORDER BY local_path ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list destinations: %w", err)
	}
	defer rows.Close()

	var destinations []*Destination
	for rows.Next() {
		dest, err := db.scanDestination(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan destination: %w", err)
		}
		destinations = append(destinations, dest)
	}

	// Load remotes for each destination
	for _, dest := range destinations {
		remotes, err := db.GetDestinationRemotes(dest.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get destination remotes: %w", err)
		}
		dest.Remotes = remotes
		plexTargets, err := db.GetDestinationPlexTargets(dest.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get destination plex targets: %w", err)
		}
		dest.PlexTargets = plexTargets
	}

	return destinations, nil
}

// ListEnabledDestinations retrieves all enabled destinations
func (db *DB) ListEnabledDestinations() ([]*Destination, error) {
	rows, err := db.Query(`
		SELECT id, local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at
		FROM destinations WHERE enabled = true ORDER BY local_path ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled destinations: %w", err)
	}
	defer rows.Close()

	var dests []*Destination
	for rows.Next() {
		dest, err := db.scanDestination(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan destination: %w", err)
		}
		dests = append(dests, dest)
	}

	// Load remotes for each destination
	for _, dest := range dests {
		remotes, err := db.GetDestinationRemotes(dest.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get destination remotes: %w", err)
		}
		dest.Remotes = remotes
		plexTargets, err := db.GetDestinationPlexTargets(dest.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get destination plex targets: %w", err)
		}
		dest.PlexTargets = plexTargets
	}

	return dests, nil
}

// UpdateDestination updates an existing destination
func (db *DB) UpdateDestination(dest *Destination) error {
	// Marshal exclude paths, extensions, and triggers to JSON
	excludePathsJSON, err := marshalToString(dest.ExcludePaths)
	if err != nil {
		return fmt.Errorf("failed to marshal exclude paths: %w", err)
	}
	excludeExtensionsJSON, err := marshalToString(dest.ExcludeExtensions)
	if err != nil {
		return fmt.Errorf("failed to marshal exclude extensions: %w", err)
	}
	includedTriggersJSON, err := marshalToString(dest.IncludedTriggers)
	if err != nil {
		return fmt.Errorf("failed to marshal included triggers: %w", err)
	}

	// Marshal advanced filters to JSON (may be nil)
	advancedFiltersJSON, err := marshalToPtr(dest.AdvancedFilters)
	if err != nil {
		return fmt.Errorf("failed to marshal advanced filters: %w", err)
	}

	// Default transfer type to move if not set
	if dest.TransferType == "" {
		dest.TransferType = TransferTypeMove
	}

	result, err := db.Exec(`
		UPDATE destinations SET local_path = ?, min_file_age_minutes = ?, min_folder_size_gb = ?, use_plex_scan_tracking = ?, transfer_type = ?, enabled = ?,
			exclude_paths = ?, exclude_extensions = ?, included_triggers = ?, advanced_filters = ?
		WHERE id = ?
	`, dest.LocalPath, dest.MinFileAgeMinutes, dest.MinFolderSizeGB, dest.UsePlexTracking, dest.TransferType, dest.Enabled,
		excludePathsJSON, excludeExtensionsJSON, includedTriggersJSON, advancedFiltersJSON, dest.ID)
	if err != nil {
		return fmt.Errorf("failed to update destination: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("destination not found")
	}

	return nil
}

// DeleteDestination deletes a destination by ID
func (db *DB) DeleteDestination(id int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM destinations WHERE id = ?", id); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("destination not found")
		}
		return fmt.Errorf("failed to delete destination: %w", err)
	}
	return nil
}

// CountDestinations returns the total number of destinations
func (db *DB) CountDestinations() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM destinations").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count destinations: %w", err)
	}
	return count, nil
}

// DestinationPathExists checks if a destination exists with the exact local path.
// If excludeID is non-nil, that destination ID is ignored.
func (db *DB) DestinationPathExists(localPath string, excludeID *int64) (bool, error) {
	query := "SELECT COUNT(*) FROM destinations WHERE local_path = ?"
	args := []any{localPath}
	if excludeID != nil {
		query += " AND id != ?"
		args = append(args, *excludeID)
	}
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check destination path: %w", err)
	}
	return count > 0, nil
}

// ListDestinationsWithPlexTracking retrieves all destinations with Plex scan tracking enabled.
func (db *DB) ListDestinationsWithPlexTracking() ([]*Destination, error) {
	rows, err := db.Query(`
		SELECT id, local_path, min_file_age_minutes, min_folder_size_gb, use_plex_scan_tracking, transfer_type, enabled,
			exclude_paths, exclude_extensions, included_triggers, advanced_filters, created_at
		FROM destinations
		WHERE enabled = true AND use_plex_scan_tracking = true
		ORDER BY local_path ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list plex tracking destinations: %w", err)
	}
	defer rows.Close()

	var destinations []*Destination
	for rows.Next() {
		dest, err := db.scanDestination(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan destination: %w", err)
		}
		destinations = append(destinations, dest)
	}

	for _, dest := range destinations {
		plexTargets, err := db.GetDestinationPlexTargets(dest.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get destination plex targets: %w", err)
		}
		dest.PlexTargets = plexTargets
	}

	return destinations, nil
}

// ========== DestinationRemote CRUD ==========

// AddDestinationRemote adds a remote mapping to a destination
func (db *DB) AddDestinationRemote(dr *DestinationRemote) error {
	_, err := db.Exec(`
		INSERT INTO destination_remotes (destination_id, remote_id, priority, remote_path)
		VALUES (?, ?, ?, ?)
	`, dr.DestinationID, dr.RemoteID, dr.Priority, dr.RemotePath)
	if err != nil {
		return fmt.Errorf("failed to add destination remote: %w", err)
	}

	return nil
}

// GetDestinationRemotes retrieves all remote mappings for a destination
func (db *DB) GetDestinationRemotes(destinationID int64) ([]*DestinationRemote, error) {
	rows, err := db.Query(`
		SELECT dr.destination_id, dr.remote_id, dr.priority, dr.remote_path, r.name, r.rclone_remote
		FROM destination_remotes dr
		JOIN remotes r ON dr.remote_id = r.id
		WHERE dr.destination_id = ?
		ORDER BY dr.priority ASC
	`, destinationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination remotes: %w", err)
	}
	defer rows.Close()

	var mappings []*DestinationRemote
	for rows.Next() {
		dr := &DestinationRemote{}
		if err := rows.Scan(&dr.DestinationID, &dr.RemoteID, &dr.Priority, &dr.RemotePath, &dr.RemoteName, &dr.RcloneRemote); err != nil {
			return nil, fmt.Errorf("failed to scan destination remote: %w", err)
		}
		mappings = append(mappings, dr)
	}

	return mappings, nil
}

// UpdateDestinationRemote updates an existing remote mapping
func (db *DB) UpdateDestinationRemote(dr *DestinationRemote) error {
	result, err := db.Exec(`
		UPDATE destination_remotes SET priority = ?, remote_path = ?
		WHERE destination_id = ? AND remote_id = ?
	`, dr.Priority, dr.RemotePath, dr.DestinationID, dr.RemoteID)
	if err != nil {
		return fmt.Errorf("failed to update destination remote: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("destination remote not found")
	}

	return nil
}

// RemoveDestinationRemote removes a remote mapping from a destination
func (db *DB) RemoveDestinationRemote(destinationID, remoteID int64) error {
	if err := db.execAndVerifyAffected(`
		DELETE FROM destination_remotes WHERE destination_id = ? AND remote_id = ?
	`, destinationID, remoteID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("destination remote not found")
		}
		return fmt.Errorf("failed to remove destination remote: %w", err)
	}
	return nil
}

// ClearDestinationRemotes removes all remote mappings for a destination
func (db *DB) ClearDestinationRemotes(destinationID int64) error {
	_, err := db.Exec(`DELETE FROM destination_remotes WHERE destination_id = ?`, destinationID)
	if err != nil {
		return fmt.Errorf("failed to clear destination remotes: %w", err)
	}
	return nil
}

// GetMaxDestinationRemotePriority returns the highest priority number for a given destination
func (db *DB) GetMaxDestinationRemotePriority(destinationID int64) (int, error) {
	var max int
	err := db.QueryRow(`SELECT COALESCE(MAX(priority), 0) FROM destination_remotes WHERE destination_id = ?`, destinationID).Scan(&max)
	if err != nil {
		return 0, fmt.Errorf("failed to get max priority: %w", err)
	}
	return max, nil
}

// ReorderDestinationRemotes updates the priority of remotes based on their position in the slice
func (db *DB) ReorderDestinationRemotes(destinationID int64, remoteIDs []int64) error {
	for i, remoteID := range remoteIDs {
		priority := i + 1
		_, err := db.Exec(`UPDATE destination_remotes SET priority = ? WHERE destination_id = ? AND remote_id = ?`,
			priority, destinationID, remoteID)
		if err != nil {
			return fmt.Errorf("failed to update priority for remote %d: %w", remoteID, err)
		}
	}
	return nil
}

// ========== Destination Plex Target CRUD ==========

// GetDestinationPlexTargets retrieves Plex targets configured for a destination.
func (db *DB) GetDestinationPlexTargets(destinationID int64) ([]*DestinationPlexTarget, error) {
	rows, err := db.Query(`
		SELECT dpt.destination_id, dpt.target_id, dpt.idle_threshold_seconds, COALESCE(t.name, '')
		FROM destination_plex_targets dpt
		LEFT JOIN targets t ON t.id = dpt.target_id
		WHERE dpt.destination_id = ?
		ORDER BY t.name ASC
	`, destinationID)
	if err != nil {
		return nil, fmt.Errorf("failed to list destination plex targets: %w", err)
	}
	defer rows.Close()

	var targets []*DestinationPlexTarget
	for rows.Next() {
		item := &DestinationPlexTarget{}
		if err := rows.Scan(&item.DestinationID, &item.TargetID, &item.IdleThresholdSeconds, &item.TargetName); err != nil {
			return nil, fmt.Errorf("failed to scan destination plex target: %w", err)
		}
		targets = append(targets, item)
	}

	return targets, nil
}

// SetDestinationPlexTargets replaces Plex target mappings for a destination.
func (db *DB) SetDestinationPlexTargets(destinationID int64, targets []*DestinationPlexTarget) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM destination_plex_targets WHERE destination_id = ?`, destinationID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to clear destination plex targets: %w", err)
	}
	for _, target := range targets {
		if _, err := tx.Exec(`
			INSERT INTO destination_plex_targets (destination_id, target_id, idle_threshold_seconds)
			VALUES (?, ?, ?)
		`, destinationID, target.TargetID, target.IdleThresholdSeconds); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to add destination plex target: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit destination plex targets: %w", err)
	}
	return nil
}

// ClearDestinationPlexTargets removes all Plex target mappings for a destination.
func (db *DB) ClearDestinationPlexTargets(destinationID int64) error {
	if _, err := db.Exec(`DELETE FROM destination_plex_targets WHERE destination_id = ?`, destinationID); err != nil {
		return fmt.Errorf("failed to clear destination plex targets: %w", err)
	}
	return nil
}

// ========== Upload CRUD ==========

// CreateUpload creates a new upload record
func (db *DB) CreateUpload(upload *Upload) error {
	result, err := db.Exec(`
		INSERT INTO uploads (scan_id, local_path, remote_name, remote_path, status, size_bytes, wait_state, wait_checks, created_at, progress_bytes, retry_count, remote_priority)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, upload.ScanID, upload.LocalPath, upload.RemoteName, upload.RemotePath, upload.Status, upload.SizeBytes, upload.WaitState, upload.WaitChecks, time.Now(), 0, 0, upload.RemotePriority)
	if err != nil {
		return fmt.Errorf("failed to create upload: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	upload.ID = id
	upload.CreatedAt = time.Now()

	return nil
}

// GetUpload retrieves an upload by ID
func (db *DB) GetUpload(id int64) (*Upload, error) {
	upload := &Upload{}
	var scanID, sizeBytes, rcloneJobID sql.NullInt64
	var startedAt, completedAt sql.NullTime
	var lastError sql.NullString

	err := db.QueryRow(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads WHERE id = ?
	`, id).Scan(&upload.ID, &scanID, &upload.LocalPath, &upload.RemoteName, &upload.RemotePath,
		&upload.Status, &sizeBytes, &upload.WaitState, &upload.WaitChecks, &upload.CreatedAt, &startedAt, &completedAt,
		&rcloneJobID, &upload.ProgressBytes, &upload.RetryCount, &lastError, &upload.RemotePriority)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get upload: %w", err)
	}

	upload.ScanID = nullInt64ToPtr(scanID)
	upload.SizeBytes = nullInt64ToPtr(sizeBytes)
	upload.StartedAt = nullTimeToPtr(startedAt)
	upload.CompletedAt = nullTimeToPtr(completedAt)
	upload.RcloneJobID = nullInt64ToPtr(rcloneJobID)
	upload.LastError = nullStringValue(lastError)

	return upload, nil
}

// uploadRowsToUploads converts sql.Rows to a slice of Upload
func (db *DB) uploadRowsToUploads(rows *sql.Rows) ([]*Upload, error) {
	var uploads []*Upload
	for rows.Next() {
		upload := &Upload{}
		var scanID, sizeBytes, rcloneJobID sql.NullInt64
		var startedAt, completedAt sql.NullTime
		var lastError sql.NullString

		if err := rows.Scan(&upload.ID, &scanID, &upload.LocalPath, &upload.RemoteName, &upload.RemotePath,
			&upload.Status, &sizeBytes, &upload.WaitState, &upload.WaitChecks, &upload.CreatedAt, &startedAt, &completedAt,
			&rcloneJobID, &upload.ProgressBytes, &upload.RetryCount, &lastError, &upload.RemotePriority); err != nil {
			return nil, fmt.Errorf("failed to scan upload: %w", err)
		}

		upload.ScanID = nullInt64ToPtr(scanID)
		upload.SizeBytes = nullInt64ToPtr(sizeBytes)
		upload.StartedAt = nullTimeToPtr(startedAt)
		upload.CompletedAt = nullTimeToPtr(completedAt)
		upload.RcloneJobID = nullInt64ToPtr(rcloneJobID)
		upload.LastError = nullStringValue(lastError)

		uploads = append(uploads, upload)
	}

	return uploads, nil
}

// ListPendingUploads returns uploads waiting for mode conditions
func (db *DB) ListPendingUploads() ([]*Upload, error) {
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE status = ?
		ORDER BY remote_priority ASC, created_at ASC
	`, UploadStatusPending)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending uploads: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// ListQueuedUploads returns uploads ready to start
func (db *DB) ListQueuedUploads() ([]*Upload, error) {
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE status = ?
		ORDER BY remote_priority ASC, created_at ASC
	`, UploadStatusQueued)
	if err != nil {
		return nil, fmt.Errorf("failed to list queued uploads: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// ListActiveUploads returns uploads currently in progress
func (db *DB) ListActiveUploads() ([]*Upload, error) {
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE status = ?
		ORDER BY started_at ASC
	`, UploadStatusUploading)
	if err != nil {
		return nil, fmt.Errorf("failed to list active uploads: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// ListRecentUploads returns the most recent uploads
func (db *DB) ListRecentUploads(limit int) ([]*Upload, error) {
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list recent uploads: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// ListUploadsPaginated returns uploads with pagination, sorted by active status first
// Excludes completed uploads (they go to history)
func (db *DB) ListUploadsPaginated(limit, offset int) ([]*Upload, error) {
	// Sort: uploading first, then queued, then pending, then failed
	// Completed uploads are excluded from the queue view
	// Within each status group, sort by created_at DESC
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE status != ?
		ORDER BY
			CASE status
				WHEN 'uploading' THEN 1
				WHEN 'queued' THEN 2
				WHEN 'pending' THEN 3
				WHEN 'failed' THEN 4
				ELSE 5
			END,
			created_at DESC
		LIMIT ? OFFSET ?
	`, UploadStatusCompleted, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list uploads paginated: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// ListUploadsByStatus returns uploads with a specific status
func (db *DB) ListUploadsByStatus(status UploadStatus, limit int) ([]*Upload, error) {
	rows, err := db.Query(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE status = ?
		ORDER BY created_at DESC
		LIMIT ?
	`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list uploads by status: %w", err)
	}
	defer rows.Close()

	return db.uploadRowsToUploads(rows)
}

// UpdateUploadStatus updates the status of an upload
func (db *DB) UpdateUploadStatus(id int64, status UploadStatus) error {
	var err error
	switch status {
	case UploadStatusUploading:
		_, err = db.Exec(`UPDATE uploads SET status = ?, started_at = ? WHERE id = ?`, status, time.Now(), id)
	case UploadStatusCompleted:
		_, err = db.Exec(`UPDATE uploads SET status = ?, completed_at = ? WHERE id = ?`, status, time.Now(), id)
	case UploadStatusFailed:
		_, err = db.Exec(`UPDATE uploads SET status = ?, retry_count = retry_count + 1 WHERE id = ?`, status, id)
	default:
		_, err = db.Exec(`UPDATE uploads SET status = ? WHERE id = ?`, status, id)
	}
	if err != nil {
		return fmt.Errorf("failed to update upload status: %w", err)
	}
	return nil
}

// UpdateUploadWaitState updates the wait state and check details for an upload.
func (db *DB) UpdateUploadWaitState(id int64, waitState string, waitChecks string) error {
	_, err := db.Exec(`UPDATE uploads SET wait_state = ?, wait_checks = ? WHERE id = ?`, waitState, waitChecks, id)
	if err != nil {
		return fmt.Errorf("failed to update upload wait state: %w", err)
	}
	return nil
}

// UpdateUploadProgress updates the progress of an upload
func (db *DB) UpdateUploadProgress(id int64, progressBytes int64, rcloneJobID *int64) error {
	_, err := db.Exec(`UPDATE uploads SET progress_bytes = ?, rclone_job_id = ? WHERE id = ?`, progressBytes, rcloneJobID, id)
	if err != nil {
		return fmt.Errorf("failed to update upload progress: %w", err)
	}
	return nil
}

// UpdateUploadError updates the error message for an upload
func (db *DB) UpdateUploadError(id int64, errMsg string) error {
	_, err := db.Exec(`UPDATE uploads SET last_error = ?, status = ?, retry_count = retry_count + 1 WHERE id = ?`, errMsg, UploadStatusFailed, id)
	if err != nil {
		return fmt.Errorf("failed to update upload error: %w", err)
	}
	return nil
}

// UpdateUploadRemote updates the remote for failover
func (db *DB) UpdateUploadRemote(id int64, remoteName, remotePath string, remotePriority int) error {
	_, err := db.Exec(`UPDATE uploads SET remote_name = ?, remote_path = ?, remote_priority = ?, retry_count = 0, status = ? WHERE id = ?`,
		remoteName, remotePath, remotePriority, UploadStatusQueued, id)
	if err != nil {
		return fmt.Errorf("failed to update upload remote: %w", err)
	}
	return nil
}

// DeleteUpload deletes an upload by ID
func (db *DB) DeleteUpload(id int64) error {
	result, err := db.Exec("DELETE FROM uploads WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete upload: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("upload not found")
	}

	return nil
}

// FindDuplicateUpload checks if an upload for the same path/remote exists
func (db *DB) FindDuplicateUpload(localPath, remoteName string) (*Upload, error) {
	upload := &Upload{}
	var scanID, sizeBytes, rcloneJobID sql.NullInt64
	var startedAt, completedAt sql.NullTime
	var lastError sql.NullString

	err := db.QueryRow(`
		SELECT id, scan_id, local_path, remote_name, remote_path, status, size_bytes,
		       wait_state, wait_checks, created_at, started_at, completed_at, rclone_job_id, progress_bytes,
		       retry_count, last_error, remote_priority
		FROM uploads
		WHERE local_path = ? AND remote_name = ? AND status IN (?, ?, ?)
		ORDER BY created_at DESC
		LIMIT 1
	`, localPath, remoteName, UploadStatusQueued, UploadStatusPending, UploadStatusUploading).Scan(
		&upload.ID, &scanID, &upload.LocalPath, &upload.RemoteName, &upload.RemotePath,
		&upload.Status, &sizeBytes, &upload.WaitState, &upload.WaitChecks, &upload.CreatedAt, &startedAt, &completedAt,
		&rcloneJobID, &upload.ProgressBytes, &upload.RetryCount, &lastError, &upload.RemotePriority)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find duplicate upload: %w", err)
	}

	upload.ScanID = nullInt64ToPtr(scanID)
	upload.SizeBytes = nullInt64ToPtr(sizeBytes)
	upload.StartedAt = nullTimeToPtr(startedAt)
	upload.CompletedAt = nullTimeToPtr(completedAt)
	upload.RcloneJobID = nullInt64ToPtr(rcloneJobID)
	upload.LastError = nullStringValue(lastError)

	return upload, nil
}

// CountUploads returns the count of uploads by status
func (db *DB) CountUploads(status UploadStatus) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status = ?", status).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count uploads: %w", err)
	}
	return count, nil
}

// CountAllUploads returns the total number of uploads (excluding completed)
func (db *DB) CountAllUploads() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status != ?", UploadStatusCompleted).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count all uploads: %w", err)
	}
	return count, nil
}

// DeleteCompletedUploads deletes all completed uploads
func (db *DB) DeleteCompletedUploads() (int64, error) {
	result, err := db.Exec("DELETE FROM uploads WHERE status = ?", UploadStatusCompleted)
	if err != nil {
		return 0, fmt.Errorf("failed to delete completed uploads: %w", err)
	}
	return result.RowsAffected()
}

// DeleteOldUploads deletes uploads older than the given duration
func (db *DB) DeleteOldUploads(age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	result, err := db.Exec(`DELETE FROM uploads WHERE created_at < ? AND status IN (?, ?)`, cutoff, UploadStatusCompleted, UploadStatusFailed)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old uploads: %w", err)
	}
	return result.RowsAffected()
}

// ========== UploadHistory CRUD ==========

// CreateUploadHistory creates a new upload history record
func (db *DB) CreateUploadHistory(history *UploadHistory) error {
	result, err := db.Exec(`
		INSERT INTO upload_history (upload_id, local_path, remote_name, remote_path, size_bytes, completed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, history.UploadID, history.LocalPath, history.RemoteName, history.RemotePath, history.SizeBytes, time.Now())
	if err != nil {
		return fmt.Errorf("failed to create upload history: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	history.ID = id
	history.CompletedAt = time.Now()

	return nil
}

// ListUploadHistory retrieves upload history with pagination
func (db *DB) ListUploadHistory(limit, offset int) ([]*UploadHistory, error) {
	return db.ListUploadHistoryFiltered("", limit, offset)
}

// ListUploadHistoryFiltered lists uploads with optional remote filter.
func (db *DB) ListUploadHistoryFiltered(remote string, limit, offset int) ([]*UploadHistory, error) {
	rows, err := db.Query(`
		SELECT id, upload_id, local_path, remote_name, remote_path, size_bytes, completed_at
		FROM upload_history
		WHERE (? = '' OR remote_name = ?)
		ORDER BY completed_at DESC
		LIMIT ? OFFSET ?
	`, remote, remote, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list upload history: %w", err)
	}
	defer rows.Close()

	var histories []*UploadHistory
	for rows.Next() {
		history := &UploadHistory{}
		var uploadID, sizeBytes sql.NullInt64

		if err := rows.Scan(&history.ID, &uploadID, &history.LocalPath, &history.RemoteName, &history.RemotePath, &sizeBytes, &history.CompletedAt); err != nil {
			return nil, fmt.Errorf("failed to scan upload history: %w", err)
		}

		history.UploadID = nullInt64ToPtr(uploadID)
		history.SizeBytes = nullInt64ToPtr(sizeBytes)

		histories = append(histories, history)
	}

	return histories, nil
}

// GetUploadStats returns aggregate upload statistics
func (db *DB) GetUploadStats() (totalUploads int, totalBytes int64, err error) {
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
		FROM upload_history
	`).Scan(&totalUploads, &totalBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get upload stats: %w", err)
	}
	return totalUploads, totalBytes, nil
}

// CountUploadHistory returns the total number of upload history records
func (db *DB) CountUploadHistory() (int, error) {
	return db.CountUploadHistoryFiltered("")
}

// CountUploadHistoryFiltered returns the number of upload history records filtered by remote name (optional)
func (db *DB) CountUploadHistoryFiltered(remote string) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM upload_history WHERE (? = '' OR remote_name = ?)", remote, remote).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count upload history: %w", err)
	}
	return count, nil
}

// ClearUploadHistory deletes all upload history records
func (db *DB) ClearUploadHistory() (int64, error) {
	result, err := db.Exec("DELETE FROM upload_history")
	if err != nil {
		return 0, fmt.Errorf("failed to clear upload history: %w", err)
	}
	return result.RowsAffected()
}

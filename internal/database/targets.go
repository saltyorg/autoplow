package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// TargetType represents the type of media server target
type TargetType string

const (
	TargetTypePlex     TargetType = "plex"
	TargetTypeEmby     TargetType = "emby"
	TargetTypeJellyfin TargetType = "jellyfin"
	TargetTypeAutoplow TargetType = "autoplow"
	TargetTypeTdarr    TargetType = "tdarr"
)

// Target represents a media server target for scan notifications
type Target struct {
	ID              int64        `json:"id"`
	Name            string       `json:"name"`
	Type            TargetType   `json:"type"`
	URL             string       `json:"url"`
	Token           string       `json:"token,omitempty"`
	APIKey          string       `json:"api_key,omitempty"`
	Enabled         bool         `json:"enabled"`
	MatcharrEnabled bool         `json:"matcharr_enabled"`
	Config          TargetConfig `json:"config"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

// TargetPathMapping represents a path mapping rule for media server scans
type TargetPathMapping struct {
	From string `json:"from"` // Local path prefix (how autoplow sees it)
	To   string `json:"to"`   // Media server path prefix (how the media server sees it)
}

// MetadataRefreshMode represents the metadata refresh mode for Emby/Jellyfin
type MetadataRefreshMode string

const (
	// MetadataRefreshModeNone - No metadata refresh
	MetadataRefreshModeNone MetadataRefreshMode = "None"
	// MetadataRefreshModeValidationOnly - Only validate existing metadata
	MetadataRefreshModeValidationOnly MetadataRefreshMode = "ValidationOnly"
	// MetadataRefreshModeDefault - Default refresh behavior
	MetadataRefreshModeDefault MetadataRefreshMode = "Default"
	// MetadataRefreshModeFullRefresh - Full metadata refresh
	MetadataRefreshModeFullRefresh MetadataRefreshMode = "FullRefresh"
)

// SessionMode represents how session monitoring is performed for a target
type SessionMode string

const (
	// SessionModePolling - Use HTTP polling to check for sessions
	SessionModePolling SessionMode = "polling"
	// SessionModeWebSocket - Use WebSocket for real-time session updates
	SessionModeWebSocket SessionMode = "websocket"
)

// TargetConfig holds target-specific configuration
type TargetConfig struct {
	// Plex specific
	PlexSections []int `json:"plex_sections,omitempty"`
	// AnalyzeMedia triggers Plex to analyze media files after scanning (Plex only)
	AnalyzeMedia bool `json:"analyze_media,omitempty"`

	// Emby/Jellyfin specific
	LibraryIDs []string `json:"library_ids,omitempty"`

	// Tdarr specific
	TdarrDBID string `json:"tdarr_db_id,omitempty"` // Tdarr library/database ID

	// Common options
	RefreshMetadata bool `json:"refresh_metadata,omitempty"`
	MaxRetries      int  `json:"max_retries,omitempty"`
	RetryDelay      int  `json:"retry_delay,omitempty"` // seconds

	// Emby/Jellyfin metadata refresh mode (None, ValidationOnly, Default, FullRefresh)
	// Only used when RefreshMetadata is true
	MetadataRefreshMode MetadataRefreshMode `json:"metadata_refresh_mode,omitempty"`

	// Path mappings for converting local paths to media server paths
	// Used when the media server sees files at different paths than autoplow
	// (e.g., different mount points, rclone on different host, etc.)
	PathMappings []TargetPathMapping `json:"path_mappings,omitempty"`

	// ScanDelay is an additional delay (in seconds) applied before scanning this target
	// This delay is applied after the scan is otherwise ready to be processed
	// Defaults to 0 (no additional delay)
	ScanDelay int `json:"scan_delay,omitempty"`

	// ExcludePaths is a list of path prefixes to exclude from scanning on this target
	// If a scan path starts with any of these prefixes, it will be skipped for this target
	// This allows filtering scans on a per-target basis
	ExcludePaths []string `json:"exclude_paths,omitempty"`

	// ExcludeExtensions is a list of file extensions to exclude (e.g., ".nfo", ".txt")
	// Extensions are case-insensitive and will be normalized to include the leading dot
	ExcludeExtensions []string `json:"exclude_extensions,omitempty"`

	// AdvancedFilters provides regex-based filtering for full control
	// These are applied after basic path prefix and extension filters
	AdvancedFilters *AdvancedFilters `json:"advanced_filters,omitempty"`

	// ExcludeTriggers is a list of trigger names to ignore for this target
	// If a scan originates from a trigger in this list, it will be skipped for this target
	// This allows configuring which triggers should notify which targets
	ExcludeTriggers []string `json:"exclude_triggers,omitempty"`

	// SessionMode controls how playback sessions are monitored for this target
	// "websocket" (default) uses real-time WebSocket connection for instant updates
	// "polling" uses HTTP polling at the global poll interval
	// Only applies to Plex, Emby, and Jellyfin targets
	SessionMode SessionMode `json:"session_mode,omitempty"`

	// ScanCompletionSeconds controls how long to wait before allowing uploads after a scan.
	// This is the upload delay - uploads are queued after all target waits complete in parallel.
	// For Plex targets, uses smart completion detection via WebSocket activity monitoring
	// which can complete early when the scan finishes.
	// For other targets, this is a fixed delay.
	// Defaults to 0 (no wait) if not set.
	ScanCompletionSeconds int `json:"scan_completion_seconds,omitempty"`

	// ScanCompletionIdleSeconds is the idle time (no Plex activity) before considering scan complete.
	// Only applies to Plex targets with smart completion detection.
	// The idle timer resets on each activity event, so this waits for all analysis tasks to finish.
	// Defaults to 10 if not set or 0.
	ScanCompletionIdleSeconds int `json:"scan_completion_idle_seconds,omitempty"`
}

// ShouldExcludePath checks if a path should be excluded from scanning on this target
// Returns true if the path should be skipped, false if it should be processed
func (c *TargetConfig) ShouldExcludePath(path string) bool {
	for _, prefix := range c.ExcludePaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// ShouldExcludeExtension checks if a file should be excluded based on its extension
// Returns true if the extension should be skipped, false if it should be processed
func (c *TargetConfig) ShouldExcludeExtension(path string) bool {
	if len(c.ExcludeExtensions) == 0 {
		return false
	}
	ext := strings.ToLower(filepath.Ext(path))
	for _, excludeExt := range c.ExcludeExtensions {
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

// ShouldExcludeByAdvancedFilters checks if a path should be excluded based on regex patterns
// Returns true if the path should be skipped, false if it should be processed
func (c *TargetConfig) ShouldExcludeByAdvancedFilters(path string) bool {
	if c.AdvancedFilters == nil {
		return false
	}
	return !c.AdvancedFilters.Matches(path)
}

// ShouldExclude checks all filter rules (path prefix, extension, and advanced filters)
// Returns true if the path should be skipped for this target
func (c *TargetConfig) ShouldExclude(path string) bool {
	return c.ShouldExcludePath(path) ||
		c.ShouldExcludeExtension(path) ||
		c.ShouldExcludeByAdvancedFilters(path)
}

// CompileAdvancedFilters compiles the regex patterns in AdvancedFilters
// This should be called after loading a target from the database
func (c *TargetConfig) CompileAdvancedFilters() error {
	if c.AdvancedFilters != nil {
		return c.AdvancedFilters.Compile()
	}
	return nil
}

// ShouldExcludeTrigger checks if a trigger should be excluded from scanning on this target
// Returns true if the trigger should be skipped, false if it should be processed
func (c *TargetConfig) ShouldExcludeTrigger(triggerName string) bool {
	return slices.Contains(c.ExcludeTriggers, triggerName)
}

// CreateTarget creates a new media server target
func (db *DB) CreateTarget(t *Target) error {
	configJSON, err := json.Marshal(t.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	result, err := db.Exec(`
		INSERT INTO targets (name, type, url, token, api_key, enabled, matcharr_enabled, config)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, t.Name, t.Type, t.URL, t.Token, t.APIKey, t.Enabled, t.MatcharrEnabled, string(configJSON))
	if err != nil {
		return fmt.Errorf("failed to create target: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get target id: %w", err)
	}
	t.ID = id

	return nil
}

// GetTarget retrieves a target by ID
func (db *DB) GetTarget(id int64) (*Target, error) {
	t := &Target{}
	var configJSON string
	var matcharrEnabled sql.NullBool

	err := db.QueryRow(`
		SELECT id, name, type, url, token, api_key, enabled, matcharr_enabled, config, created_at, updated_at
		FROM targets WHERE id = ?
	`, id).Scan(
		&t.ID, &t.Name, &t.Type, &t.URL, &t.Token, &t.APIKey,
		&t.Enabled, &matcharrEnabled, &configJSON, &t.CreatedAt, &t.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get target: %w", err)
	}

	t.MatcharrEnabled = matcharrEnabled.Valid && matcharrEnabled.Bool

	if err := json.Unmarshal([]byte(configJSON), &t.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Compile advanced filter regex patterns
	if err := t.Config.CompileAdvancedFilters(); err != nil {
		return nil, fmt.Errorf("failed to compile advanced filters: %w", err)
	}

	return t, nil
}

// ListTargets returns all targets
func (db *DB) ListTargets() ([]*Target, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, token, api_key, enabled, matcharr_enabled, config, created_at, updated_at
		FROM targets ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list targets: %w", err)
	}
	defer rows.Close()

	var targets []*Target
	for rows.Next() {
		t := &Target{}
		var configJSON string
		var matcharrEnabled sql.NullBool

		if err := rows.Scan(
			&t.ID, &t.Name, &t.Type, &t.URL, &t.Token, &t.APIKey,
			&t.Enabled, &matcharrEnabled, &configJSON, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan target: %w", err)
		}

		t.MatcharrEnabled = matcharrEnabled.Valid && matcharrEnabled.Bool

		if err := json.Unmarshal([]byte(configJSON), &t.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Compile advanced filter regex patterns
		if err := t.Config.CompileAdvancedFilters(); err != nil {
			return nil, fmt.Errorf("failed to compile advanced filters for target %d: %w", t.ID, err)
		}

		targets = append(targets, t)
	}

	return targets, nil
}

// ListEnabledTargets returns all enabled targets
func (db *DB) ListEnabledTargets() ([]*Target, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, token, api_key, enabled, matcharr_enabled, config, created_at, updated_at
		FROM targets WHERE enabled = true ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled targets: %w", err)
	}
	defer rows.Close()

	var targets []*Target
	for rows.Next() {
		t := &Target{}
		var configJSON string
		var matcharrEnabled sql.NullBool

		if err := rows.Scan(
			&t.ID, &t.Name, &t.Type, &t.URL, &t.Token, &t.APIKey,
			&t.Enabled, &matcharrEnabled, &configJSON, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan target: %w", err)
		}

		t.MatcharrEnabled = matcharrEnabled.Valid && matcharrEnabled.Bool

		if err := json.Unmarshal([]byte(configJSON), &t.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Compile advanced filter regex patterns
		if err := t.Config.CompileAdvancedFilters(); err != nil {
			return nil, fmt.Errorf("failed to compile advanced filters for target %d: %w", t.ID, err)
		}

		targets = append(targets, t)
	}

	return targets, nil
}

// ListMatcharrEnabledTargets returns all targets that are both enabled and have matcharr enabled
func (db *DB) ListMatcharrEnabledTargets() ([]*Target, error) {
	rows, err := db.Query(`
		SELECT id, name, type, url, token, api_key, enabled, matcharr_enabled, config, created_at, updated_at
		FROM targets WHERE enabled = true AND matcharr_enabled = true ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list matcharr enabled targets: %w", err)
	}
	defer rows.Close()

	var targets []*Target
	for rows.Next() {
		t := &Target{}
		var configJSON string
		var matcharrEnabled sql.NullBool

		if err := rows.Scan(
			&t.ID, &t.Name, &t.Type, &t.URL, &t.Token, &t.APIKey,
			&t.Enabled, &matcharrEnabled, &configJSON, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan target: %w", err)
		}

		t.MatcharrEnabled = matcharrEnabled.Valid && matcharrEnabled.Bool

		if err := json.Unmarshal([]byte(configJSON), &t.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Compile advanced filter regex patterns
		if err := t.Config.CompileAdvancedFilters(); err != nil {
			return nil, fmt.Errorf("failed to compile advanced filters for target %d: %w", t.ID, err)
		}

		targets = append(targets, t)
	}

	return targets, nil
}

// UpdateTarget updates an existing target
func (db *DB) UpdateTarget(t *Target) error {
	configJSON, err := json.Marshal(t.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	result, err := db.Exec(`
		UPDATE targets SET
			name = ?, type = ?, url = ?, token = ?, api_key = ?,
			enabled = ?, matcharr_enabled = ?, config = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, t.Name, t.Type, t.URL, t.Token, t.APIKey, t.Enabled, t.MatcharrEnabled, string(configJSON), t.ID)
	if err != nil {
		return fmt.Errorf("failed to update target: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("target not found: %d", t.ID)
	}

	return nil
}

// DeleteTarget deletes a target by ID
func (db *DB) DeleteTarget(id int64) error {
	result, err := db.Exec("DELETE FROM targets WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete target: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("target not found: %d", id)
	}

	return nil
}

// TargetLibrary represents a cached library path for a target
type TargetLibrary struct {
	ID        int64
	TargetID  int64
	LibraryID string
	Name      string
	Type      string
	Path      string
	FetchedAt time.Time
}

// GetCachedLibraries returns cached libraries for a target.
// Returns nil if cache is older than maxAge or doesn't exist.
func (db *DB) GetCachedLibraries(targetID int64, maxAge time.Duration) ([]TargetLibrary, error) {
	cutoff := time.Now().Add(-maxAge)

	rows, err := db.Query(`
		SELECT id, target_id, library_id, name, type, path, fetched_at
		FROM target_libraries
		WHERE target_id = ? AND fetched_at > ?
		ORDER BY name, path
	`, targetID, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to get cached libraries: %w", err)
	}
	defer rows.Close()

	var libraries []TargetLibrary
	for rows.Next() {
		var lib TargetLibrary
		var libType sql.NullString
		if err := rows.Scan(&lib.ID, &lib.TargetID, &lib.LibraryID, &lib.Name, &libType, &lib.Path, &lib.FetchedAt); err != nil {
			return nil, fmt.Errorf("failed to scan library: %w", err)
		}
		lib.Type = libType.String
		libraries = append(libraries, lib)
	}

	if len(libraries) == 0 {
		return nil, nil
	}

	return libraries, nil
}

// GetCachedLibraryPaths returns just the paths from cached libraries for a target.
// Returns nil if cache is older than maxAge or doesn't exist.
func (db *DB) GetCachedLibraryPaths(targetID int64, maxAge time.Duration) ([]string, error) {
	cutoff := time.Now().Add(-maxAge)

	rows, err := db.Query(`
		SELECT DISTINCT path
		FROM target_libraries
		WHERE target_id = ? AND fetched_at > ?
		ORDER BY path
	`, targetID, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to get cached library paths: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed to scan path: %w", err)
		}
		paths = append(paths, path)
	}

	return paths, nil
}

// SetCachedLibraries replaces cached libraries for a target.
func (db *DB) SetCachedLibraries(targetID int64, libraries []TargetLibrary) error {
	return db.Transaction(func(tx *sql.Tx) error {
		// Delete existing cached libraries for this target
		if _, err := tx.Exec("DELETE FROM target_libraries WHERE target_id = ?", targetID); err != nil {
			return fmt.Errorf("failed to delete old cached libraries: %w", err)
		}

		// Insert new libraries
		now := time.Now()
		for _, lib := range libraries {
			_, err := tx.Exec(`
				INSERT INTO target_libraries (target_id, library_id, name, type, path, fetched_at)
				VALUES (?, ?, ?, ?, ?, ?)
			`, targetID, lib.LibraryID, lib.Name, lib.Type, lib.Path, now)
			if err != nil {
				return fmt.Errorf("failed to insert library: %w", err)
			}
		}

		return nil
	})
}

// DeleteCachedLibraries removes cached libraries for a target.
func (db *DB) DeleteCachedLibraries(targetID int64) error {
	_, err := db.Exec("DELETE FROM target_libraries WHERE target_id = ?", targetID)
	if err != nil {
		return fmt.Errorf("failed to delete cached libraries: %w", err)
	}
	return nil
}

// GetLibraryCacheAge returns the age of the oldest cached library for a target.
// Returns 0 if no cached libraries exist.
func (db *DB) GetLibraryCacheAge(targetID int64) (time.Duration, error) {
	var fetchedAt sql.NullTime
	err := db.QueryRow(`
		SELECT MIN(fetched_at)
		FROM target_libraries
		WHERE target_id = ?
	`, targetID).Scan(&fetchedAt)
	if err != nil {
		return 0, fmt.Errorf("failed to get cache age: %w", err)
	}

	if !fetchedAt.Valid {
		return 0, nil
	}

	return time.Since(fetchedAt.Time), nil
}

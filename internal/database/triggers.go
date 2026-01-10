package database

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// TriggerType represents the type of trigger
type TriggerType string

const (
	TriggerTypeSonarr   TriggerType = "sonarr"
	TriggerTypeRadarr   TriggerType = "radarr"
	TriggerTypeLidarr   TriggerType = "lidarr"
	TriggerTypeWebhook  TriggerType = "webhook"
	TriggerTypeInotify  TriggerType = "inotify"
	TriggerTypePolling  TriggerType = "polling"
	TriggerTypeAutoplow TriggerType = "autoplow"
	TriggerTypeATrain   TriggerType = "a_train"
	TriggerTypeAutoscan TriggerType = "autoscan"
	TriggerTypeBazarr   TriggerType = "bazarr"
)

// AuthType represents the authentication method for a trigger
type AuthType string

const (
	// AuthTypeAPIKey uses a generated API key for authentication (default)
	AuthTypeAPIKey AuthType = "api_key"
	// AuthTypeBasic uses username/password (HTTP Basic Auth) for authentication
	AuthTypeBasic AuthType = "basic"
)

// FilesystemType represents whether files are on local or remote storage
type FilesystemType string

const (
	// FilesystemTypeLocal indicates local filesystem with stability checking
	FilesystemTypeLocal FilesystemType = "local"
	// FilesystemTypeRemote indicates remote filesystem (dumb delay, no stability checking)
	FilesystemTypeRemote FilesystemType = "remote"
)

// TriggerConfig holds the JSON configuration for a trigger
type TriggerConfig struct {
	// Path rewrites for this trigger (from -> to)
	// Note: Not applicable for local triggers (inotify, polling) as paths are already local
	PathRewrites []PathRewrite `json:"path_rewrites,omitempty"`

	// For inotify/polling triggers: paths to watch
	WatchPaths []string `json:"watch_paths,omitempty"`

	// Debounce delay for inotify events (seconds)
	DebounceSeconds int `json:"debounce_seconds,omitempty"`

	// For polling triggers: interval between directory scans (seconds, min 10)
	PollIntervalSeconds int `json:"poll_interval_seconds,omitempty"`

	// For polling triggers: whether to queue all existing files on first scan
	// Default is true (queue all files found on the initial poll)
	QueueExistingOnStart bool `json:"queue_existing_on_start,omitempty"`

	// For inotify triggers: whether to scan existing files on startup
	// Default is false
	ScanExistingOnStart bool `json:"scan_existing_on_start,omitempty"`

	// For inotify/polling triggers: whether to trigger scans (default true)
	ScanEnabled *bool `json:"scan_enabled,omitempty"`

	// For inotify/polling triggers: whether to trigger uploads (default true)
	UploadEnabled *bool `json:"upload_enabled,omitempty"`

	// Filesystem type: "local" for local storage with stability checking,
	// "remote" for remote/network storage with dumb delay
	FilesystemType FilesystemType `json:"filesystem_type,omitempty"`

	// Stability check interval in seconds for local filesystem (min 1s)
	// Only used when FilesystemType is "local"
	StabilityCheckSeconds int `json:"stability_check_seconds,omitempty"`

	// Minimum age in seconds before processing (min 60s for remote, min 1s for local)
	// If not set, uses the global processor MinimumAgeSeconds
	MinimumAgeSeconds int `json:"minimum_age_seconds,omitempty"`

	// IncludePaths is a list of path prefixes that this trigger will accept
	// If empty, all paths are accepted (default behavior)
	// If non-empty, only paths matching at least one prefix will be processed
	IncludePaths []string `json:"include_paths,omitempty"`

	// ExcludePaths is a list of path prefixes that this trigger will reject
	// Paths matching any of these prefixes will be skipped
	// Exclude rules are applied after include rules
	ExcludePaths []string `json:"exclude_paths,omitempty"`

	// ExcludeExtensions is a list of file extensions to exclude (e.g., ".nfo", ".txt")
	// Extensions are case-insensitive and will be normalized to include the leading dot
	ExcludeExtensions []string `json:"exclude_extensions,omitempty"`

	// AdvancedFilters provides regex-based filtering for full control
	// These are applied after basic path prefix and extension filters
	AdvancedFilters *AdvancedFilters `json:"advanced_filters,omitempty"`

	// FilterAfterRewrite controls when path filters (include/exclude) are applied
	// Default (false): filters are applied BEFORE path rewrites (on original paths)
	// When true: filters are applied AFTER path rewrites (on rewritten paths)
	FilterAfterRewrite bool `json:"filter_after_rewrite,omitempty"`
}

// AdvancedFilters provides regex-based include/exclude filtering
type AdvancedFilters struct {
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
func (af *AdvancedFilters) Compile() error {
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
func (af *AdvancedFilters) Matches(path string) bool {
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

// PathRewrite represents a path rewrite rule
type PathRewrite struct {
	From string `json:"from"`
	To   string `json:"to"`
	// IsRegex indicates whether From is a regex pattern (with capture groups in To)
	// When true: From is a regex, To can use $1, $2, etc. for capture groups
	// When false: From is a simple prefix to replace with To
	IsRegex bool `json:"is_regex,omitempty"`

	// Compiled regex (not serialized, populated on load)
	compiledRegex *regexp.Regexp `json:"-"`
}

// CompileRegex compiles the regex pattern if IsRegex is true
// Returns an error if the pattern is invalid
func (pr *PathRewrite) CompileRegex() error {
	if !pr.IsRegex {
		pr.compiledRegex = nil
		return nil
	}
	re, err := regexp.Compile(pr.From)
	if err != nil {
		return fmt.Errorf("invalid regex pattern %q: %w", pr.From, err)
	}
	pr.compiledRegex = re
	return nil
}

// Rewrite applies this rewrite rule to a path
// Returns the rewritten path
func (pr *PathRewrite) Rewrite(path string) string {
	if pr.IsRegex && pr.compiledRegex != nil {
		return pr.compiledRegex.ReplaceAllString(path, pr.To)
	}
	// Simple prefix replacement
	if after, ok := strings.CutPrefix(path, pr.From); ok {
		return pr.To + after
	}
	return path
}

// MatchesPathFilters checks if a path should be processed based on include/exclude rules
// Returns true if the path should be processed, false if it should be skipped
func (c *TriggerConfig) MatchesPathFilters(path string) bool {
	// If include paths are specified, check if path matches at least one
	if len(c.IncludePaths) > 0 {
		matched := false
		for _, prefix := range c.IncludePaths {
			if strings.HasPrefix(path, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check exclude paths - if path matches any, skip it
	for _, prefix := range c.ExcludePaths {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}

	// Check exclude extensions - if file extension matches any, skip it
	if len(c.ExcludeExtensions) > 0 {
		ext := strings.ToLower(filepath.Ext(path))
		for _, excludeExt := range c.ExcludeExtensions {
			// Normalize the exclude extension (ensure it starts with .)
			normalizedExt := strings.ToLower(excludeExt)
			if !strings.HasPrefix(normalizedExt, ".") {
				normalizedExt = "." + normalizedExt
			}
			if ext == normalizedExt {
				return false
			}
		}
	}

	// Check advanced filters (regex-based)
	if c.AdvancedFilters != nil {
		if !c.AdvancedFilters.Matches(path) {
			return false
		}
	}

	return true
}

// CompileAdvancedFilters compiles the regex patterns in AdvancedFilters
// This should be called after loading a trigger from the database
func (c *TriggerConfig) CompileAdvancedFilters() error {
	if c.AdvancedFilters != nil {
		return c.AdvancedFilters.Compile()
	}
	return nil
}

// CompilePathRewrites compiles all regex-based path rewrite patterns
// This should be called after loading a trigger from the database
func (c *TriggerConfig) CompilePathRewrites() error {
	for i := range c.PathRewrites {
		if err := c.PathRewrites[i].CompileRegex(); err != nil {
			return err
		}
	}
	return nil
}

// CompileAll compiles all regex patterns (advanced filters and path rewrites)
// This should be called after loading a trigger from the database
func (c *TriggerConfig) CompileAll() error {
	if err := c.CompileAdvancedFilters(); err != nil {
		return err
	}
	if err := c.CompilePathRewrites(); err != nil {
		return err
	}
	return nil
}

// ScanEnabledValue returns true when scanning is enabled for this trigger.
// Defaults to true when unset.
func (c TriggerConfig) ScanEnabledValue() bool {
	if c.ScanEnabled == nil {
		return true
	}
	return *c.ScanEnabled
}

// UploadEnabledValue returns true when uploads are enabled for this trigger.
// Defaults to true when unset.
func (c TriggerConfig) UploadEnabledValue() bool {
	if c.UploadEnabled == nil {
		return true
	}
	return *c.UploadEnabled
}

// Trigger represents a webhook or inotify trigger
type Trigger struct {
	ID        int64         `json:"id"`
	Name      string        `json:"name"`
	Type      TriggerType   `json:"type"`
	AuthType  AuthType      `json:"auth_type"`          // "api_key" or "basic"
	APIKey    string        `json:"api_key,omitempty"`  // Used when AuthType is "api_key"
	Username  string        `json:"username,omitempty"` // Used when AuthType is "basic"
	Password  string        `json:"-"`                  // Used when AuthType is "basic", stored as plain text for webhook auth
	Priority  int           `json:"priority"`
	Enabled   bool          `json:"enabled"`
	Config    TriggerConfig `json:"config"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// WebhookURL returns the full webhook URL for this trigger
func (t *Trigger) WebhookURL(baseURL string) string {
	if t.Type == TriggerTypeInotify {
		return ""
	}
	if t.AuthType == AuthTypeBasic {
		// For basic auth, don't include credentials in URL
		return fmt.Sprintf("%s/api/triggers/%s/%d", baseURL, t.Type, t.ID)
	}
	return fmt.Sprintf("%s/api/triggers/%s/%d?api_key=%s", baseURL, t.Type, t.ID, t.APIKey)
}

// CreateTrigger creates a new trigger
func (db *db) CreateTrigger(trigger *Trigger) error {
	configJSON, err := marshalToString(trigger.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Default auth type to api_key if not set
	if trigger.AuthType == "" {
		trigger.AuthType = AuthTypeAPIKey
	}

	// Convert empty strings to NULL for optional fields
	var apiKey, username, passwordHash sql.NullString
	if trigger.APIKey != "" {
		apiKey = sql.NullString{String: trigger.APIKey, Valid: true}
	}
	if trigger.Username != "" {
		username = sql.NullString{String: trigger.Username, Valid: true}
	}
	if trigger.Password != "" {
		passwordHash = sql.NullString{String: trigger.Password, Valid: true}
	}

	result, err := db.exec(`
		INSERT INTO triggers (name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, trigger.Name, trigger.Type, trigger.AuthType, apiKey, username, passwordHash, trigger.Priority, trigger.Enabled, configJSON, time.Now(), time.Now())
	if err != nil {
		return fmt.Errorf("failed to create trigger: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	trigger.ID = id

	return nil
}

// GetTrigger retrieves a trigger by ID
func (db *db) GetTrigger(id int64) (*Trigger, error) {
	var trigger Trigger
	var configJSON string
	var apiKey, authType, username, passwordHash sql.NullString

	err := db.queryRow(`
		SELECT id, name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at
		FROM triggers WHERE id = ?
	`, id).Scan(&trigger.ID, &trigger.Name, &trigger.Type, &authType, &apiKey, &username, &passwordHash, &trigger.Priority, &trigger.Enabled, &configJSON, &trigger.CreatedAt, &trigger.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get trigger: %w", err)
	}

	// Set auth type, defaulting to api_key for backward compatibility
	if authType.Valid && authType.String != "" {
		trigger.AuthType = AuthType(authType.String)
	} else {
		trigger.AuthType = AuthTypeAPIKey
	}

	if apiKey.Valid {
		trigger.APIKey = apiKey.String
	}
	if username.Valid {
		trigger.Username = username.String
	}
	if passwordHash.Valid {
		trigger.Password = passwordHash.String
	}

	if err := unmarshalFromString(configJSON, &trigger.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Compile all regex patterns (advanced filters and path rewrites)
	if err := trigger.Config.CompileAll(); err != nil {
		return nil, fmt.Errorf("failed to compile regex patterns: %w", err)
	}

	return &trigger, nil
}

// GetTriggerByAPIKey retrieves a trigger by its API key
func (db *db) GetTriggerByAPIKey(apiKey string) (*Trigger, error) {
	var trigger Trigger
	var configJSON string
	var key, authType, username, passwordHash sql.NullString

	err := db.queryRow(`
		SELECT id, name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at
		FROM triggers WHERE api_key = ?
	`, apiKey).Scan(&trigger.ID, &trigger.Name, &trigger.Type, &authType, &key, &username, &passwordHash, &trigger.Priority, &trigger.Enabled, &configJSON, &trigger.CreatedAt, &trigger.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get trigger by api key: %w", err)
	}

	// Set auth type, defaulting to api_key for backward compatibility
	if authType.Valid && authType.String != "" {
		trigger.AuthType = AuthType(authType.String)
	} else {
		trigger.AuthType = AuthTypeAPIKey
	}

	if key.Valid {
		trigger.APIKey = key.String
	}
	if username.Valid {
		trigger.Username = username.String
	}
	if passwordHash.Valid {
		trigger.Password = passwordHash.String
	}

	if err := unmarshalFromString(configJSON, &trigger.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Compile all regex patterns (advanced filters and path rewrites)
	if err := trigger.Config.CompileAll(); err != nil {
		return nil, fmt.Errorf("failed to compile regex patterns: %w", err)
	}

	return &trigger, nil
}

// ListTriggers retrieves all triggers
func (db *db) ListTriggers() ([]*Trigger, error) {
	rows, err := db.query(`
		SELECT id, name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at
		FROM triggers ORDER BY name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list triggers: %w", err)
	}
	defer rows.Close()

	var triggers []*Trigger
	for rows.Next() {
		var trigger Trigger
		var configJSON string
		var apiKey, authType, username, passwordHash sql.NullString

		if err := rows.Scan(&trigger.ID, &trigger.Name, &trigger.Type, &authType, &apiKey, &username, &passwordHash, &trigger.Priority, &trigger.Enabled, &configJSON, &trigger.CreatedAt, &trigger.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan trigger: %w", err)
		}

		// Set auth type, defaulting to api_key for backward compatibility
		if authType.Valid && authType.String != "" {
			trigger.AuthType = AuthType(authType.String)
		} else {
			trigger.AuthType = AuthTypeAPIKey
		}

		if apiKey.Valid {
			trigger.APIKey = apiKey.String
		}
		if username.Valid {
			trigger.Username = username.String
		}
		if passwordHash.Valid {
			trigger.Password = passwordHash.String
		}

		if err := unmarshalFromString(configJSON, &trigger.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Compile all regex patterns (advanced filters and path rewrites)
		if err := trigger.Config.CompileAll(); err != nil {
			return nil, fmt.Errorf("failed to compile regex patterns for trigger %d: %w", trigger.ID, err)
		}

		triggers = append(triggers, &trigger)
	}

	return triggers, nil
}

// ListTriggersByType retrieves triggers of a specific type
func (db *db) ListTriggersByType(triggerType TriggerType) ([]*Trigger, error) {
	rows, err := db.query(`
		SELECT id, name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at
		FROM triggers WHERE type = ? ORDER BY name ASC
	`, triggerType)
	if err != nil {
		return nil, fmt.Errorf("failed to list triggers by type: %w", err)
	}
	defer rows.Close()

	var triggers []*Trigger
	for rows.Next() {
		var trigger Trigger
		var configJSON string
		var apiKey, authType, username, passwordHash sql.NullString

		if err := rows.Scan(&trigger.ID, &trigger.Name, &trigger.Type, &authType, &apiKey, &username, &passwordHash, &trigger.Priority, &trigger.Enabled, &configJSON, &trigger.CreatedAt, &trigger.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan trigger: %w", err)
		}

		// Set auth type, defaulting to api_key for backward compatibility
		if authType.Valid && authType.String != "" {
			trigger.AuthType = AuthType(authType.String)
		} else {
			trigger.AuthType = AuthTypeAPIKey
		}

		if apiKey.Valid {
			trigger.APIKey = apiKey.String
		}
		if username.Valid {
			trigger.Username = username.String
		}
		if passwordHash.Valid {
			trigger.Password = passwordHash.String
		}

		if err := unmarshalFromString(configJSON, &trigger.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Compile all regex patterns (advanced filters and path rewrites)
		if err := trigger.Config.CompileAll(); err != nil {
			return nil, fmt.Errorf("failed to compile regex patterns for trigger %d: %w", trigger.ID, err)
		}

		triggers = append(triggers, &trigger)
	}

	return triggers, nil
}

// UpdateTrigger updates an existing trigger
func (db *db) UpdateTrigger(trigger *Trigger) error {
	configJSON, err := marshalToString(trigger.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	result, err := db.exec(`
		UPDATE triggers SET name = ?, type = ?, priority = ?, enabled = ?, config = ?, updated_at = ?
		WHERE id = ?
	`, trigger.Name, trigger.Type, trigger.Priority, trigger.Enabled, configJSON, time.Now(), trigger.ID)
	if err != nil {
		return fmt.Errorf("failed to update trigger: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("trigger not found")
	}

	return nil
}

// UpdateTriggerAPIKey updates a trigger's API key
func (db *db) UpdateTriggerAPIKey(id int64, apiKey string) error {
	result, err := db.exec(`
		UPDATE triggers SET api_key = ?, updated_at = ? WHERE id = ?
	`, apiKey, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update trigger api key: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("trigger not found")
	}

	return nil
}

// UpdateTriggerAuth updates a trigger's authentication type and credentials
func (db *db) UpdateTriggerAuth(id int64, authType AuthType, apiKey, username, passwordHash string) error {
	var apiKeyVal, usernameVal, passwordHashVal sql.NullString
	if apiKey != "" {
		apiKeyVal = sql.NullString{String: apiKey, Valid: true}
	}
	if username != "" {
		usernameVal = sql.NullString{String: username, Valid: true}
	}
	if passwordHash != "" {
		passwordHashVal = sql.NullString{String: passwordHash, Valid: true}
	}

	result, err := db.exec(`
		UPDATE triggers SET auth_type = ?, api_key = ?, username = ?, password_hash = ?, updated_at = ? WHERE id = ?
	`, authType, apiKeyVal, usernameVal, passwordHashVal, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update trigger auth: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("trigger not found")
	}

	return nil
}

// DeleteTrigger deletes a trigger by ID
func (db *db) DeleteTrigger(id int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM triggers WHERE id = ?", id); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("trigger not found")
		}
		return fmt.Errorf("failed to delete trigger: %w", err)
	}
	return nil
}

// CountTriggers returns the total number of triggers
func (db *db) CountTriggers() (int, error) {
	var count int
	err := db.queryRow("SELECT COUNT(*) FROM triggers").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count triggers: %w", err)
	}
	return count, nil
}

// CountEnabledTriggers returns the number of enabled triggers
func (db *db) CountEnabledTriggers() (int, error) {
	var count int
	err := db.queryRow("SELECT COUNT(*) FROM triggers WHERE enabled = 1").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count enabled triggers: %w", err)
	}
	return count, nil
}

// ListLocalTriggers retrieves all local triggers (inotify and polling types)
// These are triggers that watch local filesystem paths directly
func (db *db) ListLocalTriggers() ([]*Trigger, error) {
	rows, err := db.query(`
		SELECT id, name, type, auth_type, api_key, username, password_hash, priority, enabled, config, created_at, updated_at
		FROM triggers WHERE type IN (?, ?) ORDER BY name ASC
	`, TriggerTypeInotify, TriggerTypePolling)
	if err != nil {
		return nil, fmt.Errorf("failed to list local triggers: %w", err)
	}
	defer rows.Close()

	var triggers []*Trigger
	for rows.Next() {
		var trigger Trigger
		var configJSON string
		var apiKey, authType, username, passwordHash sql.NullString

		if err := rows.Scan(&trigger.ID, &trigger.Name, &trigger.Type, &authType, &apiKey, &username, &passwordHash, &trigger.Priority, &trigger.Enabled, &configJSON, &trigger.CreatedAt, &trigger.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan trigger: %w", err)
		}

		// Set auth type, defaulting to api_key for backward compatibility
		if authType.Valid && authType.String != "" {
			trigger.AuthType = AuthType(authType.String)
		} else {
			trigger.AuthType = AuthTypeAPIKey
		}

		if apiKey.Valid {
			trigger.APIKey = apiKey.String
		}
		if username.Valid {
			trigger.Username = username.String
		}
		if passwordHash.Valid {
			trigger.Password = passwordHash.String
		}

		if err := unmarshalFromString(configJSON, &trigger.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		triggers = append(triggers, &trigger)
	}

	return triggers, nil
}

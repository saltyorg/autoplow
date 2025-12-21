package database

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
)

// Migrate runs all database migrations
func (db *DB) Migrate() error {
	log.Info().Msg("Running database migrations")

	// Create migrations table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get current version
	var currentVersion int
	err = db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("failed to get current migration version: %w", err)
	}

	log.Debug().Int("current_version", currentVersion).Msg("Current schema version")

	// Run migrations
	for _, migration := range migrations {
		if migration.Version > currentVersion {
			log.Info().Int("version", migration.Version).Str("name", migration.Name).Msg("Applying migration")

			if err := db.Transaction(func(tx *sql.Tx) error {
				// Execute migration SQL - split by semicolons and execute each statement
				// This ensures each statement is properly executed and errors are caught
				statements := splitSQLStatements(migration.SQL)
				for i, stmt := range statements {
					if _, err := tx.Exec(stmt); err != nil {
						return fmt.Errorf("migration %d statement %d failed: %w", migration.Version, i+1, err)
					}
				}

				// Record migration
				if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES (?)", migration.Version); err != nil {
					return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
				}

				return nil
			}); err != nil {
				return err
			}
		}
	}

	log.Info().Msg("Database migrations complete")
	return nil
}

type migration struct {
	Version int
	Name    string
	SQL     string
}

// splitSQLStatements splits a SQL string into individual statements.
// It handles comments and only returns non-empty statements.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.SplitSeq(sql, "\n")
	for line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")

		// Check if line ends with semicolon (statement complete)
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" && stmt != ";" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle any remaining content without trailing semicolon
	if remaining := strings.TrimSpace(current.String()); remaining != "" {
		statements = append(statements, remaining)
	}

	return statements
}

var migrations = []migration{
	{
		Version: 1,
		Name:    "initial_schema",
		SQL: `
			-- User authentication (single user)
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				username TEXT NOT NULL UNIQUE,
				password_hash TEXT NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Sessions for web UI
			CREATE TABLE sessions (
				id TEXT PRIMARY KEY,
				user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
				expires_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Global settings
			CREATE TABLE settings (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Webhook triggers with unique API keys
			CREATE TABLE triggers (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				type TEXT NOT NULL,
				api_key TEXT UNIQUE,
				priority INTEGER DEFAULT 0,
				enabled BOOLEAN DEFAULT true,
				config TEXT NOT NULL DEFAULT '{}',
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Media server targets
			CREATE TABLE targets (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				type TEXT NOT NULL,
				url TEXT NOT NULL,
				token TEXT,
				api_key TEXT,
				enabled BOOLEAN DEFAULT true,
				config TEXT NOT NULL DEFAULT '{}',
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Upload remotes (rclone destinations)
			CREATE TABLE remotes (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				rclone_remote TEXT NOT NULL,
				priority INTEGER DEFAULT 1,
				enabled BOOLEAN DEFAULT true,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Upload path mappings
			CREATE TABLE upload_paths (
				id INTEGER PRIMARY KEY,
				local_path TEXT NOT NULL,
				upload_mode TEXT NOT NULL DEFAULT 'immediate',
				mode_value INTEGER,
				enabled BOOLEAN DEFAULT true,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Many-to-many: upload paths to remotes
			CREATE TABLE upload_path_remotes (
				upload_path_id INTEGER REFERENCES upload_paths(id) ON DELETE CASCADE,
				remote_id INTEGER REFERENCES remotes(id) ON DELETE CASCADE,
				priority INTEGER DEFAULT 1,
				remote_path TEXT NOT NULL,
				PRIMARY KEY (upload_path_id, remote_id)
			);

			-- Pending and completed scans
			CREATE TABLE scans (
				id INTEGER PRIMARY KEY,
				path TEXT NOT NULL,
				trigger_id INTEGER REFERENCES triggers(id),
				priority INTEGER DEFAULT 0,
				status TEXT DEFAULT 'pending',
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				started_at TIMESTAMP,
				completed_at TIMESTAMP,
				retry_count INTEGER DEFAULT 0,
				last_error TEXT,
				target_id INTEGER REFERENCES targets(id)
			);

			-- Upload queue
			CREATE TABLE uploads (
				id INTEGER PRIMARY KEY,
				scan_id INTEGER REFERENCES scans(id),
				local_path TEXT NOT NULL,
				remote_name TEXT NOT NULL,
				remote_path TEXT NOT NULL,
				status TEXT DEFAULT 'queued',
				size_bytes INTEGER,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				started_at TIMESTAMP,
				completed_at TIMESTAMP,
				rclone_job_id INTEGER,
				progress_bytes INTEGER DEFAULT 0,
				retry_count INTEGER DEFAULT 0,
				last_error TEXT,
				remote_priority INTEGER DEFAULT 1
			);

			-- Upload history for analytics
			CREATE TABLE upload_history (
				id INTEGER PRIMARY KEY,
				upload_id INTEGER,
				local_path TEXT,
				remote_name TEXT,
				remote_path TEXT,
				size_bytes INTEGER,
				duration_seconds INTEGER,
				avg_speed_bytes INTEGER,
				completed_at TIMESTAMP
			);

			-- Active media sessions (for throttling)
			CREATE TABLE active_sessions (
				id TEXT PRIMARY KEY,
				server_type TEXT,
				server_id TEXT,
				username TEXT,
				media_title TEXT,
				media_type TEXT,
				resolution TEXT,
				bitrate INTEGER,
				updated_at TIMESTAMP
			);

			-- Notification log
			CREATE TABLE notification_log (
				id INTEGER PRIMARY KEY,
				event_type TEXT NOT NULL,
				provider TEXT NOT NULL,
				status TEXT DEFAULT 'sent',
				message TEXT,
				error TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Indexes for common queries
			CREATE INDEX idx_sessions_expires ON sessions(expires_at);
			CREATE INDEX idx_sessions_user ON sessions(user_id);
			CREATE INDEX idx_triggers_type ON triggers(type);
			CREATE INDEX idx_triggers_api_key ON triggers(api_key);
			CREATE INDEX idx_scans_status ON scans(status);
			CREATE INDEX idx_scans_created ON scans(created_at);
			CREATE INDEX idx_uploads_status ON uploads(status);
			CREATE INDEX idx_uploads_created ON uploads(created_at);
		`,
	},
	{
		Version: 2,
		Name:    "notification_providers",
		SQL: `
			-- Notification providers configuration
			CREATE TABLE notification_providers (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				type TEXT NOT NULL,
				enabled BOOLEAN DEFAULT true,
				config TEXT NOT NULL DEFAULT '{}',
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);

			-- Add title column to notification_log
			ALTER TABLE notification_log ADD COLUMN title TEXT;

			-- Notification subscriptions (which events trigger which providers)
			CREATE TABLE notification_subscriptions (
				id INTEGER PRIMARY KEY,
				provider_id INTEGER REFERENCES notification_providers(id) ON DELETE CASCADE,
				event_type TEXT NOT NULL,
				enabled BOOLEAN DEFAULT true,
				UNIQUE(provider_id, event_type)
			);

			CREATE INDEX idx_notification_log_provider ON notification_log(provider);
			CREATE INDEX idx_notification_log_event ON notification_log(event_type);
			CREATE INDEX idx_notification_log_created ON notification_log(created_at);
		`,
	},
	{
		Version: 3,
		Name:    "trigger_user_password_auth",
		SQL: `
			-- Add auth_type to specify whether trigger uses API key or username/password
			-- Default to 'api_key' for backward compatibility
			ALTER TABLE triggers ADD COLUMN auth_type TEXT NOT NULL DEFAULT 'api_key';

			-- Add username and password_hash columns for user/password authentication
			ALTER TABLE triggers ADD COLUMN username TEXT;
			ALTER TABLE triggers ADD COLUMN password_hash TEXT;
		`,
	},
	{
		Version: 4,
		Name:    "scan_retry_scheduling",
		SQL: `
			-- Add next_retry_at column to scans for exponential backoff scheduling
			-- NULL means the scan can be processed immediately (for pending/new scans)
			-- Non-NULL means the scan is in retry state and should wait until that time
			ALTER TABLE scans ADD COLUMN next_retry_at TIMESTAMP;

			-- Index for efficient retry queries
			CREATE INDEX idx_scans_next_retry ON scans(next_retry_at);
		`,
	},
	{
		Version: 5,
		Name:    "remote_transfer_options",
		SQL: `
			-- Add transfer_options column to remotes for backend-specific settings
			-- Stored as JSON, e.g., {"transfers": 8, "checkers": 16, "bwlimit": "10M"}
			ALTER TABLE remotes ADD COLUMN transfer_options TEXT;
		`,
	},
	{
		Version: 6,
		Name:    "remove_remote_priority",
		SQL: `
			-- Remove unused priority column from remotes table
			-- Priority is only meaningful at the upload_path_remotes level
			-- SQLite doesn't support DROP COLUMN directly, so we recreate the table
			CREATE TABLE remotes_new (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				rclone_remote TEXT NOT NULL,
				enabled BOOLEAN DEFAULT true,
				transfer_options TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			INSERT INTO remotes_new (id, name, rclone_remote, enabled, transfer_options, created_at)
				SELECT id, name, rclone_remote, enabled, transfer_options, created_at FROM remotes;
			DROP TABLE remotes;
			ALTER TABLE remotes_new RENAME TO remotes;
		`,
	},
	{
		Version: 7,
		Name:    "target_libraries_cache",
		SQL: `
			-- Cache of target library paths to avoid repeated API calls
			-- Used for determining if a file path falls within a local target's library
			CREATE TABLE target_libraries (
				id INTEGER PRIMARY KEY,
				target_id INTEGER NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
				library_id TEXT NOT NULL,
				name TEXT NOT NULL,
				type TEXT,
				path TEXT NOT NULL,
				fetched_at TIMESTAMP NOT NULL,
				UNIQUE(target_id, library_id, path)
			);

			CREATE INDEX idx_target_libraries_target ON target_libraries(target_id);
			CREATE INDEX idx_target_libraries_fetched ON target_libraries(fetched_at);
		`,
	},
	{
		Version: 8,
		Name:    "upload_path_polling",
		SQL: `
			-- Add polling configuration to upload_paths
			-- Allows automatic discovery of files for upload without triggers
			ALTER TABLE upload_paths ADD COLUMN polling_enabled BOOLEAN DEFAULT true;
			ALTER TABLE upload_paths ADD COLUMN polling_buffer_seconds INTEGER DEFAULT 60;
			ALTER TABLE upload_paths ADD COLUMN polling_priority INTEGER DEFAULT 100;
			ALTER TABLE upload_paths ADD COLUMN exclude_paths TEXT;
			ALTER TABLE upload_paths ADD COLUMN exclude_extensions TEXT;
		`,
	},
	{
		Version: 9,
		Name:    "simplify_upload_history",
		SQL: `
			-- Remove duration and speed columns from upload_history
			-- These were inaccurate (based on batch timing, not per-file)
			-- SQLite doesn't support DROP COLUMN directly, so we recreate the table
			CREATE TABLE upload_history_new (
				id INTEGER PRIMARY KEY,
				upload_id INTEGER,
				local_path TEXT,
				remote_name TEXT,
				remote_path TEXT,
				size_bytes INTEGER,
				completed_at TIMESTAMP
			);
			INSERT INTO upload_history_new (id, upload_id, local_path, remote_name, remote_path, size_bytes, completed_at)
			SELECT id, upload_id, local_path, remote_name, remote_path, size_bytes, completed_at FROM upload_history;
			DROP TABLE upload_history;
			ALTER TABLE upload_history_new RENAME TO upload_history;
		`,
	},
	{
		Version: 10,
		Name:    "remove_upload_path_polling",
		SQL: `
			-- Remove polling columns from upload_paths
			-- Polling has been replaced by inotify triggers for file discovery
			-- Keep exclude_paths and exclude_extensions as they're still useful
			-- SQLite doesn't support DROP COLUMN directly, so we recreate the table
			CREATE TABLE upload_paths_new (
				id INTEGER PRIMARY KEY,
				local_path TEXT NOT NULL,
				upload_mode TEXT NOT NULL DEFAULT 'immediate',
				mode_value INTEGER,
				enabled BOOLEAN DEFAULT true,
				exclude_paths TEXT,
				exclude_extensions TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			INSERT INTO upload_paths_new (id, local_path, upload_mode, mode_value, enabled, exclude_paths, exclude_extensions, created_at)
			SELECT id, local_path, upload_mode, mode_value, enabled, exclude_paths, exclude_extensions, created_at FROM upload_paths;
			DROP TABLE upload_paths;
			ALTER TABLE upload_paths_new RENAME TO upload_paths;
		`,
	},
	{
		Version: 11,
		Name:    "upload_path_transfer_type",
		SQL: `
			-- Add transfer_type column to upload_paths
			-- Determines whether files are copied or moved (deleted after upload)
			-- Default to 'move' as the typical use case
			ALTER TABLE upload_paths ADD COLUMN transfer_type TEXT NOT NULL DEFAULT 'move';
		`,
	},
	{
		Version: 12,
		Name:    "rename_upload_paths_to_destinations",
		SQL: `
			-- Rename upload_paths to destinations and add included_triggers
			CREATE TABLE destinations (
				id INTEGER PRIMARY KEY,
				local_path TEXT NOT NULL,
				upload_mode TEXT NOT NULL DEFAULT 'immediate',
				mode_value INTEGER,
				transfer_type TEXT NOT NULL DEFAULT 'move',
				enabled BOOLEAN DEFAULT true,
				exclude_paths TEXT,
				exclude_extensions TEXT,
				included_triggers TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			INSERT INTO destinations SELECT id, local_path, upload_mode, mode_value, transfer_type, enabled, exclude_paths, exclude_extensions, NULL, created_at FROM upload_paths;
			DROP TABLE upload_paths;

			-- Rename upload_path_remotes to destination_remotes
			CREATE TABLE destination_remotes (
				destination_id INTEGER REFERENCES destinations(id) ON DELETE CASCADE,
				remote_id INTEGER REFERENCES remotes(id) ON DELETE CASCADE,
				priority INTEGER DEFAULT 1,
				remote_path TEXT NOT NULL,
				PRIMARY KEY (destination_id, remote_id)
			);
			INSERT INTO destination_remotes SELECT upload_path_id, remote_id, priority, remote_path FROM upload_path_remotes;
			DROP TABLE upload_path_remotes;
		`,
	},
	{
		Version: 13,
		Name:    "add_advanced_filters_to_destinations",
		SQL: `
			-- Add advanced_filters column to destinations for regex-based filtering
			ALTER TABLE destinations ADD COLUMN advanced_filters TEXT;
		`,
	},
	{
		Version: 14,
		Name:    "add_event_type_to_scans",
		SQL: `
			-- Add event_type column to scans to track the webhook event that created the scan
			-- Used to skip path existence checks for delete events (MovieFileDelete, EpisodeFileDelete, etc.)
			ALTER TABLE scans ADD COLUMN event_type TEXT;
		`,
	},
}

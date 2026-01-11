package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/saltyorg/autoplow/internal/logging"
)

// GetSetting retrieves a setting value by key
func (db *db) GetSetting(key string) (string, error) {
	var value string
	err := db.queryRow("SELECT value FROM settings WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to get setting %s: %w", key, err)
	}
	return value, nil
}

// GetSettingJSON retrieves a setting and unmarshal it from JSON
func (db *db) GetSettingJSON(key string, v any) error {
	value, err := db.GetSetting(key)
	if err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	return json.Unmarshal([]byte(value), v)
}

// SetSetting stores a setting value
func (db *db) SetSetting(key, value string) error {
	_, err := db.exec(`
		INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
	`, key, value, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set setting %s: %w", key, err)
	}
	return nil
}

// SetSettingJSON stores a setting as JSON
func (db *db) SetSettingJSON(key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal setting %s: %w", key, err)
	}
	return db.SetSetting(key, string(data))
}

// GetAllSettings retrieves all settings
func (db *db) GetAllSettings() (map[string]string, error) {
	rows, err := db.query("SELECT key, value FROM settings")
	if err != nil {
		return nil, fmt.Errorf("failed to get settings: %w", err)
	}
	defer rows.Close()

	settings := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("failed to scan setting: %w", err)
		}
		settings[key] = value
	}

	return settings, rows.Err()
}

// GetSettingsBatch retrieves multiple settings at once and returns a map
// Keys not found in the database will not be included in the result
func (db *db) GetSettingsBatch(keys ...string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if val, err := db.GetSetting(key); err != nil {
			return nil, err
		} else if val != "" {
			result[key] = val
		}
	}
	return result, nil
}

// DeleteSetting removes a setting
func (db *db) DeleteSetting(key string) error {
	_, err := db.exec("DELETE FROM settings WHERE key = ?", key)
	if err != nil {
		return fmt.Errorf("failed to delete setting %s: %w", key, err)
	}
	return nil
}

// Default settings
var DefaultSettings = map[string]any{
	"log.level":                              "info",
	"log.max_size_mb":                        logging.DefaultMaxSizeMB,
	"log.max_backups":                        logging.DefaultMaxBackups,
	"log.max_age_days":                       logging.DefaultMaxAgeDays,
	"log.compress":                           logging.DefaultCompress,
	"processor.minimum_age_seconds":          600,
	"processor.anchor.enabled":               true,
	"processor.batch_interval_seconds":       60,
	"processor.max_retries":                  3,
	"processor.cleanup_days":                 7, // 0 = disabled, keeps scan history forever
	"processor.path_not_found_retries":       0, // 0 = fail immediately (default)
	"processor.path_not_found_delay_seconds": 5,
	"scanning.enabled":                       true,  // Enable media server scanning
	"uploads.enabled":                        true,  // Enable cloud uploads
	"display.use_binary_units":               true,  // true = MiB/s (1024), false = MB/s (1000)
	"display.use_bits_for_bitrate":           true,  // true = Mbps (bits), false = MiB/s (bytes) for streaming
	"display.date_format":                    "ymd", // ymd, dmy, mdy
	"display.time_format":                    "24h", // 24h, 12h
	"throttle.enabled":                       false,
	"throttle.pause_below_speed":             "1M",
	"throttle.idle_speed":                    "0",
	"throttle.rules":                         []map[string]string{},
	"schedule.enabled":                       false,
	"rclone.managed":                         true, // true = managed (Autoplow runs rclone), false = unmanaged (external rclone)
	"rclone.binary_path":                     "/usr/bin/rclone",
	"rclone.config_path":                     "",
	"rclone.rcd_address":                     "127.0.0.1:5572",
	"rclone.auto_start":                      true,
	"rclone.restart_on_fail":                 true,
	"rclone.max_restarts":                    10,
	"rclone.transfers":                       4,     // concurrent file transfers
	"rclone.checkers":                        8,     // concurrent file checkers
	"rclone.buffer_size":                     "16M", // buffer size per transfer (SizeSuffix)
	"rclone.log.level":                       "NOTICE",
	"rclone.log.file_max_age":                "0s",
	"rclone.log.file_max_backups":            0,
	"rclone.log.file_max_size":               "0",
	"rclone.log.file_compress":               false,
	"notifications.discord.enabled":          false,
	"notifications.discord.webhook_url":      "",
	"notifications.discord.events":           []string{"upload_complete", "upload_failed"},
}

// InitializeDefaults sets default values for settings that don't exist
func (db *db) InitializeDefaults() error {
	for key, value := range DefaultSettings {
		existing, err := db.GetSetting(key)
		if err != nil {
			return err
		}
		if existing == "" {
			if err := db.SetSettingJSON(key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

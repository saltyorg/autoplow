package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// PlexAutoLanguagesEventType represents the type of event that triggered a language change
type PlexAutoLanguagesEventType string

const (
	PlexAutoLanguagesEventTypePlay       PlexAutoLanguagesEventType = "play"
	PlexAutoLanguagesEventTypeNewEpisode PlexAutoLanguagesEventType = "new_episode"
	PlexAutoLanguagesEventTypeActivity   PlexAutoLanguagesEventType = "activity"
	PlexAutoLanguagesEventTypeScheduler  PlexAutoLanguagesEventType = "scheduler"
)

// PlexAutoLanguagesUpdateLevel controls which episodes get updated
type PlexAutoLanguagesUpdateLevel string

const (
	PlexAutoLanguagesUpdateLevelShow   PlexAutoLanguagesUpdateLevel = "show"
	PlexAutoLanguagesUpdateLevelSeason PlexAutoLanguagesUpdateLevel = "season"
)

// PlexAutoLanguagesUpdateStrategy controls which episodes get updated
type PlexAutoLanguagesUpdateStrategy string

const (
	PlexAutoLanguagesUpdateStrategyAll  PlexAutoLanguagesUpdateStrategy = "all"
	PlexAutoLanguagesUpdateStrategyNext PlexAutoLanguagesUpdateStrategy = "next"
)

// PlexAutoLanguagesPreference stores a user's track preference for a show
type PlexAutoLanguagesPreference struct {
	ID                      int64     `json:"id"`
	TargetID                int64     `json:"target_id"`
	TargetName              string    `json:"target_name"`
	PlexUserID              string    `json:"plex_user_id"`
	PlexUsername            string    `json:"plex_username"`
	ShowRatingKey           string    `json:"show_rating_key"`
	ShowTitle               string    `json:"show_title"`
	AudioLanguageCode       string    `json:"audio_language_code"`
	AudioCodec              string    `json:"audio_codec"`
	AudioChannels           int       `json:"audio_channels"`
	AudioChannelLayout      string    `json:"audio_channel_layout"`
	AudioTitle              string    `json:"audio_title"`
	AudioDisplayTitle       string    `json:"audio_display_title"`
	AudioVisualImpaired     bool      `json:"audio_visual_impaired"`
	SubtitleLanguageCode    *string   `json:"subtitle_language_code"` // nil = no subtitles
	SubtitleForced          bool      `json:"subtitle_forced"`
	SubtitleHearingImpaired bool      `json:"subtitle_hearing_impaired"`
	SubtitleCodec           *string   `json:"subtitle_codec"`
	SubtitleTitle           *string   `json:"subtitle_title"`
	SubtitleDisplayTitle    *string   `json:"subtitle_display_title"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`
}

// PlexAutoLanguagesHistory represents a track change history entry
type PlexAutoLanguagesHistory struct {
	ID               int64                      `json:"id"`
	TargetID         int64                      `json:"target_id"`
	TargetName       string                     `json:"target_name"`
	PlexUserID       string                     `json:"plex_user_id"`
	PlexUsername     string                     `json:"plex_username"`
	ShowTitle        string                     `json:"show_title"`
	ShowRatingKey    string                     `json:"show_rating_key"`
	EpisodeTitle     string                     `json:"episode_title"`
	EpisodeRatingKey string                     `json:"episode_rating_key"`
	EventType        PlexAutoLanguagesEventType `json:"event_type"`
	AudioChanged     bool                       `json:"audio_changed"`
	AudioFrom        string                     `json:"audio_from"`
	AudioTo          string                     `json:"audio_to"`
	SubtitleChanged  bool                       `json:"subtitle_changed"`
	SubtitleFrom     string                     `json:"subtitle_from"`
	SubtitleTo       string                     `json:"subtitle_to"`
	EpisodesUpdated  int                        `json:"episodes_updated"`
	CreatedAt        time.Time                  `json:"created_at"`
}

// PlexAutoLanguagesConfig holds configuration for a target
type PlexAutoLanguagesConfig struct {
	TargetID          int64                           `json:"target_id"`
	Enabled           bool                            `json:"enabled"`
	UpdateLevel       PlexAutoLanguagesUpdateLevel    `json:"update_level"`
	UpdateStrategy    PlexAutoLanguagesUpdateStrategy `json:"update_strategy"`
	TriggerOnPlay     bool                            `json:"trigger_on_play"`
	TriggerOnScan     bool                            `json:"trigger_on_scan"`
	TriggerOnActivity bool                            `json:"trigger_on_activity"`
	IgnoreLabels      []string                        `json:"ignore_labels"`
	IgnoreLibraries   []string                        `json:"ignore_libraries"`
	Schedule          string                          `json:"schedule"`
	CreatedAt         time.Time                       `json:"created_at"`
	UpdatedAt         time.Time                       `json:"updated_at"`
}

// DefaultPlexAutoLanguagesConfig returns the default configuration
func DefaultPlexAutoLanguagesConfig(targetID int64) PlexAutoLanguagesConfig {
	return PlexAutoLanguagesConfig{
		TargetID:          targetID,
		Enabled:           false,
		UpdateLevel:       PlexAutoLanguagesUpdateLevelShow,
		UpdateStrategy:    PlexAutoLanguagesUpdateStrategyNext,
		TriggerOnPlay:     true,
		TriggerOnScan:     true,
		TriggerOnActivity: true,
		IgnoreLabels:      []string{},
		IgnoreLibraries:   []string{},
		Schedule:          "",
	}
}

// GetPlexAutoLanguagesConfig retrieves the configuration for a target
func (db *DB) GetPlexAutoLanguagesConfig(targetID int64) (*PlexAutoLanguagesConfig, error) {
	var config PlexAutoLanguagesConfig
	var ignoreLabelsJSON, ignoreLibrariesJSON string

	err := db.QueryRow(`
		SELECT target_id, enabled, update_level, update_strategy, trigger_on_play, trigger_on_scan,
			trigger_on_activity, ignore_labels, ignore_libraries, schedule, created_at, updated_at
		FROM plex_auto_languages_config WHERE target_id = ?
	`, targetID).Scan(
		&config.TargetID, &config.Enabled, &config.UpdateLevel, &config.UpdateStrategy,
		&config.TriggerOnPlay, &config.TriggerOnScan, &config.TriggerOnActivity,
		&ignoreLabelsJSON, &ignoreLibrariesJSON, &config.Schedule,
		&config.CreatedAt, &config.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		defaultConfig := DefaultPlexAutoLanguagesConfig(targetID)
		return &defaultConfig, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get plex auto languages config: %w", err)
	}

	if err := unmarshalFromString(ignoreLabelsJSON, &config.IgnoreLabels); err != nil {
		config.IgnoreLabels = []string{}
	}
	if err := unmarshalFromString(ignoreLibrariesJSON, &config.IgnoreLibraries); err != nil {
		config.IgnoreLibraries = []string{}
	}

	return &config, nil
}

// UpsertPlexAutoLanguagesConfig creates or updates the configuration for a target
func (db *DB) UpsertPlexAutoLanguagesConfig(config *PlexAutoLanguagesConfig) error {
	ignoreLabelsJSON, err := marshalToString(config.IgnoreLabels)
	if err != nil {
		return fmt.Errorf("failed to marshal ignore labels: %w", err)
	}
	ignoreLibrariesJSON, err := marshalToString(config.IgnoreLibraries)
	if err != nil {
		return fmt.Errorf("failed to marshal ignore libraries: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO plex_auto_languages_config (
			target_id, enabled, update_level, update_strategy, trigger_on_play, trigger_on_scan,
			trigger_on_activity, ignore_labels, ignore_libraries, schedule, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(target_id) DO UPDATE SET
			enabled = excluded.enabled,
			update_level = excluded.update_level,
			update_strategy = excluded.update_strategy,
			trigger_on_play = excluded.trigger_on_play,
			trigger_on_scan = excluded.trigger_on_scan,
			trigger_on_activity = excluded.trigger_on_activity,
			ignore_labels = excluded.ignore_labels,
			ignore_libraries = excluded.ignore_libraries,
			schedule = excluded.schedule,
			updated_at = CURRENT_TIMESTAMP
	`, config.TargetID, config.Enabled, config.UpdateLevel, config.UpdateStrategy,
		config.TriggerOnPlay, config.TriggerOnScan, config.TriggerOnActivity,
		ignoreLabelsJSON, ignoreLibrariesJSON, config.Schedule)
	if err != nil {
		return fmt.Errorf("failed to upsert plex auto languages config: %w", err)
	}

	return nil
}

// UpsertPlexAutoLanguagesPreference creates or updates a user's track preference for a show
func (db *DB) UpsertPlexAutoLanguagesPreference(pref *PlexAutoLanguagesPreference) error {
	_, err := db.Exec(`
		INSERT INTO plex_auto_languages_preferences (
			target_id, plex_user_id, plex_username, show_rating_key, show_title,
			audio_language_code, audio_codec, audio_channels, audio_channel_layout,
			audio_title, audio_display_title, audio_visual_impaired,
			subtitle_language_code, subtitle_forced, subtitle_hearing_impaired,
			subtitle_codec, subtitle_title, subtitle_display_title,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(target_id, plex_user_id, show_rating_key) DO UPDATE SET
			plex_username = excluded.plex_username,
			show_title = excluded.show_title,
			audio_language_code = excluded.audio_language_code,
			audio_codec = excluded.audio_codec,
			audio_channels = excluded.audio_channels,
			audio_channel_layout = excluded.audio_channel_layout,
			audio_title = excluded.audio_title,
			audio_display_title = excluded.audio_display_title,
			audio_visual_impaired = excluded.audio_visual_impaired,
			subtitle_language_code = excluded.subtitle_language_code,
			subtitle_forced = excluded.subtitle_forced,
			subtitle_hearing_impaired = excluded.subtitle_hearing_impaired,
			subtitle_codec = excluded.subtitle_codec,
			subtitle_title = excluded.subtitle_title,
			subtitle_display_title = excluded.subtitle_display_title,
			updated_at = CURRENT_TIMESTAMP
	`, pref.TargetID, pref.PlexUserID, pref.PlexUsername, pref.ShowRatingKey, pref.ShowTitle,
		pref.AudioLanguageCode, pref.AudioCodec, pref.AudioChannels, pref.AudioChannelLayout,
		pref.AudioTitle, pref.AudioDisplayTitle, pref.AudioVisualImpaired,
		pref.SubtitleLanguageCode, pref.SubtitleForced, pref.SubtitleHearingImpaired,
		pref.SubtitleCodec, pref.SubtitleTitle, pref.SubtitleDisplayTitle)
	if err != nil {
		return fmt.Errorf("failed to upsert plex auto languages preference: %w", err)
	}

	return nil
}

// GetPlexAutoLanguagesPreference retrieves a user's track preference for a show
func (db *DB) GetPlexAutoLanguagesPreference(targetID int64, plexUserID, showRatingKey string) (*PlexAutoLanguagesPreference, error) {
	var pref PlexAutoLanguagesPreference
	var plexUsername sql.NullString

	err := db.QueryRow(`
		SELECT id, target_id, plex_user_id, plex_username, show_rating_key, show_title,
			audio_language_code, audio_codec, audio_channels, audio_channel_layout,
			audio_title, audio_display_title, audio_visual_impaired,
			subtitle_language_code, subtitle_forced, subtitle_hearing_impaired,
			subtitle_codec, subtitle_title, subtitle_display_title,
			created_at, updated_at
		FROM plex_auto_languages_preferences
		WHERE target_id = ? AND plex_user_id = ? AND show_rating_key = ?
	`, targetID, plexUserID, showRatingKey).Scan(
		&pref.ID, &pref.TargetID, &pref.PlexUserID, &plexUsername, &pref.ShowRatingKey, &pref.ShowTitle,
		&pref.AudioLanguageCode, &pref.AudioCodec, &pref.AudioChannels, &pref.AudioChannelLayout,
		&pref.AudioTitle, &pref.AudioDisplayTitle, &pref.AudioVisualImpaired,
		&pref.SubtitleLanguageCode, &pref.SubtitleForced, &pref.SubtitleHearingImpaired,
		&pref.SubtitleCodec, &pref.SubtitleTitle, &pref.SubtitleDisplayTitle,
		&pref.CreatedAt, &pref.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get plex auto languages preference: %w", err)
	}
	pref.PlexUsername = plexUsername.String

	return &pref, nil
}

// ListPlexAutoLanguagesPreferences retrieves all preferences for a target
func (db *DB) ListPlexAutoLanguagesPreferences(targetID int64) ([]*PlexAutoLanguagesPreference, error) {
	rows, err := db.Query(`
		SELECT p.id, p.target_id, t.name as target_name, p.plex_user_id, p.plex_username, p.show_rating_key, p.show_title,
			p.audio_language_code, p.audio_codec, p.audio_channels, p.audio_channel_layout,
			p.audio_title, p.audio_display_title, p.audio_visual_impaired,
			p.subtitle_language_code, p.subtitle_forced, p.subtitle_hearing_impaired,
			p.subtitle_codec, p.subtitle_title, p.subtitle_display_title,
			p.created_at, p.updated_at
		FROM plex_auto_languages_preferences p
		JOIN targets t ON p.target_id = t.id
		WHERE p.target_id = ?
		ORDER BY p.show_title, p.plex_user_id
	`, targetID)
	if err != nil {
		return nil, fmt.Errorf("failed to list plex auto languages preferences: %w", err)
	}
	defer rows.Close()

	return scanPlexAutoLanguagesPreferences(rows)
}

// ListPlexAutoLanguagesPreferencesForShow retrieves all user preferences for a specific show
func (db *DB) ListPlexAutoLanguagesPreferencesForShow(targetID int64, showRatingKey string) ([]*PlexAutoLanguagesPreference, error) {
	rows, err := db.Query(`
		SELECT p.id, p.target_id, t.name as target_name, p.plex_user_id, p.plex_username, p.show_rating_key, p.show_title,
			p.audio_language_code, p.audio_codec, p.audio_channels, p.audio_channel_layout,
			p.audio_title, p.audio_display_title, p.audio_visual_impaired,
			p.subtitle_language_code, p.subtitle_forced, p.subtitle_hearing_impaired,
			p.subtitle_codec, p.subtitle_title, p.subtitle_display_title,
			p.created_at, p.updated_at
		FROM plex_auto_languages_preferences p
		JOIN targets t ON p.target_id = t.id
		WHERE p.target_id = ? AND p.show_rating_key = ?
		ORDER BY p.plex_user_id
	`, targetID, showRatingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to list plex auto languages preferences for show: %w", err)
	}
	defer rows.Close()

	return scanPlexAutoLanguagesPreferences(rows)
}

func scanPlexAutoLanguagesPreferences(rows *sql.Rows) ([]*PlexAutoLanguagesPreference, error) {
	var prefs []*PlexAutoLanguagesPreference
	for rows.Next() {
		var pref PlexAutoLanguagesPreference
		var plexUsername sql.NullString
		var targetName sql.NullString
		if err := rows.Scan(
			&pref.ID, &pref.TargetID, &targetName, &pref.PlexUserID, &plexUsername, &pref.ShowRatingKey, &pref.ShowTitle,
			&pref.AudioLanguageCode, &pref.AudioCodec, &pref.AudioChannels, &pref.AudioChannelLayout,
			&pref.AudioTitle, &pref.AudioDisplayTitle, &pref.AudioVisualImpaired,
			&pref.SubtitleLanguageCode, &pref.SubtitleForced, &pref.SubtitleHearingImpaired,
			&pref.SubtitleCodec, &pref.SubtitleTitle, &pref.SubtitleDisplayTitle,
			&pref.CreatedAt, &pref.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan plex auto languages preference: %w", err)
		}
		pref.PlexUsername = plexUsername.String
		if targetName.Valid {
			pref.TargetName = targetName.String
		}
		prefs = append(prefs, &pref)
	}
	return prefs, rows.Err()
}

// DeletePlexAutoLanguagesPreference deletes a preference by ID
func (db *DB) DeletePlexAutoLanguagesPreference(id int64) error {
	_, err := db.Exec(`DELETE FROM plex_auto_languages_preferences WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("failed to delete plex auto languages preference: %w", err)
	}
	return nil
}

// CreatePlexAutoLanguagesHistory creates a new history entry
func (db *DB) CreatePlexAutoLanguagesHistory(h *PlexAutoLanguagesHistory) error {
	result, err := db.Exec(`
		INSERT INTO plex_auto_languages_history (
			target_id, plex_user_id, plex_username, show_title, show_rating_key,
			episode_title, episode_rating_key, event_type,
			audio_changed, audio_from, audio_to,
			subtitle_changed, subtitle_from, subtitle_to,
			episodes_updated, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, h.TargetID, h.PlexUserID, h.PlexUsername, h.ShowTitle, h.ShowRatingKey,
		h.EpisodeTitle, h.EpisodeRatingKey, h.EventType,
		h.AudioChanged, h.AudioFrom, h.AudioTo,
		h.SubtitleChanged, h.SubtitleFrom, h.SubtitleTo,
		h.EpisodesUpdated)
	if err != nil {
		return fmt.Errorf("failed to create plex auto languages history: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	h.ID = id

	return nil
}

// ListPlexAutoLanguagesHistory retrieves history entries with pagination
func (db *DB) ListPlexAutoLanguagesHistory(targetID int64, limit, offset int) ([]*PlexAutoLanguagesHistory, error) {
	rows, err := db.Query(`
		SELECT h.id, h.target_id, t.name as target_name, h.plex_user_id, h.plex_username, h.show_title, h.show_rating_key,
			h.episode_title, h.episode_rating_key, h.event_type,
			h.audio_changed, h.audio_from, h.audio_to,
			h.subtitle_changed, h.subtitle_from, h.subtitle_to,
			h.episodes_updated, h.created_at
		FROM plex_auto_languages_history h
		JOIN targets t ON h.target_id = t.id
		WHERE h.target_id = ?
		ORDER BY h.created_at DESC
		LIMIT ? OFFSET ?
	`, targetID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list plex auto languages history: %w", err)
	}
	defer rows.Close()

	return scanPlexAutoLanguagesHistory(rows)
}

// ListAllPlexAutoLanguagesHistory retrieves history entries across all targets
func (db *DB) ListAllPlexAutoLanguagesHistory(limit, offset int) ([]*PlexAutoLanguagesHistory, error) {
	rows, err := db.Query(`
		SELECT h.id, h.target_id, t.name as target_name, h.plex_user_id, h.plex_username, h.show_title, h.show_rating_key,
			h.episode_title, h.episode_rating_key, h.event_type,
			h.audio_changed, h.audio_from, h.audio_to,
			h.subtitle_changed, h.subtitle_from, h.subtitle_to,
			h.episodes_updated, h.created_at
		FROM plex_auto_languages_history h
		JOIN targets t ON h.target_id = t.id
		ORDER BY h.created_at DESC
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list all plex auto languages history: %w", err)
	}
	defer rows.Close()

	return scanPlexAutoLanguagesHistory(rows)
}

func scanPlexAutoLanguagesHistory(rows *sql.Rows) ([]*PlexAutoLanguagesHistory, error) {
	var entries []*PlexAutoLanguagesHistory
	for rows.Next() {
		var h PlexAutoLanguagesHistory
		var plexUserID, plexUsername, audioFrom, audioTo, subtitleFrom, subtitleTo sql.NullString
		var targetName sql.NullString
		if err := rows.Scan(
			&h.ID, &h.TargetID, &targetName, &plexUserID, &plexUsername, &h.ShowTitle, &h.ShowRatingKey,
			&h.EpisodeTitle, &h.EpisodeRatingKey, &h.EventType,
			&h.AudioChanged, &audioFrom, &audioTo,
			&h.SubtitleChanged, &subtitleFrom, &subtitleTo,
			&h.EpisodesUpdated, &h.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan plex auto languages history: %w", err)
		}
		h.PlexUserID = plexUserID.String
		h.PlexUsername = plexUsername.String
		if targetName.Valid {
			h.TargetName = targetName.String
		}
		h.AudioFrom = audioFrom.String
		h.AudioTo = audioTo.String
		h.SubtitleFrom = subtitleFrom.String
		h.SubtitleTo = subtitleTo.String
		entries = append(entries, &h)
	}
	return entries, rows.Err()
}

// ClearPlexAutoLanguagesHistory clears all history for a target
func (db *DB) ClearPlexAutoLanguagesHistory(targetID int64) error {
	_, err := db.Exec(`DELETE FROM plex_auto_languages_history WHERE target_id = ?`, targetID)
	if err != nil {
		return fmt.Errorf("failed to clear plex auto languages history: %w", err)
	}
	return nil
}

// ClearAllPlexAutoLanguagesHistory clears history across all targets
func (db *DB) ClearAllPlexAutoLanguagesHistory() error {
	_, err := db.Exec(`DELETE FROM plex_auto_languages_history`)
	if err != nil {
		return fmt.Errorf("failed to clear all plex auto languages history: %w", err)
	}
	return nil
}

// DeleteOldPlexAutoLanguagesHistory deletes history entries older than the specified number of days
func (db *DB) DeleteOldPlexAutoLanguagesHistory(daysToKeep int) (int64, error) {
	result, err := db.Exec(`
		DELETE FROM plex_auto_languages_history
		WHERE created_at < datetime('now', '-' || ? || ' days')
	`, daysToKeep)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old plex auto languages history: %w", err)
	}
	return result.RowsAffected()
}

// ListPlexAutoLanguagesPreferencesFiltered retrieves preferences with pagination and optional user/target filter
// targetID == 0 means all targets
func (db *DB) ListPlexAutoLanguagesPreferencesFiltered(targetID int64, userFilter string, limit, offset int) ([]*PlexAutoLanguagesPreference, int, error) {
	where := []string{}
	args := []any{}
	if targetID > 0 {
		where = append(where, "p.target_id = ?")
		args = append(args, targetID)
	}
	if userFilter != "" {
		where = append(where, "p.plex_username = ?")
		args = append(args, userFilter)
	}
	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	// Get total count
	var total int
	countQuery := `SELECT COUNT(*) FROM plex_auto_languages_preferences p ` + whereClause
	if err := db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count plex auto languages preferences: %w", err)
	}

	// Get paginated results
	query := `
		SELECT p.id, p.target_id, t.name as target_name, p.plex_user_id, p.plex_username, p.show_rating_key, p.show_title,
			p.audio_language_code, p.audio_codec, p.audio_channels, p.audio_channel_layout,
			p.audio_title, p.audio_display_title, p.audio_visual_impaired,
			p.subtitle_language_code, p.subtitle_forced, p.subtitle_hearing_impaired,
			p.subtitle_codec, p.subtitle_title, p.subtitle_display_title,
			p.created_at, p.updated_at
		FROM plex_auto_languages_preferences p
		JOIN targets t ON p.target_id = t.id ` + whereClause + `
		ORDER BY p.show_title, p.plex_user_id LIMIT ? OFFSET ?`
	argsWithLimit := append(append([]any{}, args...), limit, offset)

	rows, err := db.Query(query, argsWithLimit...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list plex auto languages preferences: %w", err)
	}
	defer rows.Close()

	prefs, err := scanPlexAutoLanguagesPreferences(rows)
	if err != nil {
		return nil, 0, err
	}
	return prefs, total, nil
}

// ListAllPlexAutoLanguagesHistoryFiltered retrieves history with optional user/target filter and pagination
// targetID == 0 means all targets
func (db *DB) ListAllPlexAutoLanguagesHistoryFiltered(targetID int64, userFilter string, limit, offset int) ([]*PlexAutoLanguagesHistory, int, error) {
	where := []string{}
	args := []any{}
	if targetID > 0 {
		where = append(where, "h.target_id = ?")
		args = append(args, targetID)
	}
	if userFilter != "" {
		where = append(where, "h.plex_username = ?")
		args = append(args, userFilter)
	}
	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	// Get total count
	var total int
	countQuery := `SELECT COUNT(*) FROM plex_auto_languages_history h ` + whereClause
	if err := db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count plex auto languages history: %w", err)
	}

	// Get paginated results
	query := `
		SELECT h.id, h.target_id, t.name as target_name, h.plex_user_id, h.plex_username, h.show_title, h.show_rating_key,
			h.episode_title, h.episode_rating_key, h.event_type,
			h.audio_changed, h.audio_from, h.audio_to,
			h.subtitle_changed, h.subtitle_from, h.subtitle_to,
			h.episodes_updated, h.created_at
		FROM plex_auto_languages_history h
		JOIN targets t ON h.target_id = t.id ` + whereClause + `
		ORDER BY h.created_at DESC LIMIT ? OFFSET ?`
	argsWithLimit := append(append([]any{}, args...), limit, offset)

	rows, err := db.Query(query, argsWithLimit...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list plex auto languages history: %w", err)
	}
	defer rows.Close()

	entries, err := scanPlexAutoLanguagesHistory(rows)
	if err != nil {
		return nil, 0, err
	}
	return entries, total, nil
}

// GetDistinctPlexUsersFromPreferences returns distinct usernames for filter dropdown
func (db *DB) GetDistinctPlexUsersFromPreferences(targetID int64) ([]string, error) {
	query := `
		SELECT DISTINCT plex_username FROM plex_auto_languages_preferences
		%s
		ORDER BY plex_username
	`
	var whereClause string
	var args []any
	if targetID > 0 {
		whereClause = "WHERE target_id = ? AND plex_username != ''"
		args = append(args, targetID)
	} else {
		whereClause = "WHERE plex_username != ''"
	}

	rows, err := db.Query(fmt.Sprintf(query, whereClause), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get distinct users from preferences: %w", err)
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// GetDistinctPlexUsersFromHistory returns distinct usernames for filter dropdown
func (db *DB) GetDistinctPlexUsersFromHistory() ([]string, error) {
	rows, err := db.Query(`
		SELECT DISTINCT plex_username FROM plex_auto_languages_history
		WHERE plex_username != ''
		ORDER BY plex_username
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get distinct users from history: %w", err)
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

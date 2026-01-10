package database

import (
	"database/sql"
	"time"
)

// ActiveSession represents an active playback session on a media server
type ActiveSession struct {
	ID          string
	ServerType  string
	ServerID    string
	Username    string
	MediaTitle  string
	MediaType   string
	Format      string // Video format, e.g. "1080p (H.264)" - stored in 'resolution' column
	Bitrate     int64  // Streaming bitrate in bits per second
	Transcoding bool
	Player      string
	UpdatedAt   time.Time
}

// UpsertActiveSession creates or updates an active session
func (db *db) UpsertActiveSession(session *ActiveSession) error {
	_, err := db.exec(`
		INSERT INTO active_sessions (id, server_type, server_id, username, media_title, media_type, resolution, bitrate, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			username = excluded.username,
			media_title = excluded.media_title,
			media_type = excluded.media_type,
			resolution = excluded.resolution,
			bitrate = excluded.bitrate,
			updated_at = excluded.updated_at
	`, session.ID, session.ServerType, session.ServerID, session.Username, session.MediaTitle, session.MediaType, session.Format, session.Bitrate, time.Now())
	return err
}

// ListActiveSessions returns all active sessions
func (db *db) ListActiveSessions() ([]*ActiveSession, error) {
	rows, err := db.query(`
		SELECT id, server_type, server_id, username, media_title, media_type, resolution, bitrate, updated_at
		FROM active_sessions
		ORDER BY updated_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []*ActiveSession
	for rows.Next() {
		s := &ActiveSession{}
		err := rows.Scan(&s.ID, &s.ServerType, &s.ServerID, &s.Username, &s.MediaTitle, &s.MediaType, &s.Format, &s.Bitrate, &s.UpdatedAt)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

// ListActiveSessionsByServer returns active sessions for a specific server
func (db *db) ListActiveSessionsByServer(serverType, serverID string) ([]*ActiveSession, error) {
	rows, err := db.query(`
		SELECT id, server_type, server_id, username, media_title, media_type, resolution, bitrate, updated_at
		FROM active_sessions
		WHERE server_type = ? AND server_id = ?
		ORDER BY updated_at DESC
	`, serverType, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []*ActiveSession
	for rows.Next() {
		s := &ActiveSession{}
		err := rows.Scan(&s.ID, &s.ServerType, &s.ServerID, &s.Username, &s.MediaTitle, &s.MediaType, &s.Format, &s.Bitrate, &s.UpdatedAt)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

// DeleteActiveSession removes an active session
func (db *db) DeleteActiveSession(id string) error {
	_, err := db.exec("DELETE FROM active_sessions WHERE id = ?", id)
	return err
}

// DeleteStaleSessions removes sessions not updated in the given duration
func (db *db) DeleteStaleSessions(staleAfter time.Duration) (int64, error) {
	cutoff := time.Now().Add(-staleAfter)
	result, err := db.exec("DELETE FROM active_sessions WHERE updated_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DeleteSessionsByServer removes all sessions for a specific server
func (db *db) DeleteSessionsByServer(serverType, serverID string) error {
	_, err := db.exec("DELETE FROM active_sessions WHERE server_type = ? AND server_id = ?", serverType, serverID)
	return err
}

// GetActiveSessionCount returns the number of active sessions
func (db *db) GetActiveSessionCount() (int, error) {
	var count int
	err := db.queryRow("SELECT COUNT(*) FROM active_sessions").Scan(&count)
	return count, err
}

// GetTotalActiveBitrate returns the sum of all active session bitrate in bits per second
func (db *db) GetTotalActiveBitrate() (int64, error) {
	var total sql.NullInt64
	err := db.queryRow("SELECT SUM(bitrate) FROM active_sessions").Scan(&total)
	if err != nil {
		return 0, err
	}
	return total.Int64, nil
}

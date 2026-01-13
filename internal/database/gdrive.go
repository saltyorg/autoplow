package database

import (
	"database/sql"
	"fmt"
	"time"
)

// GDriveAuthType represents the authentication method for a Google Drive account.
type GDriveAuthType string

const (
	GDriveAuthTypeOAuth          GDriveAuthType = "oauth"
	GDriveAuthTypeServiceAccount GDriveAuthType = "service_account"
)

// GDriveAccount represents a connected Google Drive account.
// RefreshToken and ServiceAccountJSON are stored encrypted.
type GDriveAccount struct {
	ID                 int64          `json:"id"`
	Subject            string         `json:"subject"`
	Email              string         `json:"email"`
	DisplayName        string         `json:"display_name"`
	RefreshToken       string         `json:"refresh_token"`
	ServiceAccountJSON string         `json:"service_account_json"`
	AuthType           GDriveAuthType `json:"auth_type"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

// UpsertGDriveAccount creates or updates a Google Drive account by subject.
func (db *db) UpsertGDriveAccount(account *GDriveAccount) error {
	if account.AuthType == "" {
		account.AuthType = GDriveAuthTypeOAuth
	}
	result, err := db.exec(`
		INSERT INTO gdrive_accounts (subject, email, display_name, refresh_token, service_account_json, auth_type, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(subject) DO UPDATE SET
			email = excluded.email,
			display_name = excluded.display_name,
			refresh_token = excluded.refresh_token,
			service_account_json = excluded.service_account_json,
			auth_type = excluded.auth_type,
			updated_at = excluded.updated_at
	`, account.Subject, account.Email, account.DisplayName, account.RefreshToken, account.ServiceAccountJSON, account.AuthType, time.Now(), time.Now())
	if err != nil {
		return fmt.Errorf("failed to upsert gdrive account: %w", err)
	}

	if account.ID == 0 {
		if id, err := result.LastInsertId(); err == nil && id != 0 {
			account.ID = id
		}
	}
	return nil
}

// GetGDriveAccount retrieves a Google Drive account by ID.
func (db *db) GetGDriveAccount(id int64) (*GDriveAccount, error) {
	var account GDriveAccount
	err := db.queryRow(`
		SELECT id, subject, email, display_name, refresh_token, service_account_json, auth_type, created_at, updated_at
		FROM gdrive_accounts WHERE id = ?
	`, id).Scan(&account.ID, &account.Subject, &account.Email, &account.DisplayName, &account.RefreshToken, &account.ServiceAccountJSON, &account.AuthType, &account.CreatedAt, &account.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get gdrive account: %w", err)
	}
	return &account, nil
}

// GetGDriveAccountBySubject retrieves a Google Drive account by subject.
func (db *db) GetGDriveAccountBySubject(subject string) (*GDriveAccount, error) {
	var account GDriveAccount
	err := db.queryRow(`
		SELECT id, subject, email, display_name, refresh_token, service_account_json, auth_type, created_at, updated_at
		FROM gdrive_accounts WHERE subject = ?
	`, subject).Scan(&account.ID, &account.Subject, &account.Email, &account.DisplayName, &account.RefreshToken, &account.ServiceAccountJSON, &account.AuthType, &account.CreatedAt, &account.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get gdrive account by subject: %w", err)
	}
	return &account, nil
}

// ListGDriveAccounts returns all connected Google Drive accounts.
func (db *db) ListGDriveAccounts() ([]*GDriveAccount, error) {
	rows, err := db.query(`
		SELECT id, subject, email, display_name, refresh_token, service_account_json, auth_type, created_at, updated_at
		FROM gdrive_accounts ORDER BY email ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list gdrive accounts: %w", err)
	}
	defer rows.Close()

	var accounts []*GDriveAccount
	for rows.Next() {
		var account GDriveAccount
		if err := rows.Scan(&account.ID, &account.Subject, &account.Email, &account.DisplayName, &account.RefreshToken, &account.ServiceAccountJSON, &account.AuthType, &account.CreatedAt, &account.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan gdrive account: %w", err)
		}
		accounts = append(accounts, &account)
	}
	return accounts, rows.Err()
}

// DeleteGDriveAccount deletes a Google Drive account by ID.
func (db *db) DeleteGDriveAccount(id int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM gdrive_accounts WHERE id = ?", id); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("gdrive account not found")
		}
		return fmt.Errorf("failed to delete gdrive account: %w", err)
	}
	return nil
}

// GDriveSyncState tracks the last sync token for a trigger.
type GDriveSyncState struct {
	TriggerID int64     `json:"trigger_id"`
	PageToken string    `json:"page_token"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetGDriveSyncState retrieves sync state for a trigger.
func (db *db) GetGDriveSyncState(triggerID int64) (*GDriveSyncState, error) {
	var state GDriveSyncState
	err := db.queryRow(`
		SELECT trigger_id, page_token, updated_at
		FROM gdrive_sync_state WHERE trigger_id = ?
	`, triggerID).Scan(&state.TriggerID, &state.PageToken, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get gdrive sync state: %w", err)
	}
	return &state, nil
}

// UpsertGDriveSyncState stores sync state for a trigger.
func (db *db) UpsertGDriveSyncState(triggerID int64, pageToken string) error {
	_, err := db.exec(`
		INSERT INTO gdrive_sync_state (trigger_id, page_token, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(trigger_id) DO UPDATE SET
			page_token = excluded.page_token,
			updated_at = excluded.updated_at
	`, triggerID, pageToken, time.Now())
	if err != nil {
		return fmt.Errorf("failed to upsert gdrive sync state: %w", err)
	}
	return nil
}

// DeleteGDriveSyncState removes sync state for a trigger.
func (db *db) DeleteGDriveSyncState(triggerID int64) error {
	if err := db.execAndVerifyAffected("DELETE FROM gdrive_sync_state WHERE trigger_id = ?", triggerID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to delete gdrive sync state: %w", err)
	}
	return nil
}

// GDriveSnapshotEntry stores the last known state of a Google Drive item.
type GDriveSnapshotEntry struct {
	TriggerID int64     `json:"trigger_id"`
	FileID    string    `json:"file_id"`
	ParentID  string    `json:"parent_id"`
	Name      string    `json:"name"`
	MimeType  string    `json:"mime_type"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ListGDriveSnapshotEntries returns all snapshot entries for a trigger.
func (db *db) ListGDriveSnapshotEntries(triggerID int64) ([]*GDriveSnapshotEntry, error) {
	rows, err := db.query(`
		SELECT trigger_id, file_id, parent_id, name, mime_type, updated_at
		FROM gdrive_snapshot_entries
		WHERE trigger_id = ?
	`, triggerID)
	if err != nil {
		return nil, fmt.Errorf("failed to list gdrive snapshot entries: %w", err)
	}
	defer rows.Close()

	var entries []*GDriveSnapshotEntry
	for rows.Next() {
		var entry GDriveSnapshotEntry
		if err := rows.Scan(&entry.TriggerID, &entry.FileID, &entry.ParentID, &entry.Name, &entry.MimeType, &entry.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan gdrive snapshot entry: %w", err)
		}
		entries = append(entries, &entry)
	}
	return entries, rows.Err()
}

// GetGDriveSnapshotEntry retrieves a snapshot entry for a trigger and file ID.
func (db *db) GetGDriveSnapshotEntry(triggerID int64, fileID string) (*GDriveSnapshotEntry, error) {
	var entry GDriveSnapshotEntry
	err := db.queryRow(`
		SELECT trigger_id, file_id, parent_id, name, mime_type, updated_at
		FROM gdrive_snapshot_entries
		WHERE trigger_id = ? AND file_id = ?
	`, triggerID, fileID).Scan(&entry.TriggerID, &entry.FileID, &entry.ParentID, &entry.Name, &entry.MimeType, &entry.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get gdrive snapshot entry: %w", err)
	}
	return &entry, nil
}

// ReplaceGDriveSnapshotEntries replaces all snapshot entries for a trigger.
func (db *db) ReplaceGDriveSnapshotEntries(triggerID int64, entries []*GDriveSnapshotEntry) error {
	return db.transaction(func(tx *sql.Tx) error {
		if _, err := tx.Exec("DELETE FROM gdrive_snapshot_entries WHERE trigger_id = ?", triggerID); err != nil {
			return fmt.Errorf("failed to clear gdrive snapshot entries: %w", err)
		}
		if len(entries) == 0 {
			return nil
		}

		stmt, err := tx.Prepare(`
			INSERT INTO gdrive_snapshot_entries (trigger_id, file_id, parent_id, name, mime_type, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare gdrive snapshot insert: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for _, entry := range entries {
			if entry == nil || entry.FileID == "" {
				continue
			}
			parentID := entry.ParentID
			name := entry.Name
			mimeType := entry.MimeType
			if _, err := stmt.Exec(triggerID, entry.FileID, parentID, name, mimeType, now); err != nil {
				return fmt.Errorf("failed to insert gdrive snapshot entry: %w", err)
			}
		}
		return nil
	})
}

// UpsertGDriveSnapshotEntry updates or inserts a single snapshot entry.
func (db *db) UpsertGDriveSnapshotEntry(entry *GDriveSnapshotEntry) error {
	if entry == nil || entry.FileID == "" {
		return nil
	}
	_, err := db.exec(`
		INSERT INTO gdrive_snapshot_entries (trigger_id, file_id, parent_id, name, mime_type, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(trigger_id, file_id) DO UPDATE SET
			parent_id = excluded.parent_id,
			name = excluded.name,
			mime_type = excluded.mime_type,
			updated_at = excluded.updated_at
	`, entry.TriggerID, entry.FileID, entry.ParentID, entry.Name, entry.MimeType, time.Now())
	if err != nil {
		return fmt.Errorf("failed to upsert gdrive snapshot entry: %w", err)
	}
	return nil
}

// DeleteGDriveSnapshotEntry removes a snapshot entry for a trigger.
func (db *db) DeleteGDriveSnapshotEntry(triggerID int64, fileID string) error {
	if fileID == "" {
		return nil
	}
	if err := db.execAndVerifyAffected(`
		DELETE FROM gdrive_snapshot_entries WHERE trigger_id = ? AND file_id = ?
	`, triggerID, fileID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to delete gdrive snapshot entry: %w", err)
	}
	return nil
}

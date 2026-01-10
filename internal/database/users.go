package database

import (
	"database/sql"
	"fmt"
	"time"
)

// UserRecord represents a user account stored in the database.
type UserRecord struct {
	ID           int64
	Username     string
	PasswordHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SessionRecord represents a user session stored in the database.
type SessionRecord struct {
	ID        string
	UserID    int64
	ExpiresAt time.Time
	CreatedAt time.Time
}

// CreateUser inserts a new user record.
func (db *db) CreateUser(username, passwordHash string) (*UserRecord, error) {
	now := time.Now()
	result, err := db.exec(`
		INSERT INTO users (username, password_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?)
	`, username, passwordHash, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get user id: %w", err)
	}

	return &UserRecord{
		ID:           id,
		Username:     username,
		PasswordHash: passwordHash,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

// GetUserByUsername retrieves a user by username.
func (db *db) GetUserByUsername(username string) (*UserRecord, error) {
	user := &UserRecord{}
	err := db.queryRow(`
		SELECT id, username, password_hash, created_at, updated_at
		FROM users WHERE username = ?
	`, username).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	return user, nil
}

// GetUserByID retrieves a user by ID.
func (db *db) GetUserByID(id int64) (*UserRecord, error) {
	user := &UserRecord{}
	err := db.queryRow(`
		SELECT id, username, password_hash, created_at, updated_at
		FROM users WHERE id = ?
	`, id).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	return user, nil
}

// UpdateUserPassword updates the user's password hash.
func (db *db) UpdateUserPassword(userID int64, passwordHash string) error {
	_, err := db.exec(`
		UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?
	`, passwordHash, time.Now(), userID)
	if err != nil {
		return fmt.Errorf("failed to update password: %w", err)
	}
	return nil
}

// UpdateUsername updates the user's username.
func (db *db) UpdateUsername(userID int64, newUsername string) error {
	_, err := db.exec(`
		UPDATE users SET username = ?, updated_at = ? WHERE id = ?
	`, newUsername, time.Now(), userID)
	if err != nil {
		return fmt.Errorf("failed to update username: %w", err)
	}
	return nil
}

// CreateSession inserts a new session record.
func (db *db) CreateSession(id string, userID int64, expiresAt time.Time) (*SessionRecord, error) {
	now := time.Now()
	_, err := db.exec(`
		INSERT INTO sessions (id, user_id, expires_at, created_at)
		VALUES (?, ?, ?, ?)
	`, id, userID, expiresAt, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return &SessionRecord{
		ID:        id,
		UserID:    userID,
		ExpiresAt: expiresAt,
		CreatedAt: now,
	}, nil
}

// GetSession retrieves a session by ID.
func (db *db) GetSession(id string) (*SessionRecord, error) {
	session := &SessionRecord{}
	err := db.queryRow(`
		SELECT id, user_id, expires_at, created_at
		FROM sessions WHERE id = ?
	`, id).Scan(&session.ID, &session.UserID, &session.ExpiresAt, &session.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}
	return session, nil
}

// DeleteSession removes a session by ID.
func (db *db) DeleteSession(id string) error {
	_, err := db.exec("DELETE FROM sessions WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	return nil
}

// ExtendSession updates a session's expiration time.
func (db *db) ExtendSession(id string, expiresAt time.Time) error {
	_, err := db.exec("UPDATE sessions SET expires_at = ? WHERE id = ?", expiresAt, id)
	if err != nil {
		return fmt.Errorf("failed to extend session: %w", err)
	}
	return nil
}

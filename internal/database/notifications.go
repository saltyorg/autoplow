package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// NotificationProvider represents a notification provider configuration
type NotificationProvider struct {
	ID        int64
	Name      string
	Type      string // discord, etc.
	Enabled   bool
	Config    map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NotificationSubscription represents a provider's subscription to an event type
type NotificationSubscription struct {
	ID         int64
	ProviderID int64
	EventType  string
	Enabled    bool
}

// NotificationLog represents a notification log entry
type NotificationLog struct {
	ID        int64
	EventType string
	Provider  string
	Title     string
	Message   string
	Status    string
	Error     string
	CreatedAt time.Time
}

// CreateNotificationProvider creates a new notification provider
func (db *DB) CreateNotificationProvider(p *NotificationProvider) error {
	configJSON, err := json.Marshal(p.Config)
	if err != nil {
		return err
	}

	result, err := db.Exec(`
		INSERT INTO notification_providers (name, type, enabled, config)
		VALUES (?, ?, ?, ?)
	`, p.Name, p.Type, p.Enabled, string(configJSON))
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get last insert id: %w", err)
	}
	p.ID = id
	return nil
}

// GetNotificationProvider retrieves a notification provider by ID
func (db *DB) GetNotificationProvider(id int64) (*NotificationProvider, error) {
	p := &NotificationProvider{}
	var configJSON string

	err := db.QueryRow(`
		SELECT id, name, type, enabled, config, created_at, updated_at
		FROM notification_providers
		WHERE id = ?
	`, id).Scan(&p.ID, &p.Name, &p.Type, &p.Enabled, &configJSON, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(configJSON), &p.Config); err != nil {
		p.Config = make(map[string]string)
	}

	return p, nil
}

// GetNotificationProviderByName retrieves a provider by name
func (db *DB) GetNotificationProviderByName(name string) (*NotificationProvider, error) {
	p := &NotificationProvider{}
	var configJSON string

	err := db.QueryRow(`
		SELECT id, name, type, enabled, config, created_at, updated_at
		FROM notification_providers
		WHERE name = ?
	`, name).Scan(&p.ID, &p.Name, &p.Type, &p.Enabled, &configJSON, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(configJSON), &p.Config); err != nil {
		p.Config = make(map[string]string)
	}

	return p, nil
}

// UpdateNotificationProvider updates a notification provider
func (db *DB) UpdateNotificationProvider(p *NotificationProvider) error {
	configJSON, err := json.Marshal(p.Config)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		UPDATE notification_providers
		SET name = ?, type = ?, enabled = ?, config = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, p.Name, p.Type, p.Enabled, string(configJSON), p.ID)
	return err
}

// DeleteNotificationProvider deletes a notification provider
func (db *DB) DeleteNotificationProvider(id int64) error {
	_, err := db.Exec("DELETE FROM notification_providers WHERE id = ?", id)
	return err
}

// ListNotificationProviders returns all notification providers
func (db *DB) ListNotificationProviders() ([]*NotificationProvider, error) {
	rows, err := db.Query(`
		SELECT id, name, type, enabled, config, created_at, updated_at
		FROM notification_providers
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var providers []*NotificationProvider
	for rows.Next() {
		p := &NotificationProvider{}
		var configJSON string

		if err := rows.Scan(&p.ID, &p.Name, &p.Type, &p.Enabled, &configJSON, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(configJSON), &p.Config); err != nil {
			p.Config = make(map[string]string)
		}

		providers = append(providers, p)
	}

	return providers, rows.Err()
}

// ListEnabledNotificationProviders returns enabled notification providers
func (db *DB) ListEnabledNotificationProviders() ([]*NotificationProvider, error) {
	rows, err := db.Query(`
		SELECT id, name, type, enabled, config, created_at, updated_at
		FROM notification_providers
		WHERE enabled = true
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var providers []*NotificationProvider
	for rows.Next() {
		p := &NotificationProvider{}
		var configJSON string

		if err := rows.Scan(&p.ID, &p.Name, &p.Type, &p.Enabled, &configJSON, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(configJSON), &p.Config); err != nil {
			p.Config = make(map[string]string)
		}

		providers = append(providers, p)
	}

	return providers, rows.Err()
}

// SetNotificationSubscription sets a subscription for a provider and event type
func (db *DB) SetNotificationSubscription(providerID int64, eventType string, enabled bool) error {
	_, err := db.Exec(`
		INSERT INTO notification_subscriptions (provider_id, event_type, enabled)
		VALUES (?, ?, ?)
		ON CONFLICT(provider_id, event_type) DO UPDATE SET enabled = ?
	`, providerID, eventType, enabled, enabled)
	return err
}

// GetNotificationSubscriptions returns all subscriptions for a provider
func (db *DB) GetNotificationSubscriptions(providerID int64) ([]*NotificationSubscription, error) {
	rows, err := db.Query(`
		SELECT id, provider_id, event_type, enabled
		FROM notification_subscriptions
		WHERE provider_id = ?
	`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []*NotificationSubscription
	for rows.Next() {
		s := &NotificationSubscription{}
		if err := rows.Scan(&s.ID, &s.ProviderID, &s.EventType, &s.Enabled); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}

	return subs, rows.Err()
}

// GetEnabledSubscriptionsForEvent returns enabled subscriptions for an event type
func (db *DB) GetEnabledSubscriptionsForEvent(eventType string) ([]*NotificationSubscription, error) {
	rows, err := db.Query(`
		SELECT ns.id, ns.provider_id, ns.event_type, ns.enabled
		FROM notification_subscriptions ns
		JOIN notification_providers np ON ns.provider_id = np.id
		WHERE ns.event_type = ? AND ns.enabled = true AND np.enabled = true
	`, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []*NotificationSubscription
	for rows.Next() {
		s := &NotificationSubscription{}
		if err := rows.Scan(&s.ID, &s.ProviderID, &s.EventType, &s.Enabled); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}

	return subs, rows.Err()
}

// LogNotification logs a notification
func (db *DB) LogNotification(log *NotificationLog) error {
	_, err := db.Exec(`
		INSERT INTO notification_log (event_type, provider, title, message, status, error)
		VALUES (?, ?, ?, ?, ?, ?)
	`, log.EventType, log.Provider, log.Title, log.Message, log.Status, log.Error)
	return err
}

// ListNotificationLogs returns recent notification logs
func (db *DB) ListNotificationLogs(limit int) ([]*NotificationLog, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := db.Query(`
		SELECT id, event_type, provider, COALESCE(title, ''), message, status, COALESCE(error, ''), created_at
		FROM notification_log
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*NotificationLog
	for rows.Next() {
		l := &NotificationLog{}
		if err := rows.Scan(&l.ID, &l.EventType, &l.Provider, &l.Title, &l.Message, &l.Status, &l.Error, &l.CreatedAt); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}

	return logs, rows.Err()
}

// ClearNotificationLogs clears old notification logs
func (db *DB) ClearNotificationLogs(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	result, err := db.Exec("DELETE FROM notification_log WHERE created_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// GetNotificationStats returns notification statistics
func (db *DB) GetNotificationStats() (sent int, failed int, err error) {
	err = db.QueryRow(`
		SELECT
			COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
		FROM notification_log
		WHERE created_at > datetime('now', '-24 hours')
	`).Scan(&sent, &failed)
	return
}

// ProviderHasSubscription checks if a provider is subscribed to an event
func (db *DB) ProviderHasSubscription(providerID int64, eventType string) (bool, error) {
	var enabled bool
	err := db.QueryRow(`
		SELECT enabled FROM notification_subscriptions
		WHERE provider_id = ? AND event_type = ?
	`, providerID, eventType).Scan(&enabled)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return enabled, nil
}

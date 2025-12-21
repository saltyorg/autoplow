package auth

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/saltyorg/autoplow/internal/database"
)

const (
	// APIKeyLength is the length of generated API keys in bytes (will be hex encoded)
	APIKeyLength = 32
)

// TriggerAuthService handles trigger authentication (both API key and basic auth)
type TriggerAuthService struct {
	db *database.DB
}

// NewTriggerAuthService creates a new trigger auth service
func NewTriggerAuthService(db *database.DB) *TriggerAuthService {
	return &TriggerAuthService{db: db}
}

// APIKeyService handles API key management
// Deprecated: Use TriggerAuthService instead
type APIKeyService struct {
	db *database.DB
}

// NewAPIKeyService creates a new API key service
// Deprecated: Use NewTriggerAuthService instead
func NewAPIKeyService(db *database.DB) *APIKeyService {
	return &APIKeyService{db: db}
}

// GenerateKey creates a new cryptographically secure API key
func (s *APIKeyService) GenerateKey() (string, error) {
	return GenerateAPIKey()
}

// GenerateAPIKey creates a new cryptographically secure API key
func GenerateAPIKey() (string, error) {
	bytes := make([]byte, APIKeyLength)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate api key: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// ValidateAPIKey checks if an API key is valid and returns the trigger ID and type
func (s *APIKeyService) ValidateAPIKey(apiKey string) (triggerID int64, triggerType string, err error) {
	err = s.db.QueryRow(`
		SELECT id, type FROM triggers WHERE api_key = ? AND enabled = true
	`, apiKey).Scan(&triggerID, &triggerType)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("failed to validate api key: %w", err)
	}
	return triggerID, triggerType, nil
}

// ValidateTriggerAPIKey checks if an API key matches a specific trigger ID
func (s *APIKeyService) ValidateTriggerAPIKey(triggerID int64, apiKey string) (bool, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM triggers WHERE id = ? AND api_key = ? AND enabled = true
	`, triggerID, apiKey).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger api key: %w", err)
	}
	return count > 0, nil
}

// RegenerateAPIKey generates a new API key for a trigger
func (s *APIKeyService) RegenerateAPIKey(triggerID int64) (string, error) {
	apiKey, err := GenerateAPIKey()
	if err != nil {
		return "", err
	}

	result, err := s.db.Exec(`
		UPDATE triggers SET api_key = ? WHERE id = ?
	`, apiKey, triggerID)
	if err != nil {
		return "", fmt.Errorf("failed to update api key: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return "", fmt.Errorf("trigger not found: %d", triggerID)
	}

	return apiKey, nil
}

// GetTriggerByAPIKey retrieves a trigger by its API key
func (s *APIKeyService) GetTriggerByAPIKey(apiKey string) (*Trigger, error) {
	trigger := &Trigger{}
	err := s.db.QueryRow(`
		SELECT id, name, type, api_key, priority, enabled, config, created_at, updated_at
		FROM triggers WHERE api_key = ?
	`, apiKey).Scan(
		&trigger.ID, &trigger.Name, &trigger.Type, &trigger.APIKey,
		&trigger.Priority, &trigger.Enabled, &trigger.Config,
		&trigger.CreatedAt, &trigger.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get trigger by api key: %w", err)
	}
	return trigger, nil
}

// Trigger represents a webhook/inotify trigger
type Trigger struct {
	ID        int64
	Name      string
	Type      string
	APIKey    sql.NullString
	Priority  int
	Enabled   bool
	Config    string
	CreatedAt string
	UpdatedAt string
}

// GenerateKey creates a new cryptographically secure API key
func (s *TriggerAuthService) GenerateKey() (string, error) {
	return GenerateAPIKey()
}

// ValidateAPIKey checks if an API key is valid and returns the trigger ID and type
func (s *TriggerAuthService) ValidateAPIKey(apiKey string) (triggerID int64, triggerType string, err error) {
	err = s.db.QueryRow(`
		SELECT id, type FROM triggers WHERE api_key = ? AND enabled = true AND auth_type = 'api_key'
	`, apiKey).Scan(&triggerID, &triggerType)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("failed to validate api key: %w", err)
	}
	return triggerID, triggerType, nil
}

// ValidateTriggerAPIKey checks if an API key matches a specific trigger ID
func (s *TriggerAuthService) ValidateTriggerAPIKey(triggerID int64, apiKey string) (bool, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM triggers WHERE id = ? AND api_key = ? AND enabled = true AND auth_type = 'api_key'
	`, triggerID, apiKey).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger api key: %w", err)
	}
	return count > 0, nil
}

// ValidateTriggerBasicAuth checks username/password for a specific trigger ID
func (s *TriggerAuthService) ValidateTriggerBasicAuth(triggerID int64, username, password string) (bool, error) {
	var encryptedPassword sql.NullString
	err := s.db.QueryRow(`
		SELECT password_hash FROM triggers WHERE id = ? AND username = ? AND enabled = true AND auth_type = 'basic'
	`, triggerID, username).Scan(&encryptedPassword)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger basic auth: %w", err)
	}

	if !encryptedPassword.Valid || encryptedPassword.String == "" {
		return false, nil
	}

	// Decrypt the stored password and compare
	storedPassword, err := DecryptTriggerPassword(encryptedPassword.String)
	if err != nil {
		return false, fmt.Errorf("failed to decrypt password: %w", err)
	}

	return password == storedPassword, nil
}

// GetTriggerAuthType returns the auth type for a trigger
func (s *TriggerAuthService) GetTriggerAuthType(triggerID int64) (database.AuthType, error) {
	var authType sql.NullString
	err := s.db.QueryRow(`
		SELECT auth_type FROM triggers WHERE id = ? AND enabled = true
	`, triggerID).Scan(&authType)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("trigger not found or disabled")
	}
	if err != nil {
		return "", fmt.Errorf("failed to get trigger auth type: %w", err)
	}

	if !authType.Valid || authType.String == "" {
		return database.AuthTypeAPIKey, nil // default for backward compatibility
	}

	return database.AuthType(authType.String), nil
}

// RegenerateAPIKey generates a new API key for a trigger
func (s *TriggerAuthService) RegenerateAPIKey(triggerID int64) (string, error) {
	apiKey, err := GenerateAPIKey()
	if err != nil {
		return "", err
	}

	result, err := s.db.Exec(`
		UPDATE triggers SET api_key = ? WHERE id = ? AND auth_type = 'api_key'
	`, apiKey, triggerID)
	if err != nil {
		return "", fmt.Errorf("failed to update api key: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return "", fmt.Errorf("trigger not found or not using API key auth: %d", triggerID)
	}

	return apiKey, nil
}

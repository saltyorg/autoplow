package auth

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/saltyorg/autoplow/internal/database"
)

const (
	// APIKeyLength is the length of generated API keys in bytes (will be hex encoded)
	APIKeyLength = 32
)

// TriggerAuthService handles trigger authentication (both API key and basic auth)
type TriggerAuthService struct {
	db *database.Manager
}

// NewTriggerAuthService creates a new trigger auth service
func NewTriggerAuthService(db *database.Manager) *TriggerAuthService {
	return &TriggerAuthService{db: db}
}

// APIKeyService handles API key management
// Deprecated: Use TriggerAuthService instead
type APIKeyService struct {
	db *database.Manager
}

// NewAPIKeyService creates a new API key service
// Deprecated: Use NewTriggerAuthService instead
func NewAPIKeyService(db *database.Manager) *APIKeyService {
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
	trigger, err := s.db.GetTriggerByAPIKey(apiKey)
	if err != nil {
		return 0, "", fmt.Errorf("failed to validate api key: %w", err)
	}
	if trigger == nil || !trigger.Enabled {
		return 0, "", nil
	}
	return trigger.ID, string(trigger.Type), nil
}

// ValidateTriggerAPIKey checks if an API key matches a specific trigger ID
func (s *APIKeyService) ValidateTriggerAPIKey(triggerID int64, apiKey string) (bool, error) {
	trigger, err := s.db.GetTrigger(triggerID)
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger api key: %w", err)
	}
	if trigger == nil || !trigger.Enabled {
		return false, nil
	}
	return trigger.APIKey == apiKey, nil
}

// RegenerateAPIKey generates a new API key for a trigger
func (s *APIKeyService) RegenerateAPIKey(triggerID int64) (string, error) {
	apiKey, err := GenerateAPIKey()
	if err != nil {
		return "", err
	}

	if err := s.db.UpdateTriggerAPIKey(triggerID, apiKey); err != nil {
		return "", fmt.Errorf("failed to update api key: %w", err)
	}

	return apiKey, nil
}

// GetTriggerByAPIKey retrieves a trigger by its API key
func (s *APIKeyService) GetTriggerByAPIKey(apiKey string) (*Trigger, error) {
	record, err := s.db.GetTriggerByAPIKey(apiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get trigger by api key: %w", err)
	}
	if record == nil {
		return nil, nil
	}

	configJSON, _ := json.Marshal(record.Config)

	createdAt := record.CreatedAt.Format(time.RFC3339)
	updatedAt := record.UpdatedAt.Format(time.RFC3339)

	return &Trigger{
		ID:        record.ID,
		Name:      record.Name,
		Type:      string(record.Type),
		APIKey:    sql.NullString{String: record.APIKey, Valid: record.APIKey != ""},
		Priority:  record.Priority,
		Enabled:   record.Enabled,
		Config:    string(configJSON),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
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
	trigger, err := s.db.GetTriggerByAPIKey(apiKey)
	if err != nil {
		return 0, "", fmt.Errorf("failed to validate api key: %w", err)
	}
	if trigger == nil || !trigger.Enabled || trigger.AuthType != database.AuthTypeAPIKey {
		return 0, "", nil
	}
	return trigger.ID, string(trigger.Type), nil
}

// ValidateTriggerAPIKey checks if an API key matches a specific trigger ID
func (s *TriggerAuthService) ValidateTriggerAPIKey(triggerID int64, apiKey string) (bool, error) {
	trigger, err := s.db.GetTrigger(triggerID)
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger api key: %w", err)
	}
	if trigger == nil || !trigger.Enabled || trigger.AuthType != database.AuthTypeAPIKey {
		return false, nil
	}
	return trigger.APIKey == apiKey, nil
}

// ValidateTriggerBasicAuth checks username/password for a specific trigger ID
func (s *TriggerAuthService) ValidateTriggerBasicAuth(triggerID int64, username, password string) (bool, error) {
	trigger, err := s.db.GetTrigger(triggerID)
	if err != nil {
		return false, fmt.Errorf("failed to validate trigger basic auth: %w", err)
	}
	if trigger == nil || !trigger.Enabled || trigger.AuthType != database.AuthTypeBasic {
		return false, nil
	}
	if trigger.Username != username || trigger.Password == "" {
		return false, nil
	}

	// Decrypt the stored password and compare
	storedPassword, err := DecryptTriggerPassword(trigger.Password)
	if err != nil {
		return false, fmt.Errorf("failed to decrypt password: %w", err)
	}

	return password == storedPassword, nil
}

// GetTriggerAuthType returns the auth type for a trigger
func (s *TriggerAuthService) GetTriggerAuthType(triggerID int64) (database.AuthType, error) {
	trigger, err := s.db.GetTrigger(triggerID)
	if err != nil {
		return "", fmt.Errorf("failed to get trigger auth type: %w", err)
	}
	if trigger == nil || !trigger.Enabled {
		return "", fmt.Errorf("trigger not found or disabled")
	}
	if trigger.AuthType == "" {
		return database.AuthTypeAPIKey, nil
	}
	return trigger.AuthType, nil
}

// RegenerateAPIKey generates a new API key for a trigger
func (s *TriggerAuthService) RegenerateAPIKey(triggerID int64) (string, error) {
	apiKey, err := GenerateAPIKey()
	if err != nil {
		return "", err
	}

	trigger, err := s.db.GetTrigger(triggerID)
	if err != nil {
		return "", fmt.Errorf("failed to update api key: %w", err)
	}
	if trigger == nil || trigger.AuthType != database.AuthTypeAPIKey {
		return "", fmt.Errorf("trigger not found or not using API key auth: %d", triggerID)
	}
	if err := s.db.UpdateTriggerAPIKey(triggerID, apiKey); err != nil {
		return "", fmt.Errorf("failed to update api key: %w", err)
	}

	return apiKey, nil
}

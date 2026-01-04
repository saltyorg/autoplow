package auth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// LoadOrCreateTriggerPasswordKey loads the trigger password key from a file next to the database,
// creating a new random key if the file does not exist. The key is stored in base64 format.
func LoadOrCreateTriggerPasswordKey(dbPath string) error {
	keyPath := dbPath + ".key"

	// Attempt to read existing key
	if data, err := os.ReadFile(keyPath); err == nil {
		keyBytes, err := parseKeyFile(data)
		if err != nil {
			return fmt.Errorf("invalid trigger key file %s: %w", keyPath, err)
		}
		triggerPasswordKey = keyBytes
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to read trigger key file %s: %w", keyPath, err)
	}

	// Create directory in case it doesn't exist
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	keyBytes, err := generateRandomKey()
	if err != nil {
		return fmt.Errorf("failed to generate trigger key: %w", err)
	}

	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(keyBytes)), 0o600); err != nil {
		return fmt.Errorf("failed to write trigger key file %s: %w", keyPath, err)
	}

	triggerPasswordKey = keyBytes
	return nil
}

// RegenerateTriggerPasswordKey replaces the per-install key, writes it to disk, and
// re-encrypts all trigger passwords using the new key. Returns migrated and failed counts.
func RegenerateTriggerPasswordKey(db *database.DB) (int, int, error) {
	dbPath := db.Path()
	keyPath := dbPath + ".key"

	oldKey := currentTriggerPasswordKey()
	if len(oldKey) == 0 {
		oldKey = legacyTriggerPasswordKey
	}

	newKey, err := generateRandomKey()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to generate new trigger key: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return 0, 0, fmt.Errorf("failed to ensure key directory: %w", err)
	}

	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(newKey)), 0o600); err != nil {
		return 0, 0, fmt.Errorf("failed to write new trigger key file: %w", err)
	}

	// Set the new key for future operations
	triggerPasswordKey = newKey

	migrated, failed, err := migrateTriggerPasswordsWithFallback(db, oldKey, legacyTriggerPasswordKey)
	if err != nil {
		return migrated, failed, err
	}

	log.Info().Int("migrated", migrated).Int("failed", failed).Msg("Regenerated trigger key and re-encrypted trigger passwords")
	return migrated, failed, nil
}

// parseKeyFile accepts a base64-encoded key or a raw 16/24/32-byte value.
func parseKeyFile(data []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, fmt.Errorf("key file is empty")
	}

	// Try base64 first
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil && isValidAESKeyLen(len(decoded)) {
		return decoded, nil
	}

	raw := []byte(trimmed)
	if !isValidAESKeyLen(len(raw)) {
		return nil, fmt.Errorf("key must be 16, 24, or 32 bytes when decoded")
	}
	return raw, nil
}

func isValidAESKeyLen(n int) bool {
	return n == 16 || n == 24 || n == 32
}

func generateRandomKey() ([]byte, error) {
	key := make([]byte, 32) // use 32 bytes for AES-256
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

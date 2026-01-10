package auth

import (
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// MigrateTriggerPasswords re-encrypts all basic-auth trigger passwords using the current
// per-install key. If the current key is still the legacy key, no migration is needed.
func MigrateTriggerPasswords(db *database.Manager) error {
	if len(triggerPasswordKey) == 0 || string(triggerPasswordKey) == string(legacyTriggerPasswordKey) {
		// No custom key loaded; nothing to migrate.
		return nil
	}

	_, _, err := migrateTriggerPasswordsWithFallback(db, triggerPasswordKey, legacyTriggerPasswordKey)
	return err
}

// migrateTriggerPasswordsWithFallback re-encrypts using current key, decrypting with provided fallback keys.
func migrateTriggerPasswordsWithFallback(db *database.Manager, fallbackKeys ...[]byte) (int, int, error) {
	triggers, err := db.ListTriggers()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list triggers for password migration: %w", err)
	}

	var migrated, failed int
	currentKey := currentTriggerPasswordKey()

	for _, trigger := range triggers {
		if trigger.AuthType != database.AuthTypeBasic || trigger.Password == "" {
			continue
		}

		plaintext, err := decryptTriggerPasswordWithKey(currentKey, trigger.Password)
		if err != nil {
			for _, fk := range fallbackKeys {
				if plaintext, err = decryptTriggerPasswordWithKey(fk, trigger.Password); err == nil {
					break
				}
			}
		}
		if err != nil {
			failed++
			log.Warn().Int64("trigger_id", trigger.ID).Err(err).Msg("Failed to decrypt trigger password during migration")
			continue
		}

		newEncrypted, err := encryptTriggerPasswordWithKey(currentKey, plaintext)
		if err != nil {
			failed++
			log.Warn().Int64("trigger_id", trigger.ID).Err(err).Msg("Failed to re-encrypt trigger password during migration")
			continue
		}

		if err := db.UpdateTriggerAuth(trigger.ID, trigger.AuthType, trigger.APIKey, trigger.Username, newEncrypted); err != nil {
			failed++
			log.Warn().Int64("trigger_id", trigger.ID).Err(err).Msg("Failed to update trigger password during migration")
			continue
		}

		migrated++
	}

	if migrated > 0 {
		log.Info().Int("count", migrated).Msg("Migrated trigger passwords to per-install key")
	}
	if failed > 0 {
		log.Warn().Int("failed", failed).Msg("Some trigger passwords could not be migrated; they remain on the fallback key")
	}

	return migrated, failed, nil
}

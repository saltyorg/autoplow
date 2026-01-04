package auth

import (
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// MigrateTriggerPasswords re-encrypts all basic-auth trigger passwords using the current
// per-install key. If the current key is still the legacy key, no migration is needed.
func MigrateTriggerPasswords(db *database.DB) error {
	if len(triggerPasswordKey) == 0 || string(triggerPasswordKey) == string(legacyTriggerPasswordKey) {
		// No custom key loaded; nothing to migrate.
		return nil
	}

	_, _, err := migrateTriggerPasswordsWithFallback(db, triggerPasswordKey, legacyTriggerPasswordKey)
	return err
}

// migrateTriggerPasswordsWithFallback re-encrypts using current key, decrypting with provided fallback keys.
func migrateTriggerPasswordsWithFallback(db *database.DB, fallbackKeys ...[]byte) (int, int, error) {
	rows, err := db.Query(`
		SELECT id, password_hash FROM triggers
		WHERE auth_type = ? AND password_hash IS NOT NULL AND password_hash != ''
	`, database.AuthTypeBasic)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list triggers for password migration: %w", err)
	}
	defer rows.Close()

	var migrated, failed int
	currentKey := currentTriggerPasswordKey()

	for rows.Next() {
		var id int64
		var encrypted sql.NullString
		if err := rows.Scan(&id, &encrypted); err != nil {
			return migrated, failed, fmt.Errorf("failed to scan trigger: %w", err)
		}
		if !encrypted.Valid || encrypted.String == "" {
			continue
		}

		plaintext, err := decryptTriggerPasswordWithKey(currentKey, encrypted.String)
		if err != nil {
			for _, fk := range fallbackKeys {
				if plaintext, err = decryptTriggerPasswordWithKey(fk, encrypted.String); err == nil {
					break
				}
			}
		}
		if err != nil {
			failed++
			log.Warn().Int64("trigger_id", id).Err(err).Msg("Failed to decrypt trigger password during migration")
			continue
		}

		newEncrypted, err := encryptTriggerPasswordWithKey(currentKey, plaintext)
		if err != nil {
			failed++
			log.Warn().Int64("trigger_id", id).Err(err).Msg("Failed to re-encrypt trigger password during migration")
			continue
		}

		if _, err := db.Exec(`
			UPDATE triggers SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
		`, newEncrypted, id); err != nil {
			failed++
			log.Warn().Int64("trigger_id", id).Err(err).Msg("Failed to update trigger password during migration")
			continue
		}

		migrated++
	}

	if err := rows.Err(); err != nil {
		return migrated, failed, fmt.Errorf("error iterating triggers for migration: %w", err)
	}

	if migrated > 0 {
		log.Info().Int("count", migrated).Msg("Migrated trigger passwords to per-install key")
	}
	if failed > 0 {
		log.Warn().Int("failed", failed).Msg("Some trigger passwords could not be migrated; they remain on the fallback key")
	}

	return migrated, failed, nil
}

package database

import (
	"fmt"
)

// ListUploadRemotes returns distinct remote names present in upload history.
func (db *DB) ListUploadRemotes() ([]string, error) {
	rows, err := db.Query(`SELECT DISTINCT remote_name FROM upload_history ORDER BY remote_name`)
	if err != nil {
		return nil, fmt.Errorf("failed to list upload remotes: %w", err)
	}
	defer rows.Close()

	var remotes []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan remote: %w", err)
		}
		if name != "" {
			remotes = append(remotes, name)
		}
	}

	return remotes, rows.Err()
}

package database

import (
	"database/sql"
	"fmt"
	"strings"
)

// ScanTargetPath stores the mapped scan path used for a target.
type ScanTargetPath struct {
	ScanID   int64
	TargetID int64
	ScanPath string
}

// SetScanTargetPaths replaces the stored target paths for a scan.
func (db *db) SetScanTargetPaths(scanID int64, targetPaths map[int64]string) error {
	if len(targetPaths) == 0 {
		return nil
	}

	return db.transaction(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM scan_target_paths WHERE scan_id = ?`, scanID); err != nil {
			return fmt.Errorf("failed to clear scan target paths: %w", err)
		}

		stmt, err := tx.Prepare(`INSERT INTO scan_target_paths (scan_id, target_id, scan_path) VALUES (?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("failed to prepare scan target path insert: %w", err)
		}
		defer stmt.Close()

		for targetID, path := range targetPaths {
			if _, err := stmt.Exec(scanID, targetID, path); err != nil {
				return fmt.Errorf("failed to insert scan target path: %w", err)
			}
		}

		return nil
	})
}

// ListScanTargetPaths returns target path mappings for the provided scan IDs.
func (db *db) ListScanTargetPaths(scanIDs []int64) (map[int64][]ScanTargetPath, error) {
	results := make(map[int64][]ScanTargetPath)
	if len(scanIDs) == 0 {
		return results, nil
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(scanIDs)), ",")
	args := make([]any, len(scanIDs))
	for i, id := range scanIDs {
		args[i] = id
	}

	rows, err := db.query(
		fmt.Sprintf(`SELECT scan_id, target_id, scan_path FROM scan_target_paths WHERE scan_id IN (%s) ORDER BY scan_id, target_id`, placeholders),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list scan target paths: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var record ScanTargetPath
		if err := rows.Scan(&record.ScanID, &record.TargetID, &record.ScanPath); err != nil {
			return nil, fmt.Errorf("failed to scan scan target path: %w", err)
		}
		results[record.ScanID] = append(results[record.ScanID], record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate scan target paths: %w", err)
	}

	return results, nil
}

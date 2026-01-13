package database

import "fmt"

// Optimize runs SQLite's PRAGMA optimize to refresh planner stats.
func (db *db) Optimize() error {
	if db == nil || db.conn == nil {
		return fmt.Errorf("database not initialized")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.exec("PRAGMA optimize"); err != nil {
		return fmt.Errorf("failed to optimize database: %w", err)
	}

	return nil
}

// Vacuum rebuilds the database file to reclaim unused space.
func (db *db) Vacuum() error {
	if db == nil || db.conn == nil {
		return fmt.Errorf("database not initialized")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.exec("VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	return nil
}

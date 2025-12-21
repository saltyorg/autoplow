package database

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database connection
type DB struct {
	*sql.DB
	path string
	mu   sync.RWMutex
}

// New creates a new database connection
func New(path string) (*DB, error) {
	// SQLite connection with WAL mode for better concurrency
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", path)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings for SQLite
	// SQLite serializes all writes, so limiting to 1 connection prevents "database is locked" errors
	// With WAL mode, reads can still happen concurrently with the single write connection
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	log.Debug().Str("path", path).Msg("Database connection established")

	return &DB{
		DB:   db,
		path: path,
	}, nil
}

// Path returns the database file path
func (db *DB) Path() string {
	return db.path
}

// IsFirstRun checks if this is the first run (no users exist)
func (db *DB) IsFirstRun() (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check users: %w", err)
	}
	return count == 0, nil
}

// Transaction wraps a function in a database transaction
func (db *DB) Transaction(fn func(*sql.Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().Err(rbErr).Msg("Failed to rollback transaction")
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

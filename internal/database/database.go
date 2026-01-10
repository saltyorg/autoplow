package database

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

// db wraps the SQLite database connection.
type db struct {
	conn *sql.DB
	path string
	mu   sync.RWMutex
}

// New creates a new database manager.
func New(path string) (*Manager, error) {
	db, err := newDB(path)
	if err != nil {
		return nil, err
	}
	return newManager(db), nil
}

func newDB(path string) (*db, error) {
	// SQLite connection with WAL mode for better concurrency
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", path)

	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings for SQLite
	// SQLite serializes all writes, so limiting to 1 connection prevents "database is locked" errors
	// With WAL mode, reads can still happen concurrently with the single write connection
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	log.Debug().Str("path", path).Msg("Database connection established")

	return &db{
		conn: conn,
		path: path,
	}, nil
}

// Path returns the database file path
func (db *db) Path() string {
	return db.path
}

// Close closes the underlying database connection.
func (db *db) Close() error {
	if db == nil || db.conn == nil {
		return nil
	}
	return db.conn.Close()
}

// IsFirstRun checks if this is the first run (no users exist)
func (db *db) IsFirstRun() (bool, error) {
	var count int
	err := db.queryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check users: %w", err)
	}
	return count == 0, nil
}

// transaction wraps a function in a database transaction.
func (db *db) transaction(fn func(*sql.Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.begin()
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

// execAndVerifyAffected executes a query and returns an error if no rows were affected.
// This is useful for UPDATE and DELETE operations where at least one row should be modified.
func (db *db) execAndVerifyAffected(query string, args ...any) error {
	result, err := db.exec(query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

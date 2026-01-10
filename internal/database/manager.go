package database

// Manager is the approved entrypoint for database access across the app.
// It intentionally exposes only the database package API (no raw *sql.DB).
type Manager struct {
	*db
}

func newManager(db *db) *Manager {
	return &Manager{db: db}
}

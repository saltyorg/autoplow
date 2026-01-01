package config

import (
	"strconv"
	"time"
)

// SettingsGetter is an interface for retrieving settings from storage
type SettingsGetter interface {
	GetSetting(key string) (string, error)
}

// Loader provides typed access to settings with default values
type Loader struct {
	db SettingsGetter
}

// NewLoader creates a new settings loader
func NewLoader(db SettingsGetter) *Loader {
	return &Loader{db: db}
}

// Int retrieves an integer setting, returning defaultVal if not found or invalid
func (l *Loader) Int(key string, defaultVal int) int {
	if val, _ := l.db.GetSetting(key); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			return v
		}
	}
	return defaultVal
}

// Int64 retrieves an int64 setting, returning defaultVal if not found or invalid
func (l *Loader) Int64(key string, defaultVal int64) int64 {
	if val, _ := l.db.GetSetting(key); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			return v
		}
	}
	return defaultVal
}

// Bool retrieves a boolean setting, returning defaultVal if not found
// Recognizes "true" as true, anything else (including "false") as false
func (l *Loader) Bool(key string, defaultVal bool) bool {
	if val, _ := l.db.GetSetting(key); val != "" {
		return val == "true"
	}
	return defaultVal
}

// BoolDefaultTrue retrieves a boolean setting where the default is true
// Returns false only if the value is explicitly "false"
func (l *Loader) BoolDefaultTrue(key string) bool {
	if val, _ := l.db.GetSetting(key); val != "" {
		return val != "false"
	}
	return true
}

// String retrieves a string setting, returning defaultVal if not found or empty
func (l *Loader) String(key, defaultVal string) string {
	if val, _ := l.db.GetSetting(key); val != "" {
		return val
	}
	return defaultVal
}

// Duration retrieves a duration setting, returning defaultVal if not found or invalid
// Expects the value to be in Go duration format (e.g., "1h30m", "5s")
func (l *Loader) Duration(key string, defaultVal time.Duration) time.Duration {
	if val, _ := l.db.GetSetting(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

// DurationMinutes retrieves a duration setting stored as minutes
func (l *Loader) DurationMinutes(key string, defaultMinutes int) time.Duration {
	minutes := l.Int(key, defaultMinutes)
	return time.Duration(minutes) * time.Minute
}

// DurationSeconds retrieves a duration setting stored as seconds
func (l *Loader) DurationSeconds(key string, defaultSeconds int) time.Duration {
	seconds := l.Int(key, defaultSeconds)
	return time.Duration(seconds) * time.Second
}

// Float64 retrieves a float64 setting, returning defaultVal if not found or invalid
func (l *Loader) Float64(key string, defaultVal float64) float64 {
	if val, _ := l.db.GetSetting(key); val != "" {
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			return v
		}
	}
	return defaultVal
}

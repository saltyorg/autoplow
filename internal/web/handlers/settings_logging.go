package handlers

import (
	"net/http"
	"strconv"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/logging"
)

// LoggingSettings holds the logging configuration for display
type LoggingSettings struct {
	LogLevel      string
	LogMaxSizeMB  int
	LogMaxBackups int
	LogMaxAgeDays int
	LogCompress   bool
}

// SettingsLoggingPage renders the logging settings page
func (h *Handlers) SettingsLoggingPage(w http.ResponseWriter, r *http.Request) {
	loader := config.NewLoader(h.db)

	settings := LoggingSettings{
		LogLevel:      loader.String("log.level", "info"),
		LogMaxSizeMB:  loader.Int("log.max_size_mb", logging.DefaultMaxSizeMB),
		LogMaxBackups: loader.Int("log.max_backups", logging.DefaultMaxBackups),
		LogMaxAgeDays: loader.Int("log.max_age_days", logging.DefaultMaxAgeDays),
		LogCompress:   loader.Bool("log.compress", logging.DefaultCompress),
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":      "logging",
		"Settings": settings,
	})
}

// SettingsLoggingUpdate handles logging settings updates
func (h *Handlers) SettingsLoggingUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/logging")
		return
	}

	// Parse form values
	logLevel := r.FormValue("log_level")
	logMaxSizeMB, _ := strconv.Atoi(r.FormValue("log_max_size_mb"))
	logMaxBackups, _ := strconv.Atoi(r.FormValue("log_max_backups"))
	logMaxAgeDays, _ := strconv.Atoi(r.FormValue("log_max_age_days"))
	logCompress := r.FormValue("log_compress") == "on"

	// Validate log level
	switch logLevel {
	case "trace", "debug", "info":
		// valid
	default:
		logLevel = "info"
	}

	// Validate numeric values
	if logMaxSizeMB < 1 {
		logMaxSizeMB = logging.DefaultMaxSizeMB
	}
	if logMaxBackups < 0 {
		logMaxBackups = logging.DefaultMaxBackups
	}
	if logMaxAgeDays < 0 {
		logMaxAgeDays = logging.DefaultMaxAgeDays
	}

	// Save to database
	var saveErr error
	if err := h.db.SetSetting("log.level", logLevel); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("log.max_size_mb", strconv.Itoa(logMaxSizeMB)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("log.max_backups", strconv.Itoa(logMaxBackups)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("log.max_age_days", strconv.Itoa(logMaxAgeDays)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("log.compress", strconv.FormatBool(logCompress)); err != nil {
		saveErr = err
	}

	if saveErr != nil {
		h.flashErr(w, "Failed to save some settings")
		h.redirect(w, r, "/settings/logging")
		return
	}

	// Apply logging changes immediately
	loader := config.NewLoader(h.db)
	logging.Apply(logLevel, loader, logging.FilePathForDB(h.db.Path()))

	h.flash(w, "Logging settings saved")
	h.redirect(w, r, "/settings/logging")
}

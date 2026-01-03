package handlers

import (
	"net/http"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/saltyorg/autoplow/internal/config"
)

// GeneralSettings holds the general configuration for display
type GeneralSettings struct {
	LogLevel          string
	MaxRetries        int
	CleanupDays       int
	ScanningEnabled   bool
	UploadsEnabled    bool
	UseBinaryUnits    bool
	UseBitsForBitrate bool
}

// SettingsPage renders the general settings page
func (h *Handlers) SettingsPage(w http.ResponseWriter, r *http.Request) {
	loader := config.NewLoader(h.db)

	settings := GeneralSettings{
		LogLevel:          loader.String("log.level", "info"),
		MaxRetries:        loader.Int("processor.max_retries", 3),
		CleanupDays:       loader.Int("processor.cleanup_days", 7),
		ScanningEnabled:   loader.BoolDefaultTrue("scanning.enabled"),
		UploadsEnabled:    loader.BoolDefaultTrue("uploads.enabled"),
		UseBinaryUnits:    loader.BoolDefaultTrue("display.use_binary_units"),
		UseBitsForBitrate: loader.BoolDefaultTrue("display.use_bits_for_bitrate"),
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":      "general",
		"Settings": settings,
	})
}

// SettingsUpdate handles general settings updates
func (h *Handlers) SettingsUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings")
		return
	}

	// Get current uploads.enabled state before update
	loader := config.NewLoader(h.db)
	wasUploadsEnabled := loader.BoolDefaultTrue("uploads.enabled")

	// Parse form values
	logLevel := r.FormValue("log_level")
	maxRetries, _ := strconv.Atoi(r.FormValue("max_retries"))
	cleanupDays, _ := strconv.Atoi(r.FormValue("cleanup_days"))
	scanningEnabled := r.FormValue("scanning_enabled") == "on"
	uploadsEnabled := r.FormValue("uploads_enabled") == "on"
	useBinaryUnits := r.FormValue("use_binary_units") == "on"
	useBitsForBitrate := r.FormValue("use_bits_for_bitrate") == "on"

	// Validate log level
	switch logLevel {
	case "trace", "debug", "info":
		// valid
	default:
		logLevel = "info"
	}

	// Validate that at least one of scanning or uploads is enabled
	if !scanningEnabled && !uploadsEnabled {
		h.flashErr(w, "At least one of Scanning or Uploads must be enabled")
		h.redirect(w, r, "/settings")
		return
	}

	// Validate
	if maxRetries < 0 {
		maxRetries = 3
	}
	if cleanupDays < 0 {
		cleanupDays = 0 // 0 = disabled
	}

	// Save to database
	var saveErr error
	if err := h.db.SetSetting("log.level", logLevel); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("processor.max_retries", strconv.Itoa(maxRetries)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("processor.cleanup_days", strconv.Itoa(cleanupDays)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("scanning.enabled", strconv.FormatBool(scanningEnabled)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("uploads.enabled", strconv.FormatBool(uploadsEnabled)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("display.use_binary_units", strconv.FormatBool(useBinaryUnits)); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("display.use_bits_for_bitrate", strconv.FormatBool(useBitsForBitrate)); err != nil {
		saveErr = err
	}

	if saveErr != nil {
		h.flashErr(w, "Failed to save some settings")
		h.redirect(w, r, "/settings")
		return
	}

	// Apply log level change immediately
	switch logLevel {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Handle upload subsystem toggle
	if h.uploadSubsystemToggler != nil {
		if !wasUploadsEnabled && uploadsEnabled {
			// Uploads were disabled, now enabled - start subsystem
			if err := h.uploadSubsystemToggler.StartUploadSubsystem(); err != nil {
				log.Error().Err(err).Msg("Failed to start upload subsystem")
				h.flashErr(w, "Settings saved but failed to start upload subsystem")
				h.redirect(w, r, "/settings")
				return
			}
			log.Info().Msg("Upload subsystem started via settings change")
		} else if wasUploadsEnabled && !uploadsEnabled {
			// Uploads were enabled, now disabled - stop subsystem
			if err := h.uploadSubsystemToggler.StopUploadSubsystem(); err != nil {
				log.Error().Err(err).Msg("Failed to stop upload subsystem")
				h.flashErr(w, "Settings saved but failed to stop upload subsystem")
				h.redirect(w, r, "/settings")
				return
			}
			log.Info().Msg("Upload subsystem stopped via settings change")
		}
	}

	h.flash(w, "Settings saved")
	h.redirect(w, r, "/settings")
}

// ClearUploadHistory handles clearing all upload history
func (h *Handlers) ClearUploadHistory(w http.ResponseWriter, r *http.Request) {
	count, err := h.db.ClearUploadHistory()
	if err != nil {
		h.flashErr(w, "Failed to clear upload history")
		h.redirect(w, r, "/settings")
		return
	}

	h.flash(w, "Cleared "+strconv.FormatInt(count, 10)+" upload history records")
	h.redirect(w, r, "/settings")
}

// SettingsAboutPage renders the about settings page
func (h *Handlers) SettingsAboutPage(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, "settings.html", map[string]any{
		"Tab": "about",
	})
}

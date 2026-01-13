package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/config"
)

// GeneralSettings holds the general configuration for display
type GeneralSettings struct {
	CleanupDays       int
	ScanningEnabled   bool
	UploadsEnabled    bool
	UseBinaryUnits    bool
	UseBitsForBitrate bool
	DateFormat        string
	TimeFormat        string
}

// SettingsPage renders the general settings page
func (h *Handlers) SettingsPage(w http.ResponseWriter, r *http.Request) {
	loader := config.NewLoader(h.db)

	settings := GeneralSettings{
		CleanupDays:       loader.Int("processor.cleanup_days", 7),
		ScanningEnabled:   loader.BoolDefaultTrue("scanning.enabled"),
		UploadsEnabled:    loader.BoolDefaultTrue("uploads.enabled"),
		UseBinaryUnits:    loader.BoolDefaultTrue("display.use_binary_units"),
		UseBitsForBitrate: loader.BoolDefaultTrue("display.use_bits_for_bitrate"),
		DateFormat:        loader.String("display.date_format", "ymd"),
		TimeFormat:        loader.String("display.time_format", "24h"),
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
	cleanupDays, _ := strconv.Atoi(r.FormValue("cleanup_days"))
	scanningEnabled := r.FormValue("scanning_enabled") == "on"
	uploadsEnabled := r.FormValue("uploads_enabled") == "on"
	useBinaryUnits := r.FormValue("use_binary_units") == "on"
	useBitsForBitrate := r.FormValue("use_bits_for_bitrate") == "on"
	dateFormat := r.FormValue("date_format")
	timeFormat := r.FormValue("time_format")

	// Validate date format
	switch dateFormat {
	case "ymd", "dmy", "mdy":
		// valid
	default:
		dateFormat = "ymd"
	}

	// Validate time format
	switch timeFormat {
	case "24h", "12h":
		// valid
	default:
		timeFormat = "24h"
	}

	// Validate that at least one of scanning or uploads is enabled
	if !scanningEnabled && !uploadsEnabled {
		h.flashErr(w, "At least one of Scanning or Uploads must be enabled")
		h.redirect(w, r, "/settings")
		return
	}

	// Validate
	if cleanupDays < 0 {
		cleanupDays = 0 // 0 = disabled
	}

	// Save to database
	var saveErr error
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
	if err := h.db.SetSetting("display.date_format", dateFormat); err != nil {
		saveErr = err
	}
	if err := h.db.SetSetting("display.time_format", timeFormat); err != nil {
		saveErr = err
	}

	if saveErr != nil {
		h.flashErr(w, "Failed to save some settings")
		h.redirect(w, r, "/settings")
		return
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

// ClearScanHistory handles clearing all scan history
func (h *Handlers) ClearScanHistory(w http.ResponseWriter, r *http.Request) {
	count, err := h.db.ClearScanHistory()
	if err != nil {
		h.flashErr(w, "Failed to clear scan history")
		h.redirect(w, r, "/settings")
		return
	}

	h.flash(w, "Cleared "+strconv.FormatInt(count, 10)+" scan history records")
	h.redirect(w, r, "/settings")
}

// DatabaseOptimize runs SQLite's PRAGMA optimize via the database manager.
func (h *Handlers) DatabaseOptimize(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Optimize(); err != nil {
		log.Error().Err(err).Msg("Failed to optimize database")
		h.flashErr(w, "Failed to optimize database")
		h.redirect(w, r, "/settings")
		return
	}

	h.flash(w, "Database optimized")
	h.redirect(w, r, "/settings")
}

// DatabaseVacuum rebuilds the database file to reclaim unused space.
func (h *Handlers) DatabaseVacuum(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Vacuum(); err != nil {
		log.Error().Err(err).Msg("Failed to vacuum database")
		h.flashErr(w, "Failed to vacuum database")
		h.redirect(w, r, "/settings")
		return
	}

	h.flash(w, "Database vacuum completed")
	h.redirect(w, r, "/settings")
}

// RegenerateTriggerKey regenerates the per-install trigger key and re-encrypts trigger passwords.
func (h *Handlers) RegenerateTriggerKey(w http.ResponseWriter, r *http.Request) {
	migrated, failed, err := auth.RegenerateTriggerPasswordKey(h.db)
	if err != nil {
		log.Error().Err(err).Msg("Failed to regenerate trigger key")
		h.flashErr(w, "Failed to regenerate trigger key")
		h.redirect(w, r, "/settings")
		return
	}

	msg := fmt.Sprintf("Regenerated trigger key and re-encrypted %d trigger passwords", migrated)
	if failed > 0 {
		msg += " (some failed: " + strconv.Itoa(failed) + ")"
	}
	h.flash(w, msg)
	h.redirect(w, r, "/settings")
}

// SettingsAboutPage renders the about settings page
func (h *Handlers) SettingsAboutPage(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, "settings.html", map[string]any{
		"Tab": "about",
	})
}

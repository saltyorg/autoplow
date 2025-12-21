package handlers

import (
	"net/http"
	"strconv"
)

// GeneralSettings holds the general configuration for display
type GeneralSettings struct {
	MaxRetries        int
	CleanupDays       int
	ScanningEnabled   bool
	UploadsEnabled    bool
	UseBinaryUnits    bool
	UseBitsForBitrate bool
}

// SettingsPage renders the general settings page
func (h *Handlers) SettingsPage(w http.ResponseWriter, r *http.Request) {
	settings := GeneralSettings{
		MaxRetries:        3,
		CleanupDays:       7,
		ScanningEnabled:   true, // Default to enabled
		UploadsEnabled:    true, // Default to enabled
		UseBinaryUnits:    true, // Default to binary (MiB/s)
		UseBitsForBitrate: true, // Default to bits (Mbps) for streaming bitrates
	}

	// Load from database
	if val, _ := h.db.GetSetting("processor.max_retries"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			settings.MaxRetries = v
		}
	}
	if val, _ := h.db.GetSetting("processor.cleanup_days"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			settings.CleanupDays = v
		}
	}
	if val, _ := h.db.GetSetting("scanning.enabled"); val != "" {
		settings.ScanningEnabled = val != "false" // Default true unless explicitly false
	}
	if val, _ := h.db.GetSetting("uploads.enabled"); val != "" {
		settings.UploadsEnabled = val != "false" // Default true unless explicitly false
	}
	if val, _ := h.db.GetSetting("display.use_binary_units"); val != "" {
		settings.UseBinaryUnits = val != "false" // Default true unless explicitly false
	}
	if val, _ := h.db.GetSetting("display.use_bits_for_bitrate"); val != "" {
		settings.UseBitsForBitrate = val != "false" // Default true unless explicitly false
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

	// Parse form values
	maxRetries, _ := strconv.Atoi(r.FormValue("max_retries"))
	cleanupDays, _ := strconv.Atoi(r.FormValue("cleanup_days"))
	scanningEnabled := r.FormValue("scanning_enabled") == "on"
	uploadsEnabled := r.FormValue("uploads_enabled") == "on"
	useBinaryUnits := r.FormValue("use_binary_units") == "on"
	useBitsForBitrate := r.FormValue("use_bits_for_bitrate") == "on"

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
	h.db.SetSetting("processor.max_retries", strconv.Itoa(maxRetries))
	h.db.SetSetting("processor.cleanup_days", strconv.Itoa(cleanupDays))
	h.db.SetSetting("scanning.enabled", strconv.FormatBool(scanningEnabled))
	h.db.SetSetting("uploads.enabled", strconv.FormatBool(uploadsEnabled))
	h.db.SetSetting("display.use_binary_units", strconv.FormatBool(useBinaryUnits))
	h.db.SetSetting("display.use_bits_for_bitrate", strconv.FormatBool(useBitsForBitrate))

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

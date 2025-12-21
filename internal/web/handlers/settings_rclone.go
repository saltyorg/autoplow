package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/saltyorg/autoplow/internal/rclone"
)

// generateRandomString generates a random hex string of the specified byte length
func generateRandomString(byteLen int) string {
	b := make([]byte, byteLen)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// LoadRcloneConfigFromDB loads rclone configuration from database settings
func LoadRcloneConfigFromDB(db interface {
	GetSetting(key string) (string, error)
	SetSetting(key, value string) error
}) rclone.ManagerConfig {
	config := rclone.DefaultManagerConfig()

	// Load managed mode setting (default true for backwards compatibility)
	if val, _ := db.GetSetting("rclone.managed"); val != "" {
		config.Managed = val == "true"
	}

	if val, _ := db.GetSetting("rclone.binary_path"); val != "" {
		val = strings.Trim(val, "\"")
		if val != "" {
			config.BinaryPath = val
		}
	}
	if val, _ := db.GetSetting("rclone.config_path"); val != "" {
		val = strings.Trim(val, "\"")
		if val != "" {
			config.ConfigPath = val
		}
	}
	if val, _ := db.GetSetting("rclone.rcd_address"); val != "" {
		val = strings.Trim(val, "\"")
		if val != "" {
			config.Address = val
		}
	}

	// Get credentials - ensure they exist (generate if needed)
	username, _ := db.GetSetting("rclone.username")
	password, _ := db.GetSetting("rclone.password")
	if username == "" && password == "" {
		// Generate random credentials for security
		username = generateRandomString(16)
		password = generateRandomString(32)
		if err := db.SetSetting("rclone.username", username); err != nil {
			log.Error().Err(err).Msg("Failed to save rclone username")
		}
		if err := db.SetSetting("rclone.password", password); err != nil {
			log.Error().Err(err).Msg("Failed to save rclone password")
		}
		log.Info().Msg("Generated random rclone RCD credentials for security")
	}
	config.Username = username
	config.Password = password

	if val, _ := db.GetSetting("rclone.auto_start"); val != "" {
		config.AutoStart = val == "true"
	}
	if val, _ := db.GetSetting("rclone.restart_on_fail"); val != "" {
		config.RestartOnFail = val == "true"
	}
	if val, _ := db.GetSetting("rclone.max_restarts"); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			config.MaxRestarts = n
		}
	}

	return config
}

// loadRcloneConfig loads rclone configuration from database settings
func (h *Handlers) loadRcloneConfig() rclone.ManagerConfig {
	return LoadRcloneConfigFromDB(h.db)
}

// formatUptime formats a duration as a human-readable string with whole units
func formatUptime(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	var parts []string

	years := int(d.Hours() / 24 / 365)
	if years > 0 {
		parts = append(parts, strconv.Itoa(years)+"y")
		d -= time.Duration(years) * 365 * 24 * time.Hour
	}

	months := int(d.Hours() / 24 / 30)
	if months > 0 {
		parts = append(parts, strconv.Itoa(months)+"mo")
		d -= time.Duration(months) * 30 * 24 * time.Hour
	}

	weeks := int(d.Hours() / 24 / 7)
	if weeks > 0 {
		parts = append(parts, strconv.Itoa(weeks)+"w")
		d -= time.Duration(weeks) * 7 * 24 * time.Hour
	}

	days := int(d.Hours() / 24)
	if days > 0 {
		parts = append(parts, strconv.Itoa(days)+"d")
		d -= time.Duration(days) * 24 * time.Hour
	}

	hours := int(d.Hours())
	if hours > 0 {
		parts = append(parts, strconv.Itoa(hours)+"h")
		d -= time.Duration(hours) * time.Hour
	}

	minutes := int(d.Minutes())
	if minutes > 0 {
		parts = append(parts, strconv.Itoa(minutes)+"m")
		d -= time.Duration(minutes) * time.Minute
	}

	seconds := int(d.Seconds())
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, strconv.Itoa(seconds)+"s")
	}

	return strings.Join(parts, " ")
}

// SettingsRclonePage renders rclone settings page
func (h *Handlers) SettingsRclonePage(w http.ResponseWriter, r *http.Request) {
	var rcloneStatus map[string]any
	var rcloneRunning bool
	var globalOptions map[string]any

	if h.rcloneMgr != nil {
		status := h.rcloneMgr.Status()
		rcloneRunning = status.Running
		rcloneStatus = map[string]any{
			"running":       status.Running,
			"pid":           status.PID,
			"address":       status.Address,
			"restart_count": status.RestartCount,
			"uptime":        formatUptime(status.Uptime),
			"version":       strings.TrimPrefix(status.Version, "v"),
		}

		// Get global options for transfer settings if rclone is running
		if rcloneRunning {
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			defer cancel()
			globalOptions, _ = h.rcloneMgr.Client().GetOptions(ctx)
		}
	}

	// Load config from database
	config := h.loadRcloneConfig()

	h.render(w, r, "settings.html", map[string]any{
		"Tab":           "rclone",
		"RcloneStatus":  rcloneStatus,
		"RcloneRunning": rcloneRunning,
		"GlobalOptions": globalOptions,
		"RcloneConfig": map[string]any{
			"Managed":       config.Managed,
			"BinaryPath":    config.BinaryPath,
			"ConfigPath":    config.ConfigPath,
			"Address":       config.Address,
			"Username":      config.Username,
			"Password":      config.Password,
			"AutoStart":     config.AutoStart,
			"RestartOnFail": config.RestartOnFail,
			"MaxRestarts":   config.MaxRestarts,
		},
	})
}

// SettingsRcloneUpdate handles rclone settings update
func (h *Handlers) SettingsRcloneUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	// Get form values
	managed := r.FormValue("managed") == "on"
	username := r.FormValue("username")
	password := r.FormValue("password")

	// If user clears both username and password, generate new random credentials
	if username == "" && password == "" {
		username = generateRandomString(16)
		password = generateRandomString(32)
		log.Info().Msg("Generated new random rclone RCD credentials")
	}

	// For managed mode, we don't need the address field - it auto-finds an available port
	// For unmanaged mode, we need the address of the external RCD instance
	address := r.FormValue("address")
	if managed {
		address = "" // Clear address for managed mode, will use default port range
	} else if address == "" {
		address = "127.0.0.1:5572" // Default for unmanaged
	}

	// Save rclone settings to database
	settings := map[string]string{
		"rclone.managed":         strconv.FormatBool(managed),
		"rclone.binary_path":     r.FormValue("binary_path"),
		"rclone.config_path":     r.FormValue("config_path"),
		"rclone.rcd_address":     address,
		"rclone.username":        username,
		"rclone.password":        password,
		"rclone.auto_start":      strconv.FormatBool(r.FormValue("auto_start") == "on"),
		"rclone.restart_on_fail": strconv.FormatBool(r.FormValue("restart_on_fail") == "on"),
		"rclone.max_restarts":    r.FormValue("max_restarts"),
	}

	for key, value := range settings {
		if err := h.db.SetSetting(key, value); err != nil {
			log.Error().Err(err).Str("key", key).Msg("Failed to save rclone setting")
		}
	}

	log.Info().Bool("managed", managed).Msg("Rclone settings updated")
	h.flash(w, "Rclone settings updated. Restart rclone for changes to take effect.")
	h.redirect(w, r, "/settings/rclone")
}

// SettingsRcloneTest tests rclone RCD connection
func (h *Handlers) SettingsRcloneTest(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Rclone manager not initialized",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if !h.rcloneMgr.Healthy(ctx) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Rclone RCD is not responding",
		})
		return
	}

	// Get version info
	version, err := h.rcloneMgr.Client().Version(ctx)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Failed to get rclone version: " + err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "Connected to rclone " + version.Version,
		"version": version.Version,
	})
}

// SettingsRcloneStart starts rclone RCD
func (h *Handlers) SettingsRcloneStart(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil {
		h.flashErr(w, "Rclone manager not initialized")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	if h.rcloneMgr.IsRunning() {
		h.flash(w, "Rclone is already running")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	// Load fresh config from database before starting
	config := h.loadRcloneConfig()
	h.rcloneMgr.UpdateConfig(config)

	if err := h.rcloneMgr.Start(); err != nil {
		log.Error().Err(err).Msg("Failed to start rclone")
		h.flashErr(w, "Failed to start rclone: "+err.Error())
		h.redirect(w, r, "/settings/rclone")
		return
	}

	log.Info().Msg("Rclone RCD started via settings")
	h.flash(w, "Rclone started successfully")
	h.redirect(w, r, "/settings/rclone")
}

// SettingsRcloneStop stops rclone RCD
func (h *Handlers) SettingsRcloneStop(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil {
		h.flashErr(w, "Rclone manager not initialized")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	if !h.rcloneMgr.IsRunning() {
		h.flash(w, "Rclone is not running")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	if err := h.rcloneMgr.Stop(); err != nil {
		log.Error().Err(err).Msg("Failed to stop rclone")
		h.flashErr(w, "Failed to stop rclone: "+err.Error())
		h.redirect(w, r, "/settings/rclone")
		return
	}

	log.Info().Msg("Rclone RCD stopped via settings")
	h.flash(w, "Rclone stopped successfully")
	h.redirect(w, r, "/settings/rclone")
}

// SettingsRcloneRestart restarts rclone RCD
func (h *Handlers) SettingsRcloneRestart(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil {
		h.flashErr(w, "Rclone manager not initialized")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	// Load fresh config from database before restarting
	config := h.loadRcloneConfig()
	h.rcloneMgr.UpdateConfig(config)

	if err := h.rcloneMgr.Restart(); err != nil {
		log.Error().Err(err).Msg("Failed to restart rclone")
		h.flashErr(w, "Failed to restart rclone: "+err.Error())
		h.redirect(w, r, "/settings/rclone")
		return
	}

	log.Info().Msg("Rclone RCD restarted via settings")
	h.flash(w, "Rclone restarted successfully")
	h.redirect(w, r, "/settings/rclone")
}

// SettingsRcloneOptionsUpdate handles updating rclone transfer options from settings page
func (h *Handlers) SettingsRcloneOptionsUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/rclone")
		return
	}

	// Build the options to set - structure: {"main": {"Transfers": 4, ...}}
	mainOpts := make(map[string]any)

	// Parse and save transfers
	if v := r.FormValue("transfers"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Transfers"] = i
			if err := h.db.SetSetting("rclone.transfers", v); err != nil {
				log.Error().Err(err).Msg("Failed to save rclone.transfers setting")
			}
		}
	}

	// Parse and save checkers
	if v := r.FormValue("checkers"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Checkers"] = i
			if err := h.db.SetSetting("rclone.checkers", v); err != nil {
				log.Error().Err(err).Msg("Failed to save rclone.checkers setting")
			}
		}
	}

	// Parse and save buffer size
	if v := r.FormValue("buffer_size"); v != "" {
		mainOpts["BufferSize"] = ParseSizeString(v)
		if err := h.db.SetSetting("rclone.buffer_size", v); err != nil {
			log.Error().Err(err).Msg("Failed to save rclone.buffer_size setting")
		}
	}

	// Apply to running rclone if available
	if h.rcloneMgr != nil && h.rcloneMgr.IsRunning() && len(mainOpts) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		options := map[string]any{
			"main": mainOpts,
		}

		if err := h.rcloneMgr.Client().SetOptions(ctx, options); err != nil {
			log.Error().Err(err).Interface("options", mainOpts).Msg("Failed to apply rclone options")
			h.flashErr(w, "Settings saved but failed to apply to running rclone: "+err.Error())
			h.redirect(w, r, "/settings/rclone")
			return
		}
		log.Info().Interface("options", mainOpts).Msg("Applied rclone transfer options")
	}

	h.flash(w, "Transfer options saved successfully")
	h.redirect(w, r, "/settings/rclone")
}

// SettingsRcloneStatus returns rclone RCD status (for HTMX polling)
func (h *Handlers) SettingsRcloneStatus(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<span class="inline-flex items-center rounded-md bg-gray-100 dark:bg-gray-700 px-2.5 py-0.5 text-xs font-medium text-gray-800 dark:text-gray-300">Not initialized</span>`))
		return
	}

	status := h.rcloneMgr.Status()

	var statusClass, statusText string
	if status.Running {
		statusClass = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
		statusText = "Running"
	} else {
		statusClass = "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200"
		statusText = "Stopped"
	}

	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte(`<span class="inline-flex items-center rounded-md px-2.5 py-0.5 text-xs font-medium ` + statusClass + `">` + statusText + `</span>`))
}

// SettingsRcloneStartStopBtn returns context-aware start/stop button HTML
func (h *Handlers) SettingsRcloneStartStopBtn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	if h.rcloneMgr == nil {
		_, _ = w.Write([]byte(`<button type="button" disabled class="inline-flex items-center rounded-md bg-gray-400 px-3 py-2 text-sm font-semibold text-white shadow-sm cursor-not-allowed"><svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>Not Initialized</button>`))
		return
	}

	status := h.rcloneMgr.Status()

	if status.Running {
		// Show Stop button - use hx-post, page redirects after action
		_, _ = w.Write([]byte(`<button hx-post="/settings/rclone/stop" class="inline-flex items-center rounded-md bg-red-600 px-3 py-2 text-sm font-semibold text-white shadow-sm hover:bg-red-500"><svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 10a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1v-4z"/></svg>Stop</button>`))
	} else {
		// Show Start button - use hx-post, page redirects after action
		_, _ = w.Write([]byte(`<button hx-post="/settings/rclone/start" class="inline-flex items-center rounded-md bg-green-600 px-3 py-2 text-sm font-semibold text-white shadow-sm hover:bg-green-500"><svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>Start</button>`))
	}
}

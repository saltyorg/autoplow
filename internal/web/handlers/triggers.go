package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
)

// TriggersPage renders the triggers list page
func (h *Handlers) TriggersPage(w http.ResponseWriter, r *http.Request) {
	triggers, err := h.db.ListTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list triggers")
		h.flashErr(w, "Failed to load triggers")
		h.redirect(w, r, "/")
		return
	}

	// Build a map of decrypted passwords for basic auth triggers
	passwords := make(map[int64]string)
	for _, t := range triggers {
		if t.AuthType == database.AuthTypeBasic && t.Password != "" {
			decrypted, err := auth.DecryptTriggerPassword(t.Password)
			if err != nil {
				log.Error().Err(err).Int64("id", t.ID).Msg("Failed to decrypt trigger password")
				continue
			}
			passwords[t.ID] = decrypted
		}
	}

	// Get base URL for webhook URLs
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	baseURL := scheme + "://" + r.Host

	h.render(w, r, "triggers.html", map[string]any{
		"Triggers":  triggers,
		"BaseURL":   baseURL,
		"Passwords": passwords,
	})
}

// TriggerNew renders the new trigger form
func (h *Handlers) TriggerNew(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	baseURL := scheme + "://" + r.Host

	h.render(w, r, "triggers.html", map[string]any{
		"IsNew":   true,
		"BaseURL": baseURL,
	})
}

// TriggerCreate handles trigger creation
func (h *Handlers) TriggerCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/triggers/new")
		return
	}

	name := r.FormValue("name")
	triggerType := r.FormValue("type")
	priorityStr := r.FormValue("priority")
	enabled := r.FormValue("enabled") == "on"

	if name == "" {
		h.flashErr(w, "Name is required")
		h.redirect(w, r, "/triggers/new")
		return
	}

	priority, _ := strconv.Atoi(priorityStr)

	trigger := &database.Trigger{
		Name:     name,
		Type:     database.TriggerType(triggerType),
		Priority: priority,
		Enabled:  enabled,
		Config:   database.TriggerConfig{},
	}

	// For inotify triggers, parse watch paths and debounce
	if trigger.Type == database.TriggerTypeInotify {
		watchPathsRaw := r.Form["watch_paths[]"]

		// Filter out empty paths
		var watchPaths []string
		for _, path := range watchPathsRaw {
			path = strings.TrimSpace(path)
			if path != "" {
				watchPaths = append(watchPaths, path)
			}
		}

		if len(watchPaths) == 0 {
			h.flashErr(w, "At least one watch path is required")
			h.redirect(w, r, "/triggers/new")
			return
		}

		debounceStr := r.FormValue("debounce_seconds")
		debounce, _ := strconv.Atoi(debounceStr)
		if debounce < 1 {
			debounce = 5
		}

		trigger.Config.WatchPaths = watchPaths
		trigger.Config.DebounceSeconds = debounce
	}

	// For polling triggers, parse watch paths, poll interval, and queue existing setting
	if trigger.Type == database.TriggerTypePolling {
		watchPathsRaw := r.Form["watch_paths[]"]

		// Filter out empty paths
		var watchPaths []string
		for _, path := range watchPathsRaw {
			path = strings.TrimSpace(path)
			if path != "" {
				watchPaths = append(watchPaths, path)
			}
		}

		if len(watchPaths) == 0 {
			h.flashErr(w, "At least one watch path is required")
			h.redirect(w, r, "/triggers/new")
			return
		}

		pollIntervalStr := r.FormValue("poll_interval_seconds")
		pollInterval, _ := strconv.Atoi(pollIntervalStr)
		if pollInterval < 10 {
			pollInterval = 60 // Default to 60 seconds
		}

		trigger.Config.WatchPaths = watchPaths
		trigger.Config.PollIntervalSeconds = pollInterval
		trigger.Config.QueueExistingOnStart = r.FormValue("queue_existing_on_start") == "on"
	}

	// Parse path rewrites (for webhook trigger types only, not local triggers)
	if trigger.Type != database.TriggerTypeInotify && trigger.Type != database.TriggerTypePolling {
		rewrites, err := parsePathRewrites(r)
		if err != nil {
			h.flashErr(w, "Invalid path rewrite regex: "+err.Error())
			h.redirect(w, r, "/triggers/new")
			return
		}
		trigger.Config.PathRewrites = rewrites
	}

	// Parse path filters (include/exclude)
	trigger.Config.IncludePaths = parsePathList(r, "include_paths[]")
	trigger.Config.ExcludePaths = parsePathList(r, "exclude_paths[]")
	trigger.Config.FilterAfterRewrite = r.FormValue("filter_after_rewrite") == "on"

	// Parse exclude extensions
	trigger.Config.ExcludeExtensions = parsePathList(r, "exclude_extensions[]")

	// Parse advanced filters (regex patterns)
	advIncludePatterns := parsePathList(r, "advanced_include_patterns[]")
	advExcludePatterns := parsePathList(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		trigger.Config.AdvancedFilters = &database.AdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := trigger.Config.AdvancedFilters.Compile(); err != nil {
			h.flashErr(w, "Invalid regex pattern: "+err.Error())
			h.redirect(w, r, "/triggers/new")
			return
		}
	}

	// Parse filesystem settings
	trigger.Config.FilesystemType = database.FilesystemType(r.FormValue("filesystem_type"))
	if trigger.Config.FilesystemType == "" {
		trigger.Config.FilesystemType = database.FilesystemTypeRemote // default
	}

	minAgeSeconds, _ := strconv.Atoi(r.FormValue("minimum_age_seconds"))
	stabilityCheckSeconds, _ := strconv.Atoi(r.FormValue("stability_check_seconds"))

	// Validate and set minimum age based on filesystem type
	if trigger.Config.FilesystemType == database.FilesystemTypeRemote {
		if minAgeSeconds < 60 {
			minAgeSeconds = 60
		}
	} else {
		if minAgeSeconds < 1 {
			minAgeSeconds = 60 // default for local
		}
		if stabilityCheckSeconds < 1 {
			stabilityCheckSeconds = 10 // default
		}
		trigger.Config.StabilityCheckSeconds = stabilityCheckSeconds
	}
	trigger.Config.MinimumAgeSeconds = minAgeSeconds

	// Set auth type and credentials for webhook triggers (not local triggers)
	if trigger.Type != database.TriggerTypeInotify && trigger.Type != database.TriggerTypePolling {
		authTypeStr := r.FormValue("auth_type")
		if authTypeStr == "basic" {
			trigger.AuthType = database.AuthTypeBasic
			trigger.Username = strings.TrimSpace(r.FormValue("trigger_username"))
			password := r.FormValue("trigger_password")

			if trigger.Username == "" || password == "" {
				h.flashErr(w, "Username and password are required for basic auth")
				h.redirect(w, r, "/triggers/new")
				return
			}

			// Encrypt the password (this is for webhook auth, not user login)
			encryptedPassword, err := auth.EncryptTriggerPassword(password)
			if err != nil {
				log.Error().Err(err).Msg("Failed to encrypt password")
				h.flashErr(w, "Failed to process password")
				h.redirect(w, r, "/triggers/new")
				return
			}
			trigger.Password = encryptedPassword
		} else {
			// Default to API key auth
			trigger.AuthType = database.AuthTypeAPIKey
			apiKey, err := h.apiKeyService.GenerateKey()
			if err != nil {
				log.Error().Err(err).Msg("Failed to generate API key")
				h.flashErr(w, "Failed to generate API key")
				h.redirect(w, r, "/triggers/new")
				return
			}
			trigger.APIKey = apiKey
		}
	}

	if err := h.db.CreateTrigger(trigger); err != nil {
		log.Error().Err(err).Msg("Failed to create trigger")
		h.flashErr(w, "Failed to create trigger")
		h.redirect(w, r, "/triggers/new")
		return
	}

	// Notify inotify manager of new trigger
	if h.inotifyMgr != nil && trigger.Type == database.TriggerTypeInotify {
		if err := h.inotifyMgr.ReloadTrigger(trigger.ID); err != nil {
			log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to reload inotify trigger")
		}
	}

	// Notify polling manager of new trigger
	if h.pollingMgr != nil && trigger.Type == database.TriggerTypePolling {
		if err := h.pollingMgr.ReloadTrigger(trigger.ID); err != nil {
			log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to reload polling trigger")
		}
	}

	log.Info().Str("name", trigger.Name).Str("type", string(trigger.Type)).Msg("Trigger created")
	h.flash(w, "Trigger created successfully")
	h.redirect(w, r, "/triggers")
}

// TriggerEdit renders the trigger edit form
func (h *Handlers) TriggerEdit(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid trigger ID")
		h.redirect(w, r, "/triggers")
		return
	}

	trigger, err := h.db.GetTrigger(id)
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to get trigger")
		h.flashErr(w, "Failed to load trigger")
		h.redirect(w, r, "/triggers")
		return
	}
	if trigger == nil {
		h.flashErr(w, "Trigger not found")
		h.redirect(w, r, "/triggers")
		return
	}

	// Decrypt password for display if using basic auth
	var decryptedPassword string
	if trigger.AuthType == database.AuthTypeBasic && trigger.Password != "" {
		decryptedPassword, err = auth.DecryptTriggerPassword(trigger.Password)
		if err != nil {
			log.Error().Err(err).Int64("id", id).Msg("Failed to decrypt trigger password")
			// Don't fail, just leave password empty
		}
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	baseURL := scheme + "://" + r.Host

	h.render(w, r, "triggers.html", map[string]any{
		"Trigger":           trigger,
		"BaseURL":           baseURL,
		"DecryptedPassword": decryptedPassword,
	})
}

// TriggerUpdate handles trigger update
func (h *Handlers) TriggerUpdate(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid trigger ID")
		h.redirect(w, r, "/triggers")
		return
	}

	trigger, err := h.db.GetTrigger(id)
	if err != nil || trigger == nil {
		h.flashErr(w, "Trigger not found")
		h.redirect(w, r, "/triggers")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/triggers/"+idStr)
		return
	}

	name := r.FormValue("name")
	priorityStr := r.FormValue("priority")
	enabled := r.FormValue("enabled") == "on"

	if name == "" {
		h.flashErr(w, "Name is required")
		h.redirect(w, r, "/triggers/"+idStr)
		return
	}

	priority, _ := strconv.Atoi(priorityStr)

	trigger.Name = name
	trigger.Priority = priority
	trigger.Enabled = enabled

	// For inotify triggers, update watch paths and debounce
	if trigger.Type == database.TriggerTypeInotify {
		watchPathsRaw := r.Form["watch_paths[]"]

		// Filter out empty paths
		var watchPaths []string
		for _, path := range watchPathsRaw {
			path = strings.TrimSpace(path)
			if path != "" {
				watchPaths = append(watchPaths, path)
			}
		}

		if len(watchPaths) == 0 {
			h.flashErr(w, "At least one watch path is required")
			h.redirect(w, r, "/triggers/"+idStr)
			return
		}

		debounceStr := r.FormValue("debounce_seconds")
		debounce, _ := strconv.Atoi(debounceStr)
		if debounce < 1 {
			debounce = 5
		}

		trigger.Config.WatchPaths = watchPaths
		trigger.Config.DebounceSeconds = debounce
	}

	// For polling triggers, update watch paths, poll interval, and queue existing setting
	if trigger.Type == database.TriggerTypePolling {
		watchPathsRaw := r.Form["watch_paths[]"]

		// Filter out empty paths
		var watchPaths []string
		for _, path := range watchPathsRaw {
			path = strings.TrimSpace(path)
			if path != "" {
				watchPaths = append(watchPaths, path)
			}
		}

		if len(watchPaths) == 0 {
			h.flashErr(w, "At least one watch path is required")
			h.redirect(w, r, "/triggers/"+idStr)
			return
		}

		pollIntervalStr := r.FormValue("poll_interval_seconds")
		pollInterval, _ := strconv.Atoi(pollIntervalStr)
		if pollInterval < 10 {
			pollInterval = 60 // Default to 60 seconds
		}

		trigger.Config.WatchPaths = watchPaths
		trigger.Config.PollIntervalSeconds = pollInterval
		trigger.Config.QueueExistingOnStart = r.FormValue("queue_existing_on_start") == "on"
	}

	// Parse path rewrites (for webhook trigger types only, not local triggers)
	if trigger.Type != database.TriggerTypeInotify && trigger.Type != database.TriggerTypePolling {
		rewrites, err := parsePathRewrites(r)
		if err != nil {
			h.flashErr(w, "Invalid path rewrite regex: "+err.Error())
			h.redirect(w, r, "/triggers/"+idStr)
			return
		}
		trigger.Config.PathRewrites = rewrites
	} else {
		// Clear path rewrites for local triggers
		trigger.Config.PathRewrites = nil
	}

	// Parse path filters (include/exclude)
	trigger.Config.IncludePaths = parsePathList(r, "include_paths[]")
	trigger.Config.ExcludePaths = parsePathList(r, "exclude_paths[]")
	trigger.Config.FilterAfterRewrite = r.FormValue("filter_after_rewrite") == "on"

	// Parse exclude extensions
	trigger.Config.ExcludeExtensions = parsePathList(r, "exclude_extensions[]")

	// Parse advanced filters (regex patterns)
	advIncludePatterns := parsePathList(r, "advanced_include_patterns[]")
	advExcludePatterns := parsePathList(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		trigger.Config.AdvancedFilters = &database.AdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := trigger.Config.AdvancedFilters.Compile(); err != nil {
			h.flashErr(w, "Invalid regex pattern: "+err.Error())
			h.redirect(w, r, "/triggers/"+idStr)
			return
		}
	} else {
		trigger.Config.AdvancedFilters = nil
	}

	// Parse filesystem settings
	trigger.Config.FilesystemType = database.FilesystemType(r.FormValue("filesystem_type"))
	if trigger.Config.FilesystemType == "" {
		trigger.Config.FilesystemType = database.FilesystemTypeRemote // default
	}

	minAgeSeconds, _ := strconv.Atoi(r.FormValue("minimum_age_seconds"))
	stabilityCheckSeconds, _ := strconv.Atoi(r.FormValue("stability_check_seconds"))

	// Validate and set minimum age based on filesystem type
	if trigger.Config.FilesystemType == database.FilesystemTypeRemote {
		if minAgeSeconds < 60 {
			minAgeSeconds = 60
		}
		trigger.Config.StabilityCheckSeconds = 0 // not used for remote
	} else {
		if minAgeSeconds < 1 {
			minAgeSeconds = 60 // default for local
		}
		if stabilityCheckSeconds < 1 {
			stabilityCheckSeconds = 10 // default
		}
		trigger.Config.StabilityCheckSeconds = stabilityCheckSeconds
	}
	trigger.Config.MinimumAgeSeconds = minAgeSeconds

	if err := h.db.UpdateTrigger(trigger); err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to update trigger")
		h.flashErr(w, "Failed to update trigger")
		h.redirect(w, r, "/triggers/"+idStr)
		return
	}

	// Notify inotify watcher of trigger update
	if h.inotifyMgr != nil && trigger.Type == database.TriggerTypeInotify {
		if err := h.inotifyMgr.ReloadTrigger(trigger.ID); err != nil {
			log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to reload inotify trigger")
		}
	}

	// Notify polling manager of trigger update
	if h.pollingMgr != nil && trigger.Type == database.TriggerTypePolling {
		if err := h.pollingMgr.ReloadTrigger(trigger.ID); err != nil {
			log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to reload polling trigger")
		}
	}

	log.Info().Int64("id", id).Str("name", trigger.Name).Msg("Trigger updated")
	h.flash(w, "Trigger updated successfully")
	h.redirect(w, r, "/triggers")
}

// TriggerDelete handles trigger deletion
func (h *Handlers) TriggerDelete(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid trigger ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteTrigger(id); err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to delete trigger")
		h.jsonError(w, "Failed to delete trigger", http.StatusInternalServerError)
		return
	}

	// Notify inotify watcher of trigger deletion
	if h.inotifyMgr != nil {
		h.inotifyMgr.RemoveTrigger(id)
	}

	// Notify polling manager of trigger deletion
	if h.pollingMgr != nil {
		h.pollingMgr.RemoveTrigger(id)
	}

	log.Info().Int64("id", id).Msg("Trigger deleted")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Trigger deleted successfully")
	h.redirect(w, r, "/triggers")
}

// TriggerRegenerateKey regenerates a trigger's API key
func (h *Handlers) TriggerRegenerateKey(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid trigger ID", http.StatusBadRequest)
		return
	}

	trigger, err := h.db.GetTrigger(id)
	if err != nil || trigger == nil {
		h.jsonError(w, "Trigger not found", http.StatusNotFound)
		return
	}

	if trigger.Type == database.TriggerTypeInotify || trigger.Type == database.TriggerTypePolling {
		h.jsonError(w, "Local triggers do not have API keys", http.StatusBadRequest)
		return
	}

	if trigger.AuthType == database.AuthTypeBasic {
		h.jsonError(w, "This trigger uses basic auth, not API key", http.StatusBadRequest)
		return
	}

	newKey, err := h.apiKeyService.GenerateKey()
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to generate new API key")
		h.jsonError(w, "Failed to generate API key", http.StatusInternalServerError)
		return
	}

	if err := h.db.UpdateTriggerAPIKey(id, newKey); err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to update API key")
		h.jsonError(w, "Failed to update API key", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("id", id).Msg("API key regenerated")
	h.jsonSuccess(w, "API key regenerated")
}

// TriggerUpdatePassword updates a trigger's password (for basic auth triggers)
func (h *Handlers) TriggerUpdatePassword(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid trigger ID", http.StatusBadRequest)
		return
	}

	trigger, err := h.db.GetTrigger(id)
	if err != nil || trigger == nil {
		h.jsonError(w, "Trigger not found", http.StatusNotFound)
		return
	}

	if trigger.Type == database.TriggerTypeInotify || trigger.Type == database.TriggerTypePolling {
		h.jsonError(w, "Local triggers do not have authentication", http.StatusBadRequest)
		return
	}

	if trigger.AuthType != database.AuthTypeBasic {
		h.jsonError(w, "This trigger uses API key auth, not basic auth", http.StatusBadRequest)
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		h.jsonError(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	newPassword := r.FormValue("new_password")
	if newPassword == "" {
		h.jsonError(w, "New password is required", http.StatusBadRequest)
		return
	}

	// Encrypt the new password
	encryptedPassword, err := auth.EncryptTriggerPassword(newPassword)
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to encrypt password")
		h.jsonError(w, "Failed to process password", http.StatusInternalServerError)
		return
	}

	// Update the password
	if err := h.db.UpdateTriggerAuth(id, database.AuthTypeBasic, "", trigger.Username, encryptedPassword); err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to update password")
		h.jsonError(w, "Failed to update password", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("id", id).Msg("Trigger password updated")
	h.jsonSuccess(w, "Password updated")
}

// parsePathList extracts a list of paths from form data by field name
func parsePathList(r *http.Request, fieldName string) []string {
	rawPaths := r.Form[fieldName]
	var paths []string
	for _, path := range rawPaths {
		path = strings.TrimSpace(path)
		if path != "" {
			paths = append(paths, path)
		}
	}
	return paths
}

// parsePathRewrites extracts path rewrite rules from form data
// Returns the rewrites and an error if any regex pattern is invalid
// Note: All rewrites must be the same type (all simple OR all regex, not mixed)
func parsePathRewrites(r *http.Request) ([]database.PathRewrite, error) {
	fromPaths := r.Form["path_rewrites_from[]"]
	toPaths := r.Form["path_rewrites_to[]"]
	isRegexValues := r.Form["path_rewrites_is_regex[]"]

	var rewrites []database.PathRewrite
	var hasSimple, hasRegex bool

	for i := 0; i < len(fromPaths) && i < len(toPaths); i++ {
		from := strings.TrimSpace(fromPaths[i])
		to := strings.TrimSpace(toPaths[i])
		// Only add if both from and to are non-empty
		if from != "" && to != "" {
			isRegex := false
			if i < len(isRegexValues) && isRegexValues[i] == "true" {
				isRegex = true
			}

			// Track which types we've seen
			if isRegex {
				hasRegex = true
			} else {
				hasSimple = true
			}

			rewrite := database.PathRewrite{
				From:    from,
				To:      to,
				IsRegex: isRegex,
			}

			// Validate regex patterns
			if isRegex {
				if err := rewrite.CompileRegex(); err != nil {
					return nil, err
				}
			}

			rewrites = append(rewrites, rewrite)
		}
	}

	// Validate that we don't have both types
	if hasSimple && hasRegex {
		return nil, fmt.Errorf("cannot mix simple and regex path rewrites - use one type only")
	}

	return rewrites, nil
}

// ManualScan handles quick manual scan requests from the UI
func (h *Handlers) ManualScan(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Path string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		h.jsonError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	path := strings.TrimSpace(payload.Path)
	if path == "" {
		h.jsonError(w, "Path is required", http.StatusBadRequest)
		return
	}

	// Normalize the path
	path = filepath.Clean(path)

	// Queue the scan with high priority and no trigger association
	h.processor.QueueScan(processor.ScanRequest{
		Path:     path,
		Priority: 100, // High priority for manual scans
	})

	log.Info().Str("path", path).Msg("Manual scan queued from UI")

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "Scan queued for: " + path,
		"path":    path,
	})
}

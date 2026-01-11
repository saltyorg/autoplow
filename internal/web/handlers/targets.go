package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/targets"
)

// TargetsPage renders the targets list page
func (h *Handlers) TargetsPage(w http.ResponseWriter, r *http.Request) {
	targets, err := h.db.ListTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets")
		h.flashErr(w, "Failed to load targets")
		h.redirect(w, r, "/")
		return
	}

	// Group targets by type
	var plex, emby, jellyfin, autoplow, tdarr []*database.Target
	for _, t := range targets {
		switch t.Type {
		case database.TargetTypePlex:
			plex = append(plex, t)
		case database.TargetTypeEmby:
			emby = append(emby, t)
		case database.TargetTypeJellyfin:
			jellyfin = append(jellyfin, t)
		case database.TargetTypeAutoplow:
			autoplow = append(autoplow, t)
		case database.TargetTypeTdarr:
			tdarr = append(tdarr, t)
		}
	}

	h.render(w, r, "targets.html", map[string]any{
		"Targets":  targets,
		"Plex":     plex,
		"Emby":     emby,
		"Jellyfin": jellyfin,
		"Autoplow": autoplow,
		"Tdarr":    tdarr,
	})
}

// TargetNew renders the new target form
func (h *Handlers) TargetNew(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, "targets.html", map[string]any{
		"IsNew": true,
	})
}

// TargetCreate handles target creation
func (h *Handlers) TargetCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.jsonError(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	targetType := database.TargetType(r.FormValue("type"))
	if targetType != database.TargetTypePlex &&
		targetType != database.TargetTypeEmby &&
		targetType != database.TargetTypeJellyfin &&
		targetType != database.TargetTypeAutoplow &&
		targetType != database.TargetTypeTdarr {
		h.jsonError(w, "Invalid target type", http.StatusBadRequest)
		return
	}

	target := &database.Target{
		Name:    r.FormValue("name"),
		Type:    targetType,
		URL:     r.FormValue("url"),
		Token:   r.FormValue("token"),
		APIKey:  r.FormValue("api_key"),
		Enabled: r.FormValue("enabled") == "on",
	}

	// Parse scan delay (common to all target types)
	if delayStr := r.FormValue("scan_delay"); delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil && delay >= 0 {
			target.Config.ScanDelay = delay
		}
	}
	if targetType == database.TargetTypePlex {
		if limitStr := r.FormValue("plex_max_concurrent_scans"); limitStr != "" {
			if limit, err := strconv.Atoi(limitStr); err == nil && limit >= 0 {
				target.Config.PlexMaxConcurrentScans = limit
			}
		}
	}

	// Parse Emby/Jellyfin specific options
	if targetType == database.TargetTypeEmby || targetType == database.TargetTypeJellyfin {
		target.Config.RefreshMetadata = r.FormValue("refresh_metadata") == "on"
		if mode := r.FormValue("metadata_refresh_mode"); mode != "" {
			target.Config.MetadataRefreshMode = database.MetadataRefreshMode(mode)
		} else {
			target.Config.MetadataRefreshMode = database.MetadataRefreshModeFullRefresh
		}
	}

	// Parse Tdarr specific options
	if targetType == database.TargetTypeTdarr {
		target.Config.TdarrDBID = r.FormValue("tdarr_db_id")
	}

	// Parse session mode for media servers (Plex, Emby, Jellyfin)
	if targetType == database.TargetTypePlex ||
		targetType == database.TargetTypeEmby ||
		targetType == database.TargetTypeJellyfin {
		if mode := r.FormValue("session_mode"); mode != "" {
			target.Config.SessionMode = database.SessionMode(mode)
		} else {
			target.Config.SessionMode = database.SessionModeWebSocket // Default to WebSocket
		}
	}

	// Parse path mappings from form
	mappings, mappingErrors := parsePathMappings(r)
	if len(mappingErrors) > 0 {
		h.jsonError(w, strings.Join(mappingErrors, "; "), http.StatusBadRequest)
		return
	}
	target.Config.PathMappings = mappings

	// Parse exclude paths
	target.Config.ExcludePaths = parseExcludePaths(r)

	// Parse exclude triggers
	target.Config.ExcludeTriggers = parseExcludeTriggers(r)

	// Parse exclude extensions
	target.Config.ExcludeExtensions = parseExcludeExtensions(r)

	// Parse advanced filters (regex patterns)
	advIncludePatterns := parseAdvancedPatterns(r, "advanced_include_patterns[]")
	advExcludePatterns := parseAdvancedPatterns(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		target.Config.AdvancedFilters = &database.AdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := target.Config.AdvancedFilters.Compile(); err != nil {
			h.jsonError(w, "Invalid regex pattern: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if err := h.db.CreateTarget(target); err != nil {
		log.Error().Err(err).Msg("Failed to create target")
		h.jsonError(w, "Failed to create target", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("target_id", target.ID).Str("name", target.Name).Msg("Target created")
	h.flash(w, "Target created successfully")
	h.redirect(w, r, "/targets")
}

// TargetEdit renders the target edit form
func (h *Handlers) TargetEdit(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid target ID")
		h.redirect(w, r, "/targets")
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil {
		log.Error().Err(err).Int64("target_id", id).Msg("Failed to get target")
		h.flashErr(w, "Failed to load target")
		h.redirect(w, r, "/targets")
		return
	}
	if target == nil {
		h.flashErr(w, "Target not found")
		h.redirect(w, r, "/targets")
		return
	}

	h.render(w, r, "targets.html", map[string]any{
		"Target": target,
	})
}

// TargetUpdate handles target update
func (h *Handlers) TargetUpdate(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid target ID", http.StatusBadRequest)
		return
	}

	if err := r.ParseForm(); err != nil {
		h.jsonError(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil || target == nil {
		h.jsonError(w, "Target not found", http.StatusNotFound)
		return
	}

	target.Name = r.FormValue("name")
	target.URL = r.FormValue("url")
	target.Token = r.FormValue("token")
	target.APIKey = r.FormValue("api_key")
	target.Enabled = r.FormValue("enabled") == "on"

	// Parse scan delay (common to all target types)
	if delayStr := r.FormValue("scan_delay"); delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil && delay >= 0 {
			target.Config.ScanDelay = delay
		}
	} else {
		target.Config.ScanDelay = 0
	}
	if target.Type == database.TargetTypePlex {
		limit, _ := strconv.Atoi(r.FormValue("plex_max_concurrent_scans"))
		if limit < 0 {
			limit = 0
		}
		target.Config.PlexMaxConcurrentScans = limit
	} else {
		target.Config.PlexMaxConcurrentScans = 0
	}

	// Parse Emby/Jellyfin specific options
	if target.Type == database.TargetTypeEmby || target.Type == database.TargetTypeJellyfin {
		target.Config.RefreshMetadata = r.FormValue("refresh_metadata") == "on"
		// Always set MetadataRefreshMode to indicate user has configured this target
		// When disabled in UI, form may not send the value, so default to FullRefresh
		if mode := r.FormValue("metadata_refresh_mode"); mode != "" {
			target.Config.MetadataRefreshMode = database.MetadataRefreshMode(mode)
		} else {
			target.Config.MetadataRefreshMode = database.MetadataRefreshModeFullRefresh
		}
	}

	// Parse Tdarr specific options
	if target.Type == database.TargetTypeTdarr {
		target.Config.TdarrDBID = r.FormValue("tdarr_db_id")
	}

	// Parse session mode for media servers (Plex, Emby, Jellyfin)
	if target.Type == database.TargetTypePlex ||
		target.Type == database.TargetTypeEmby ||
		target.Type == database.TargetTypeJellyfin {
		if mode := r.FormValue("session_mode"); mode != "" {
			target.Config.SessionMode = database.SessionMode(mode)
		} else {
			target.Config.SessionMode = database.SessionModeWebSocket // Default to WebSocket
		}
	}

	// Parse path mappings from form
	mappings, mappingErrors := parsePathMappings(r)
	if len(mappingErrors) > 0 {
		h.jsonError(w, strings.Join(mappingErrors, "; "), http.StatusBadRequest)
		return
	}
	target.Config.PathMappings = mappings

	// Parse exclude paths
	target.Config.ExcludePaths = parseExcludePaths(r)

	// Parse exclude triggers
	target.Config.ExcludeTriggers = parseExcludeTriggers(r)

	// Parse exclude extensions
	target.Config.ExcludeExtensions = parseExcludeExtensions(r)

	// Parse advanced filters (regex patterns)
	advIncludePatterns := parseAdvancedPatterns(r, "advanced_include_patterns[]")
	advExcludePatterns := parseAdvancedPatterns(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		target.Config.AdvancedFilters = &database.AdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := target.Config.AdvancedFilters.Compile(); err != nil {
			h.jsonError(w, "Invalid regex pattern: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		target.Config.AdvancedFilters = nil
	}

	if err := h.db.UpdateTarget(target); err != nil {
		log.Error().Err(err).Int64("target_id", id).Msg("Failed to update target")
		h.jsonError(w, "Failed to update target", http.StatusInternalServerError)
		return
	}

	// Invalidate library cache for this target since path mappings may have changed
	if err := h.db.DeleteCachedLibraries(id); err != nil {
		log.Warn().Err(err).Int64("target_id", id).Msg("Failed to invalidate library cache")
	}

	log.Info().Int64("target_id", id).Msg("Target updated")
	h.flash(w, "Target updated successfully")
	h.redirect(w, r, "/targets")
}

// parsePathMappings extracts and validates path mappings from form data
// Returns the valid mappings and any validation errors encountered
func parsePathMappings(r *http.Request) ([]database.TargetPathMapping, []string) {
	fromPaths := r.Form["path_mapping_from[]"]
	toPaths := r.Form["path_mapping_to[]"]

	var mappings []database.TargetPathMapping
	var errors []string

	for i := 0; i < len(fromPaths) && i < len(toPaths); i++ {
		from := strings.TrimSpace(fromPaths[i])
		to := strings.TrimSpace(toPaths[i])

		// Skip empty mappings
		if from == "" && to == "" {
			continue
		}

		// Validate the mapping
		if err := ValidatePathMapping(from, to); err != nil {
			errors = append(errors, fmt.Sprintf("Path mapping %d: %s", i+1, err.Error()))
			continue
		}

		// Normalize paths
		mappings = append(mappings, database.TargetPathMapping{
			From: NormalizePath(from),
			To:   NormalizePath(to),
		})
	}
	return mappings, errors
}

// parseExcludePaths extracts exclude paths from form data
func parseExcludePaths(r *http.Request) []string {
	rawPaths := r.Form["exclude_paths[]"]
	var paths []string
	for _, path := range rawPaths {
		path = strings.TrimSpace(path)
		if path != "" {
			paths = append(paths, path)
		}
	}
	return paths
}

// parseExcludeTriggers extracts exclude triggers from form data
func parseExcludeTriggers(r *http.Request) []string {
	rawTriggers := r.Form["exclude_triggers[]"]
	var triggers []string
	for _, trigger := range rawTriggers {
		trigger = strings.TrimSpace(trigger)
		if trigger != "" {
			triggers = append(triggers, trigger)
		}
	}
	return triggers
}

// TargetDelete handles target deletion
func (h *Handlers) TargetDelete(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid target ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteTarget(id); err != nil {
		log.Error().Err(err).Int64("target_id", id).Msg("Failed to delete target")
		h.jsonError(w, "Failed to delete target", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("target_id", id).Msg("Target deleted")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Target deleted successfully")
	h.redirect(w, r, "/targets")
}

// TargetTest tests connection to a media server
func (h *Handlers) TargetTest(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid target ID", http.StatusBadRequest)
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil || target == nil {
		h.jsonError(w, "Target not found", http.StatusNotFound)
		return
	}

	h.testTarget(w, r, target)
}

// TargetTestNew tests connection for a new target (not yet saved)
func (h *Handlers) TargetTestNew(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form data (used by FormData in JavaScript)
	if err := r.ParseMultipartForm(32 << 10); err != nil {
		// Fall back to regular form parsing
		if err := r.ParseForm(); err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": false,
				"message": "Invalid form data",
			})
			return
		}
	}

	targetType := database.TargetType(r.FormValue("type"))
	if targetType != database.TargetTypePlex &&
		targetType != database.TargetTypeEmby &&
		targetType != database.TargetTypeJellyfin &&
		targetType != database.TargetTypeAutoplow &&
		targetType != database.TargetTypeTdarr {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": fmt.Sprintf("Invalid target type: %q", r.FormValue("type")),
		})
		return
	}

	target := &database.Target{
		Type:   targetType,
		URL:    r.FormValue("url"),
		Token:  r.FormValue("token"),
		APIKey: r.FormValue("api_key"),
	}

	// Set Tdarr specific config for testing
	if targetType == database.TargetTypeTdarr {
		target.Config.TdarrDBID = r.FormValue("tdarr_db_id")
	}

	h.testTarget(w, r, target)
}

// testTarget tests connection to a target and returns the result
func (h *Handlers) testTarget(w http.ResponseWriter, r *http.Request, target *database.Target) {
	// Create targets manager and get target instance
	mgr := targets.NewManager(h.db)
	t, err := mgr.GetTargetForDBTarget(target)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// Test connection with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := t.TestConnection(ctx); err != nil {
		log.Warn().Err(err).Str("target_url", target.URL).Msg("Target connection test failed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// On success, also fetch libraries to display
	libraries, err := t.GetLibraries(ctx)
	if err != nil {
		log.Warn().Err(err).Str("target_url", target.URL).Msg("Failed to get libraries after successful connection test")
		// Still return success, just without libraries
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"message": "Connection successful",
		})
		return
	}

	log.Info().Str("target_url", target.URL).Int("library_count", len(libraries)).Msg("Target connection test successful")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":   true,
		"message":   "Connection successful",
		"libraries": libraries,
	})
}

// TargetLibraries returns available libraries for a target
func (h *Handlers) TargetLibraries(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid target ID", http.StatusBadRequest)
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil || target == nil {
		h.jsonError(w, "Target not found", http.StatusNotFound)
		return
	}

	// Create targets manager and get libraries
	mgr := targets.NewManager(h.db)
	t, err := mgr.GetTargetForDBTarget(target)
	if err != nil {
		h.jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	libraries, err := t.GetLibraries(ctx)
	if err != nil {
		h.jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(libraries)
}

// parseExcludeExtensions extracts exclude extensions from form data
func parseExcludeExtensions(r *http.Request) []string {
	rawExtensions := r.Form["exclude_extensions[]"]
	var extensions []string
	for _, ext := range rawExtensions {
		ext = strings.TrimSpace(ext)
		if ext != "" {
			extensions = append(extensions, ext)
		}
	}
	return extensions
}

// parseAdvancedPatterns extracts advanced filter patterns from form data
func parseAdvancedPatterns(r *http.Request, fieldName string) []string {
	rawPatterns := r.Form[fieldName]
	var patterns []string
	for _, pattern := range rawPatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern != "" {
			patterns = append(patterns, pattern)
		}
	}
	return patterns
}

package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
)

// APITriggerSonarr handles Sonarr webhook triggers
func (h *Handlers) APITriggerSonarr(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "sonarr")
}

// APITriggerRadarr handles Radarr webhook triggers
func (h *Handlers) APITriggerRadarr(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "radarr")
}

// APITriggerLidarr handles Lidarr webhook triggers
func (h *Handlers) APITriggerLidarr(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "lidarr")
}

// APITriggerWebhook handles generic webhook triggers
func (h *Handlers) APITriggerWebhook(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "webhook")
}

// APITriggerAutoplow handles autoplow-to-autoplow webhook triggers
func (h *Handlers) APITriggerAutoplow(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "autoplow")
}

// APITriggerATrain handles A-Train webhook triggers (compatible with autoscan a_train trigger)
func (h *Handlers) APITriggerATrain(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "a_train")
}

// APITriggerAutoscan handles autoscan-compatible triggers
// This provides compatibility with the autoscan format using query param "dir" for path
func (h *Handlers) APITriggerAutoscan(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "autoscan")
}

// APITriggerBazarr handles Bazarr webhook triggers
// Bazarr uses query param "path" to specify the file path
func (h *Handlers) APITriggerBazarr(w http.ResponseWriter, r *http.Request) {
	h.handleTrigger(w, r, "bazarr")
}

// handleTrigger is the common handler for all trigger types
func (h *Handlers) handleTrigger(w http.ResponseWriter, r *http.Request, expectedType string) {
	// Get trigger ID from URL
	idStr := chi.URLParam(r, "id")
	triggerID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid trigger ID", http.StatusBadRequest)
		return
	}

	// Get trigger auth type to determine which auth method to use
	authType, err := h.triggerAuthService.GetTriggerAuthType(triggerID)
	if err != nil {
		log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to get trigger auth type")
		h.jsonError(w, "Trigger not found or disabled", http.StatusNotFound)
		return
	}

	var valid bool

	switch authType {
	case database.AuthTypeBasic:
		// Try HTTP Basic Auth
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="Trigger Authentication"`)
			h.jsonError(w, "Basic authentication required", http.StatusUnauthorized)
			return
		}
		valid, err = h.triggerAuthService.ValidateTriggerBasicAuth(triggerID, username, password)
		if err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to validate basic auth")
			h.jsonError(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if !valid {
			w.Header().Set("WWW-Authenticate", `Basic realm="Trigger Authentication"`)
			h.jsonError(w, "Invalid credentials", http.StatusUnauthorized)
			return
		}

	default: // AuthTypeAPIKey
		// Get API key from query param or header
		apiKey := r.URL.Query().Get("api_key")
		if apiKey == "" {
			apiKey = r.Header.Get("X-API-Key")
		}
		if apiKey == "" {
			h.jsonError(w, "API key required", http.StatusUnauthorized)
			return
		}

		// Validate API key matches trigger
		valid, err = h.triggerAuthService.ValidateTriggerAPIKey(triggerID, apiKey)
		if err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to validate API key")
			h.jsonError(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if !valid {
			h.jsonError(w, "Invalid API key for this trigger", http.StatusUnauthorized)
			return
		}
	}

	// Get trigger info with parsed config
	trigger, err := h.db.GetTrigger(triggerID)
	if err != nil || trigger == nil {
		h.jsonError(w, "Trigger not found", http.StatusNotFound)
		return
	}

	// Verify trigger type matches
	if string(trigger.Type) != expectedType {
		h.jsonError(w, "Trigger type mismatch", http.StatusBadRequest)
		return
	}

	// Read and log raw request body at trace level for debugging
	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			log.Warn().Err(err).Int64("trigger_id", triggerID).Msg("Failed to read webhook request body")
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		if len(bodyBytes) > 0 {
			log.Trace().
				Str("type", expectedType).
				Int64("trigger_id", triggerID).
				RawJSON("payload", bodyBytes).
				Msg("Trigger webhook received")
		}
	}

	// Extract event type from payload (for *arr webhooks)
	eventType := extractEventType(expectedType, bodyBytes)

	// Check for ignored events from *arr applications and return success without queueing
	if _, ignored := shouldIgnoreEvent(expectedType, bodyBytes); ignored {
		log.Debug().
			Str("type", expectedType).
			Int64("trigger_id", triggerID).
			Str("event_type", eventType).
			Msg("Ignoring event")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"message": "Event ignored: " + eventType,
		})
		return
	}

	// Parse request body based on type
	var paths []string
	switch expectedType {
	case "sonarr":
		paths = h.parseSonarrWebhook(r)
	case "radarr":
		paths = h.parseRadarrWebhook(r)
	case "lidarr":
		paths = h.parseLidarrWebhook(r)
	case "webhook", "autoplow":
		paths = h.parseWebhookTrigger(r)
	case "a_train":
		paths = h.parseATrainWebhook(r)
	case "autoscan":
		paths = h.parseAutoscanTrigger(r)
	case "bazarr":
		paths = h.parseBazarrTrigger(r)
	}

	if len(paths) == 0 {
		h.jsonError(w, "No paths found in request", http.StatusBadRequest)
		return
	}

	// Apply path filters and rewrites in the configured order
	// Default: filter before rewrite (filters apply to original paths)
	if trigger.Config.FilterAfterRewrite {
		// Filter after rewrite: rewrites first, then filters on rewritten paths
		paths = applyPathRewrites(paths, trigger.Config.PathRewrites)
		paths = filterPaths(paths, &trigger.Config)
	} else {
		// Default: filter before rewrite (filters apply to original paths)
		paths = filterPaths(paths, &trigger.Config)
		paths = applyPathRewrites(paths, trigger.Config.PathRewrites)
	}

	if len(paths) == 0 {
		log.Info().
			Str("type", expectedType).
			Int64("trigger_id", triggerID).
			Msg("All paths filtered out by trigger path rules")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"message": "All paths filtered out by path rules",
			"paths":   []string{},
		})
		return
	}

	log.Info().
		Str("type", expectedType).
		Int64("trigger_id", triggerID).
		Strs("paths", paths).
		Msg("Trigger received")

	// Queue scans for processing
	for _, path := range paths {
		h.processor.QueueScan(processor.ScanRequest{
			Path:      path,
			TriggerID: &triggerID,
			Priority:  trigger.Priority,
			EventType: eventType,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "Scan queued",
		"paths":   paths,
	})
}

// parseSonarrWebhook extracts paths from Sonarr webhook payload
func (h *Handlers) parseSonarrWebhook(r *http.Request) []string {
	var payload struct {
		EventType       string `json:"eventType"`
		DestinationPath string `json:"destinationPath"`
		Series          struct {
			Path string `json:"path"`
		} `json:"series"`
		EpisodeFile struct {
			Path         string `json:"path"`
			RelativePath string `json:"relativePath"`
		} `json:"episodeFile"`
		EpisodeFiles []struct {
			Path         string `json:"path"`
			RelativePath string `json:"relativePath"`
		} `json:"episodeFiles"`
		RenamedEpisodeFiles []struct {
			PreviousPath string `json:"previousPath"`
			RelativePath string `json:"relativePath"`
		} `json:"renamedEpisodeFiles"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Error().Err(err).Msg("Failed to parse Sonarr webhook")
		return nil
	}

	if payload.Series.Path == "" {
		return nil
	}

	seasonPathFromFile := func(filePath, relativePath string) string {
		fullPath := filePath
		if fullPath == "" && relativePath != "" {
			fullPath = joinPath(payload.Series.Path, relativePath)
		} else if fullPath != "" && !strings.HasPrefix(fullPath, "/") {
			fullPath = joinPath(payload.Series.Path, fullPath)
		}
		if fullPath == "" {
			return ""
		}

		dir := filepath.Dir(filepath.Clean(fullPath))
		if dir == "." || dir == "/" {
			return ""
		}
		return dir
	}

	collectSeasonPaths := func() []string {
		seen := make(map[string]struct{})
		paths := make([]string, 0)
		add := func(path string) {
			if path == "" {
				return
			}
			if _, ok := seen[path]; ok {
				return
			}
			seen[path] = struct{}{}
			paths = append(paths, path)
		}

		for _, file := range payload.EpisodeFiles {
			add(seasonPathFromFile(file.Path, file.RelativePath))
		}
		add(seasonPathFromFile(payload.EpisodeFile.Path, payload.EpisodeFile.RelativePath))

		if len(paths) == 0 && payload.DestinationPath != "" {
			destPath := payload.DestinationPath
			if !strings.HasPrefix(destPath, "/") {
				destPath = joinPath(payload.Series.Path, destPath)
			}
			destPath = filepath.Clean(destPath)
			if destPath != "." && destPath != "/" {
				add(destPath)
			}
		}

		return paths
	}

	switch payload.EventType {
	case "Rename":
		// Return season folder paths for renamed files when available.
		seen := make(map[string]struct{})
		var paths []string
		for _, file := range payload.RenamedEpisodeFiles {
			if season := seasonPathFromFile(file.PreviousPath, ""); season != "" {
				if _, ok := seen[season]; !ok {
					seen[season] = struct{}{}
					paths = append(paths, season)
				}
			}
			if season := seasonPathFromFile("", file.RelativePath); season != "" {
				if _, ok := seen[season]; !ok {
					seen[season] = struct{}{}
					paths = append(paths, season)
				}
			}
		}
		if len(paths) == 0 {
			return []string{payload.Series.Path}
		}
		return paths
	case "EpisodeFileDelete":
		if paths := collectSeasonPaths(); len(paths) > 0 {
			return paths
		}
		// Fallback to series folder path for episode deletion (file no longer exists)
		return []string{payload.Series.Path}
	default:
		if paths := collectSeasonPaths(); len(paths) > 0 {
			return paths
		}
		// For other events (Download, SeriesDelete, Test, etc.), return series path
		return []string{payload.Series.Path}
	}
}

// parseRadarrWebhook extracts paths from Radarr webhook payload
func (h *Handlers) parseRadarrWebhook(r *http.Request) []string {
	var payload struct {
		EventType string `json:"eventType"`
		Movie     struct {
			FolderPath string `json:"folderPath"`
		} `json:"movie"`
		MovieFile struct {
			RelativePath string `json:"relativePath"`
		} `json:"movieFile"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Error().Err(err).Msg("Failed to parse Radarr webhook")
		return nil
	}

	if payload.Movie.FolderPath == "" {
		return nil
	}

	// All events return the movie folder path
	// For MovieFileDelete, the file no longer exists so we scan the folder
	return []string{payload.Movie.FolderPath}
}

// parseLidarrWebhook extracts paths from Lidarr webhook payload
func (h *Handlers) parseLidarrWebhook(r *http.Request) []string {
	var payload struct {
		EventType string `json:"eventType"`
		Artist    struct {
			Path string `json:"path"`
		} `json:"artist"`
		RenamedTrackFiles []struct {
			Path         string `json:"path"`
			PreviousPath string `json:"previousPath"`
		} `json:"renamedTrackFiles"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Error().Err(err).Msg("Failed to parse Lidarr webhook")
		return nil
	}

	if payload.Artist.Path == "" {
		return nil
	}

	switch payload.EventType {
	case "Rename":
		// Return both old and new paths for each renamed track
		var paths []string
		for _, file := range payload.RenamedTrackFiles {
			paths = append(paths, file.PreviousPath)
			paths = append(paths, file.Path)
		}
		if len(paths) == 0 {
			return []string{payload.Artist.Path}
		}
		return paths
	default:
		// For other events (Download, ArtistDelete, AlbumDelete, Test, etc.), return artist path
		return []string{payload.Artist.Path}
	}
}

// parseWebhookTrigger extracts paths from generic webhook trigger request
func (h *Handlers) parseWebhookTrigger(r *http.Request) []string {
	var payload struct {
		Paths []string `json:"paths"`
		Path  string   `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Error().Err(err).Msg("Failed to parse manual trigger")
		return nil
	}

	if len(payload.Paths) > 0 {
		return payload.Paths
	}
	if payload.Path != "" {
		return []string{payload.Path}
	}

	return nil
}

// parseATrainWebhook extracts paths from A-Train webhook payload
// A-Train sends JSON with Created and Deleted arrays of paths
// Compatible with autoscan's a_train trigger format
func (h *Handlers) parseATrainWebhook(r *http.Request) []string {
	var payload struct {
		Created []string `json:"Created"`
		Deleted []string `json:"Deleted"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Error().Err(err).Msg("Failed to parse A-Train webhook")
		return nil
	}

	// Combine created and deleted paths (both trigger scans)
	paths := make([]string, 0, len(payload.Created)+len(payload.Deleted))
	paths = append(paths, payload.Created...)
	paths = append(paths, payload.Deleted...)

	return paths
}

// parseAutoscanTrigger extracts paths from autoscan-compatible requests
// Autoscan uses a query parameter "dir" for the path (GET request)
// Also supports POST with JSON body containing "dir" or "path"
func (h *Handlers) parseAutoscanTrigger(r *http.Request) []string {
	// First try query parameter (autoscan's default format)
	dir := r.URL.Query().Get("dir")
	if dir != "" {
		return []string{dir}
	}

	// Also try "path" query param for flexibility
	path := r.URL.Query().Get("path")
	if path != "" {
		return []string{path}
	}

	// If no query params, try JSON body
	if r.Body != nil && r.ContentLength > 0 {
		var payload struct {
			Dir  string `json:"dir"`
			Path string `json:"path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			if payload.Dir != "" {
				return []string{payload.Dir}
			}
			if payload.Path != "" {
				return []string{payload.Path}
			}
		}
	}

	return nil
}

// parseBazarrTrigger extracts paths from Bazarr webhook requests
// Bazarr typically sends the path via query parameter or POST body
// Used in Bazarr's custom post-processing: curl '...?path={{directory}}'
func (h *Handlers) parseBazarrTrigger(r *http.Request) []string {
	// First try query parameter (Bazarr's typical format)
	path := r.URL.Query().Get("path")
	if path != "" {
		return []string{path}
	}

	// Also try "directory" query param for flexibility
	dir := r.URL.Query().Get("directory")
	if dir != "" {
		return []string{dir}
	}

	// If no query params, try JSON body
	if r.Body != nil && r.ContentLength > 0 {
		var payload struct {
			Path      string `json:"path"`
			Directory string `json:"directory"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			if payload.Path != "" {
				return []string{payload.Path}
			}
			if payload.Directory != "" {
				return []string{payload.Directory}
			}
		}
	}

	return nil
}

// applyPathRewrites applies trigger's path rewrite rules to a list of paths
func applyPathRewrites(paths []string, rewrites []database.PathRewrite) []string {
	if len(rewrites) == 0 {
		return paths
	}

	result := make([]string, len(paths))
	for i, path := range paths {
		result[i] = rewritePath(path, rewrites)
	}
	return result
}

// rewritePath applies path rewrite rules to a single path
// Supports both simple prefix replacement and regex-based replacement
func rewritePath(path string, rewrites []database.PathRewrite) string {
	result := path
	for i := range rewrites {
		newPath := rewrites[i].Rewrite(result)
		if newPath != result {
			result = newPath
			// For simple prefix replacement, only the first match applies
			// For regex, all rules are applied sequentially
			if !rewrites[i].IsRegex {
				break
			}
		}
	}
	return result
}

// filterPaths applies trigger's include/exclude path filters to a list of paths
func filterPaths(paths []string, config *database.TriggerConfig) []string {
	if len(config.IncludePaths) == 0 && len(config.ExcludePaths) == 0 {
		return paths
	}

	var result []string
	for _, path := range paths {
		if config.MatchesPathFilters(path) {
			result = append(result, path)
		}
	}
	return result
}

// ignoredArrEvents defines which event types to ignore from *arr applications.
// Events can be ignored globally (all *arr types) or per specific trigger type.
var ignoredArrEvents = struct {
	// Global events ignored for all *arr trigger types (sonarr, radarr, lidarr)
	Global map[string]bool
	// Per-type events (checked in addition to global)
	Sonarr map[string]bool
	Radarr map[string]bool
	Lidarr map[string]bool
}{
	Global: map[string]bool{
		"Test":                      true, // Test notification
		"Grab":                      true, // Sent to download client (file not ready yet)
		"Health":                    true, // Health check notification
		"HealthRestored":            true, // Health issue resolved
		"ApplicationUpdate":         true, // Application updated
		"ManualInteractionRequired": true, // Manual import needed
	},
	Sonarr: map[string]bool{
		"SeriesAdd": true, // Series added (no files yet)
	},
	Radarr: map[string]bool{
		"MovieAdded": true, // Movie added (no files yet)
	},
	Lidarr: map[string]bool{
		"ArtistAdd": true, // Artist added (no files yet)
	},
}

// shouldIgnoreEvent checks if the webhook event should be ignored
// Returns the event type and whether it should be ignored
func shouldIgnoreEvent(triggerType string, body []byte) (string, bool) {
	eventType := extractEventType(triggerType, body)
	if eventType == "" {
		return "", false
	}

	// Check global ignored events first
	if ignoredArrEvents.Global[eventType] {
		return eventType, true
	}
	// Check per-type ignored events
	switch triggerType {
	case "sonarr":
		return eventType, ignoredArrEvents.Sonarr[eventType]
	case "radarr":
		return eventType, ignoredArrEvents.Radarr[eventType]
	case "lidarr":
		return eventType, ignoredArrEvents.Lidarr[eventType]
	}
	return eventType, false
}

// extractEventType extracts the event type from a webhook payload
func extractEventType(triggerType string, body []byte) string {
	if len(body) == 0 {
		return ""
	}

	switch triggerType {
	case "sonarr", "radarr", "lidarr":
		var payload struct {
			EventType string `json:"eventType"`
		}
		if err := json.Unmarshal(body, &payload); err == nil {
			return payload.EventType
		}
	}
	return ""
}

// joinPath joins a base path with a relative path using forward slashes
func joinPath(base, relative string) string {
	if relative == "" {
		return base
	}
	base = strings.TrimSuffix(base, "/")
	relative = strings.TrimPrefix(relative, "/")
	return base + "/" + relative
}

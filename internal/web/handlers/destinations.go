package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
)

// DestinationsPage renders the destinations list page
func (h *Handlers) DestinationsPage(w http.ResponseWriter, r *http.Request) {
	dests, err := h.db.ListDestinations()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list destinations")
		h.flashErr(w, "Failed to load destinations")
		h.redirect(w, r, "/uploads")
		return
	}

	remotes, err := h.db.ListRemotes()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list remotes")
	}

	// Get local triggers (inotify + polling) for the included triggers selector
	localTriggers, err := h.db.ListLocalTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list local triggers")
	}
	hasLocalTriggers := len(localTriggers) > 0

	h.render(w, r, "uploads.html", map[string]any{
		"Destinations":     dests,
		"Remotes":          remotes,
		"LocalTriggers":    localTriggers,
		"HasLocalTriggers": hasLocalTriggers,
		"Tab":              "destinations",
	})
}

// DestinationNew renders the new destination form
func (h *Handlers) DestinationNew(w http.ResponseWriter, r *http.Request) {
	remotes, err := h.db.ListRemotes()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list remotes")
	}

	destinations, err := h.db.ListDestinations()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list destinations")
	}

	// Get local triggers (inotify + polling) for the included triggers selector
	localTriggers, err := h.db.ListLocalTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list local triggers")
	}
	hasLocalTriggers := len(localTriggers) > 0

	targets, err := h.db.ListTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets")
	}
	var plexTargets []*database.Target
	for _, target := range targets {
		if target.Type == database.TargetTypePlex {
			plexTargets = append(plexTargets, target)
		}
	}

	settingsLoader := config.NewLoader(h.db)
	maxConcurrentScans := settingsLoader.Int("processor.max_concurrent_scans", 0)

	h.render(w, r, "uploads.html", map[string]any{
		"IsNew":                       true,
		"Tab":                         "destinations",
		"Remotes":                     remotes,
		"LocalTriggers":               localTriggers,
		"HasLocalTriggers":            hasLocalTriggers,
		"Destinations":                destinations,
		"PlexTargets":                 plexTargets,
		"ProcessorMaxConcurrentScans": maxConcurrentScans,
	})
}

// DestinationCreate handles destination creation
func (h *Handlers) DestinationCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}

	localTriggers, err := h.db.ListLocalTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list local triggers")
	}
	if len(localTriggers) == 0 {
		h.flashErr(w, "At least one local trigger is required before configuring uploads")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	localPath := r.FormValue("local_path")
	if err := ValidatePath(localPath, "local_path"); err != nil {
		h.flashErr(w, err.Error())
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}
	localPath = NormalizePath(localPath)

	if exists, err := h.db.DestinationPathExists(localPath, nil); err != nil {
		log.Error().Err(err).Msg("Failed to check destination path")
		h.flashErr(w, "Failed to validate destination path")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	} else if exists {
		h.flashErr(w, "A destination already exists for this local path")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}

	minFileAgeMinutes, _ := strconv.Atoi(r.FormValue("min_file_age_minutes"))
	if minFileAgeMinutes < 0 {
		minFileAgeMinutes = 0
	}
	minFolderSizeGB, _ := strconv.Atoi(r.FormValue("min_folder_size_gb"))
	if minFolderSizeGB < 0 {
		minFolderSizeGB = 0
	}
	usePlexTracking := r.FormValue("use_plex_scan_tracking") == "on"

	var plexTargets []*database.DestinationPlexTarget
	if usePlexTracking {
		var err error
		plexTargets, err = h.parseDestinationPlexTargets(r)
		if err != nil {
			h.flashErr(w, err.Error())
			h.redirect(w, r, "/uploads/destinations/new")
			return
		}
	}

	transferType := database.TransferType(r.FormValue("transfer_type"))
	if transferType != database.TransferTypeCopy && transferType != database.TransferTypeMove {
		transferType = database.TransferTypeMove // Default to move
	}

	// Parse exclude paths (from form array)
	var excludePaths []string
	for _, p := range r.Form["exclude_paths[]"] {
		p = strings.TrimSpace(p)
		if p != "" {
			excludePaths = append(excludePaths, p)
		}
	}

	// Parse exclude extensions (from form array)
	var excludeExtensions []string
	for _, ext := range r.Form["exclude_extensions[]"] {
		ext = strings.TrimSpace(ext)
		if ext != "" {
			excludeExtensions = append(excludeExtensions, ext)
		}
	}

	// Parse advanced filters (regex patterns)
	var advancedFilters *database.DestinationAdvancedFilters
	advIncludePatterns := parseDestAdvancedPatterns(r, "advanced_include_patterns[]")
	advExcludePatterns := parseDestAdvancedPatterns(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		advancedFilters = &database.DestinationAdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := advancedFilters.Compile(); err != nil {
			h.flashErr(w, "Invalid regex pattern: "+err.Error())
			h.redirect(w, r, "/uploads/destinations/new")
			return
		}
	}

	// Parse included triggers (inotify triggers that can upload to this destination)
	var includedTriggers []int64
	for _, idStr := range r.Form["included_triggers[]"] {
		if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
			includedTriggers = append(includedTriggers, id)
		}
	}

	// At least one trigger must be selected
	if len(includedTriggers) == 0 {
		h.flashErr(w, "At least one local trigger must be selected")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}

	// Parse remote mappings and validate at least one is provided
	remoteIDs := r.Form["remote_ids[]"]
	remotePaths := r.Form["remote_paths[]"]
	if len(remoteIDs) == 0 {
		h.flashErr(w, "At least one remote must be configured")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}

	dest := &database.Destination{
		LocalPath:         localPath,
		MinFileAgeMinutes: minFileAgeMinutes,
		MinFolderSizeGB:   minFolderSizeGB,
		UsePlexTracking:   usePlexTracking,
		TransferType:      transferType,
		Enabled:           r.FormValue("enabled") == "on",
		ExcludePaths:      excludePaths,
		ExcludeExtensions: excludeExtensions,
		AdvancedFilters:   advancedFilters,
		IncludedTriggers:  includedTriggers,
	}

	if err := h.db.CreateDestination(dest); err != nil {
		log.Error().Err(err).Msg("Failed to create destination")
		h.flashErr(w, "Failed to create destination")
		h.redirect(w, r, "/uploads/destinations/new")
		return
	}

	if usePlexTracking {
		if err := h.db.SetDestinationPlexTargets(dest.ID, plexTargets); err != nil {
			log.Error().Err(err).Int64("destination_id", dest.ID).Msg("Failed to save destination Plex targets")
			h.flashErr(w, "Failed to save Plex tracking settings")
			h.redirect(w, r, "/uploads/destinations/new")
			return
		}
	} else {
		if err := h.db.ClearDestinationPlexTargets(dest.ID); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to clear destination Plex targets")
		}
	}

	// Add remote mappings (priority is based on order in the form)
	for i := range remoteIDs {
		remoteID, err := strconv.ParseInt(remoteIDs[i], 10, 64)
		if err != nil {
			continue
		}
		remotePath := ""
		if i < len(remotePaths) {
			remotePath = remotePaths[i]
		}

		// Validate remote path if provided
		if remotePath != "" {
			if err := ValidateRemotePath(remotePath, "remote_path"); err != nil {
				log.Warn().Err(err).Int64("destination_id", dest.ID).Int64("remote_id", remoteID).Msg("Invalid remote path, skipping")
				continue
			}
			remotePath = NormalizePath(remotePath)
		}

		dr := &database.DestinationRemote{
			DestinationID: dest.ID,
			RemoteID:      remoteID,
			Priority:      i + 1, // Priority based on order
			RemotePath:    remotePath,
		}
		if err := h.db.AddDestinationRemote(dr); err != nil {
			log.Error().Err(err).Int64("destination_id", dest.ID).Int64("remote_id", remoteID).Msg("Failed to add remote mapping")
		}
	}

	log.Info().Int64("destination_id", dest.ID).Str("local_path", dest.LocalPath).Msg("Destination created")
	h.flash(w, "Destination created successfully")
	h.redirect(w, r, "/uploads/destinations")
}

// DestinationEdit renders the destination edit form
func (h *Handlers) DestinationEdit(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid destination ID")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	dest, err := h.db.GetDestination(id)
	if err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to get destination")
		h.flashErr(w, "Failed to load destination")
		h.redirect(w, r, "/uploads/destinations")
		return
	}
	if dest == nil {
		h.flashErr(w, "Destination not found")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	remotes, err := h.db.ListRemotes()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list remotes")
	}

	destinations, err := h.db.ListDestinations()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list destinations")
	}

	// Get local triggers (inotify + polling) for the included triggers selector
	localTriggers, err := h.db.ListLocalTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list local triggers")
	}
	hasLocalTriggers := len(localTriggers) > 0

	targets, err := h.db.ListTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets")
	}
	var plexTargets []*database.Target
	for _, target := range targets {
		if target.Type == database.TargetTypePlex {
			plexTargets = append(plexTargets, target)
		}
	}

	settingsLoader := config.NewLoader(h.db)
	maxConcurrentScans := settingsLoader.Int("processor.max_concurrent_scans", 0)

	h.render(w, r, "uploads.html", map[string]any{
		"Destination":                 dest,
		"Tab":                         "destinations",
		"Remotes":                     remotes,
		"LocalTriggers":               localTriggers,
		"HasLocalTriggers":            hasLocalTriggers,
		"Destinations":                destinations,
		"PlexTargets":                 plexTargets,
		"ProcessorMaxConcurrentScans": maxConcurrentScans,
	})
}

// DestinationUpdate handles destination update
func (h *Handlers) DestinationUpdate(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid destination ID")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	localTriggers, err := h.db.ListLocalTriggers()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list local triggers")
	}
	if len(localTriggers) == 0 {
		h.flashErr(w, "At least one local trigger is required before configuring uploads")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	dest, err := h.db.GetDestination(id)
	if err != nil || dest == nil {
		h.flashErr(w, "Destination not found")
		h.redirect(w, r, "/uploads/destinations")
		return
	}

	localPath := r.FormValue("local_path")
	if err := ValidatePath(localPath, "local_path"); err != nil {
		h.flashErr(w, err.Error())
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	dest.LocalPath = NormalizePath(localPath)
	dest.Enabled = r.FormValue("enabled") == "on"

	if exists, err := h.db.DestinationPathExists(dest.LocalPath, &dest.ID); err != nil {
		log.Error().Err(err).Msg("Failed to check destination path")
		h.flashErr(w, "Failed to validate destination path")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	} else if exists {
		h.flashErr(w, "A destination already exists for this local path")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	transferType := database.TransferType(r.FormValue("transfer_type"))
	if transferType != database.TransferTypeCopy && transferType != database.TransferTypeMove {
		transferType = database.TransferTypeMove // Default to move
	}
	dest.TransferType = transferType

	minFileAgeMinutes, _ := strconv.Atoi(r.FormValue("min_file_age_minutes"))
	if minFileAgeMinutes < 0 {
		minFileAgeMinutes = 0
	}
	minFolderSizeGB, _ := strconv.Atoi(r.FormValue("min_folder_size_gb"))
	if minFolderSizeGB < 0 {
		minFolderSizeGB = 0
	}
	usePlexTracking := r.FormValue("use_plex_scan_tracking") == "on"

	dest.MinFileAgeMinutes = minFileAgeMinutes
	dest.MinFolderSizeGB = minFolderSizeGB
	dest.UsePlexTracking = usePlexTracking

	var plexTargets []*database.DestinationPlexTarget
	if usePlexTracking {
		var err error
		plexTargets, err = h.parseDestinationPlexTargets(r)
		if err != nil {
			h.flashErr(w, err.Error())
			h.redirect(w, r, "/uploads/destinations/"+idStr)
			return
		}
	}

	// Parse exclude paths (from form array)
	dest.ExcludePaths = nil
	for _, p := range r.Form["exclude_paths[]"] {
		p = strings.TrimSpace(p)
		if p != "" {
			dest.ExcludePaths = append(dest.ExcludePaths, p)
		}
	}

	// Parse exclude extensions (from form array)
	dest.ExcludeExtensions = nil
	for _, ext := range r.Form["exclude_extensions[]"] {
		ext = strings.TrimSpace(ext)
		if ext != "" {
			dest.ExcludeExtensions = append(dest.ExcludeExtensions, ext)
		}
	}

	// Parse advanced filters (regex patterns)
	advIncludePatterns := parseDestAdvancedPatterns(r, "advanced_include_patterns[]")
	advExcludePatterns := parseDestAdvancedPatterns(r, "advanced_exclude_patterns[]")
	if len(advIncludePatterns) > 0 || len(advExcludePatterns) > 0 {
		dest.AdvancedFilters = &database.DestinationAdvancedFilters{
			IncludePatterns: advIncludePatterns,
			ExcludePatterns: advExcludePatterns,
		}
		// Validate regex patterns
		if err := dest.AdvancedFilters.Compile(); err != nil {
			h.flashErr(w, "Invalid regex pattern: "+err.Error())
			h.redirect(w, r, "/uploads/destinations/"+idStr)
			return
		}
	} else {
		dest.AdvancedFilters = nil
	}

	// Parse included triggers (inotify triggers that can upload to this destination)
	dest.IncludedTriggers = nil
	for _, idStr := range r.Form["included_triggers[]"] {
		if triggerID, err := strconv.ParseInt(idStr, 10, 64); err == nil {
			dest.IncludedTriggers = append(dest.IncludedTriggers, triggerID)
		}
	}

	// At least one trigger must be selected
	if len(dest.IncludedTriggers) == 0 {
		h.flashErr(w, "At least one local trigger must be selected")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	// Parse remote mappings and validate at least one is provided
	remoteIDs := r.Form["remote_ids[]"]
	remotePaths := r.Form["remote_paths[]"]
	if len(remoteIDs) == 0 {
		h.flashErr(w, "At least one remote must be configured")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	if err := h.db.UpdateDestination(dest); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to update destination")
		h.flashErr(w, "Failed to update destination")
		h.redirect(w, r, "/uploads/destinations/"+idStr)
		return
	}

	if usePlexTracking {
		if err := h.db.SetDestinationPlexTargets(dest.ID, plexTargets); err != nil {
			log.Error().Err(err).Int64("destination_id", dest.ID).Msg("Failed to save destination Plex targets")
			h.flashErr(w, "Failed to save Plex tracking settings")
			h.redirect(w, r, "/uploads/destinations/"+idStr)
			return
		}
	} else {
		if err := h.db.ClearDestinationPlexTargets(dest.ID); err != nil {
			log.Warn().Err(err).Int64("destination_id", dest.ID).Msg("Failed to clear destination Plex targets")
		}
	}

	// Update remote mappings - clear existing and add from form
	if err := h.db.ClearDestinationRemotes(id); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to clear remote mappings")
	}

	for i := range remoteIDs {
		remoteID, err := strconv.ParseInt(remoteIDs[i], 10, 64)
		if err != nil {
			continue
		}
		remotePath := ""
		if i < len(remotePaths) {
			remotePath = remotePaths[i]
		}

		// Validate remote path if provided
		if remotePath != "" {
			if err := ValidateRemotePath(remotePath, "remote_path"); err != nil {
				log.Warn().Err(err).Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Invalid remote path, skipping")
				continue
			}
			remotePath = NormalizePath(remotePath)
		}

		dr := &database.DestinationRemote{
			DestinationID: id,
			RemoteID:      remoteID,
			Priority:      i + 1, // Priority based on order
			RemotePath:    remotePath,
		}
		if err := h.db.AddDestinationRemote(dr); err != nil {
			log.Error().Err(err).Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Failed to add remote mapping")
		}
	}

	log.Info().Int64("destination_id", id).Msg("Destination updated")
	h.flash(w, "Destination updated successfully")
	h.redirect(w, r, "/uploads/destinations")
}

// DestinationDelete handles destination deletion
func (h *Handlers) DestinationDelete(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid destination ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteDestination(id); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to delete destination")
		h.jsonError(w, "Failed to delete destination", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("destination_id", id).Msg("Destination deleted")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Destination deleted successfully")
	h.redirect(w, r, "/uploads/destinations")
}

// DestinationAddRemote adds a remote mapping to a destination
func (h *Handlers) DestinationAddRemote(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid destination ID", http.StatusBadRequest)
		return
	}

	// ParseMultipartForm for multipart/form-data (which FormData sends)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		if err := r.ParseForm(); err != nil {
			h.jsonError(w, "Invalid form data", http.StatusBadRequest)
			return
		}
	}

	remoteID, err := strconv.ParseInt(r.FormValue("remote_id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid remote ID", http.StatusBadRequest)
		return
	}

	remotePath := r.FormValue("remote_path")
	if err := ValidateRemotePath(remotePath, "remote_path"); err != nil {
		h.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	remotePath = NormalizePath(remotePath)

	// Get the next priority (max + 1)
	maxPriority, err := h.db.GetMaxDestinationRemotePriority(id)
	if err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to get max priority")
		h.jsonError(w, "Failed to add remote destination", http.StatusInternalServerError)
		return
	}

	dr := &database.DestinationRemote{
		DestinationID: id,
		RemoteID:      remoteID,
		Priority:      maxPriority + 1,
		RemotePath:    remotePath,
	}

	if err := h.db.AddDestinationRemote(dr); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Failed to add remote destination")
		h.jsonError(w, "Failed to add remote destination", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Remote destination added")
	h.flash(w, "Remote destination added")
	h.redirect(w, r, "/uploads/destinations/"+idStr)
}

// DestinationRemoveRemote removes a remote destination from a destination
func (h *Handlers) DestinationRemoveRemote(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid destination ID", http.StatusBadRequest)
		return
	}

	remoteIDStr := chi.URLParam(r, "remote_id")
	remoteID, err := strconv.ParseInt(remoteIDStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid remote ID", http.StatusBadRequest)
		return
	}

	if err := h.db.RemoveDestinationRemote(id, remoteID); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Failed to remove remote destination")
		h.jsonError(w, "Failed to remove remote destination", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("destination_id", id).Int64("remote_id", remoteID).Msg("Remote destination removed")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Remote destination removed")
	h.redirect(w, r, "/uploads/destinations/"+idStr)
}

// DestinationReorderRemotes handles reordering remote destinations for a destination
func (h *Handlers) DestinationReorderRemotes(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid destination ID", http.StatusBadRequest)
		return
	}

	// Parse JSON body with ordered remote IDs
	var remoteIDs []int64
	if err := json.NewDecoder(r.Body).Decode(&remoteIDs); err != nil {
		h.jsonError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.db.ReorderDestinationRemotes(id, remoteIDs); err != nil {
		log.Error().Err(err).Int64("destination_id", id).Msg("Failed to reorder remote destinations")
		h.jsonError(w, "Failed to reorder remote destinations", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("destination_id", id).Msg("Remote destinations reordered")
	w.WriteHeader(http.StatusOK)
}

// parseDestAdvancedPatterns extracts advanced filter patterns from form data
func parseDestAdvancedPatterns(r *http.Request, fieldName string) []string {
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

func (h *Handlers) parseDestinationPlexTargets(r *http.Request) ([]*database.DestinationPlexTarget, error) {
	ids := r.Form["plex_target_ids[]"]
	if len(ids) == 0 {
		return nil, fmt.Errorf("Select at least one Plex target for scan tracking")
	}

	targets, err := h.db.ListTargets()
	if err != nil {
		return nil, fmt.Errorf("Failed to load targets")
	}

	plexTargets := make(map[int64]*database.Target)
	for _, target := range targets {
		if target.Type == database.TargetTypePlex {
			plexTargets[target.ID] = target
		}
	}
	if len(plexTargets) == 0 {
		return nil, fmt.Errorf("No Plex targets are configured")
	}

	var results []*database.DestinationPlexTarget
	seen := make(map[int64]struct{})

	for _, idStr := range ids {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		target, ok := plexTargets[id]
		if !ok {
			return nil, fmt.Errorf("Selected Plex target not found")
		}

		idleStr := r.FormValue(fmt.Sprintf("plex_idle_threshold_%d", id))
		idleSeconds, err := strconv.Atoi(idleStr)
		if err != nil || idleSeconds < 60 {
			return nil, fmt.Errorf("Idle threshold must be at least 60 seconds for Plex target: %s", target.Name)
		}

		results = append(results, &database.DestinationPlexTarget{
			TargetID:             id,
			IdleThresholdSeconds: idleSeconds,
			TargetName:           target.Name,
		})
		seen[id] = struct{}{}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("Select at least one Plex target for scan tracking")
	}

	return results, nil
}

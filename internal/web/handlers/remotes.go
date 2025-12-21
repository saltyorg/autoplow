package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/rclone"
)

// RemotesPage renders the remotes list page
func (h *Handlers) RemotesPage(w http.ResponseWriter, r *http.Request) {
	remotes, err := h.db.ListRemotes()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list remotes")
		h.flashErr(w, "Failed to load remotes")
		h.redirect(w, r, "/uploads")
		return
	}

	h.render(w, r, "uploads.html", map[string]any{
		"Remotes": remotes,
		"Tab":     "remotes",
	})
}

// RemoteNew renders the new remote form
func (h *Handlers) RemoteNew(w http.ResponseWriter, r *http.Request) {
	// Get available rclone remotes and providers from cache
	var availableRemotes []string
	var providers []rclone.Provider
	if h.rcloneMgr != nil && h.rcloneMgr.IsRunning() {
		availableRemotes = h.rcloneMgr.ConfiguredRemotes()
		providers = h.rcloneMgr.Providers()
		// Sort providers alphabetically
		sort.Slice(providers, func(i, j int) bool {
			return providers[i].Name < providers[j].Name
		})
	}

	h.render(w, r, "uploads.html", map[string]any{
		"IsNew":            true,
		"Tab":              "remotes",
		"AvailableRemotes": availableRemotes,
		"Providers":        providers,
	})
}

// RemoteCreate handles remote creation
func (h *Handlers) RemoteCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/uploads/remotes/new")
		return
	}

	name := r.FormValue("name")
	if name == "" {
		h.flashErr(w, "Name is required")
		h.redirect(w, r, "/uploads/remotes/new")
		return
	}

	rcloneRemote := r.FormValue("rclone_remote")
	if rcloneRemote == "" {
		h.flashErr(w, "Rclone remote is required")
		h.redirect(w, r, "/uploads/remotes/new")
		return
	}

	// Parse transfer options from form
	transferOptions := parseTransferOptions(r)

	remote := &database.Remote{
		Name:            name,
		RcloneRemote:    rcloneRemote,
		Enabled:         r.FormValue("enabled") == "on",
		TransferOptions: transferOptions,
	}

	if err := h.db.CreateRemote(remote); err != nil {
		log.Error().Err(err).Msg("Failed to create remote")
		h.flashErr(w, "Failed to create remote")
		h.redirect(w, r, "/uploads/remotes/new")
		return
	}

	log.Info().Int64("remote_id", remote.ID).Str("name", remote.Name).Msg("Remote created")
	h.flash(w, "Remote created successfully")
	h.redirect(w, r, "/uploads/remotes")
}

// parseTransferOptions extracts transfer options from form values
// It uses type information from hidden fields (opt_type_*) to properly parse values
func parseTransferOptions(r *http.Request) map[string]any {
	options := make(map[string]any)

	// Check for transfer options fields (prefixed with "opt_")
	for key, values := range r.Form {
		if strings.HasPrefix(key, "opt_") && !strings.HasPrefix(key, "opt_type_") && len(values) > 0 && values[0] != "" {
			optName := strings.TrimPrefix(key, "opt_")
			value := values[0]

			// Get the type from the hidden field if available
			optType := r.FormValue("opt_type_" + optName)

			if optType != "" {
				// Use type-aware parsing
				parsedValue, err := rclone.ParseOptionValue(optType, value)
				if err != nil {
					// Log warning but fall back to string
					log.Warn().Err(err).Str("option", optName).Str("type", optType).Msg("Failed to parse option value, using string")
					options[optName] = value
				} else {
					options[optName] = parsedValue
				}
			} else {
				// Fallback: Try to parse as int first, then float, otherwise keep as string
				if intVal, err := strconv.Atoi(value); err == nil {
					options[optName] = intVal
				} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
					options[optName] = floatVal
				} else if value == "true" {
					options[optName] = true
				} else if value == "false" {
					options[optName] = false
				} else {
					options[optName] = value
				}
			}
		}
	}

	if len(options) == 0 {
		return nil
	}
	return options
}

// RemoteEdit renders the remote edit form
func (h *Handlers) RemoteEdit(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid remote ID")
		h.redirect(w, r, "/uploads/remotes")
		return
	}

	remote, err := h.db.GetRemote(id)
	if err != nil {
		log.Error().Err(err).Int64("remote_id", id).Msg("Failed to get remote")
		h.flashErr(w, "Failed to load remote")
		h.redirect(w, r, "/uploads/remotes")
		return
	}
	if remote == nil {
		h.flashErr(w, "Remote not found")
		h.redirect(w, r, "/uploads/remotes")
		return
	}

	// Get available rclone remotes and providers from cache
	var availableRemotes []string
	var providers []rclone.Provider
	if h.rcloneMgr != nil && h.rcloneMgr.IsRunning() {
		availableRemotes = h.rcloneMgr.ConfiguredRemotes()
		providers = h.rcloneMgr.Providers()
		// Sort providers alphabetically
		sort.Slice(providers, func(i, j int) bool {
			return providers[i].Name < providers[j].Name
		})
	}

	h.render(w, r, "uploads.html", map[string]any{
		"Remote":           remote,
		"Tab":              "remotes",
		"AvailableRemotes": availableRemotes,
		"Providers":        providers,
	})
}

// RemoteUpdate handles remote update
func (h *Handlers) RemoteUpdate(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid remote ID")
		h.redirect(w, r, "/uploads/remotes")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/uploads/remotes/"+idStr)
		return
	}

	remote, err := h.db.GetRemote(id)
	if err != nil || remote == nil {
		h.flashErr(w, "Remote not found")
		h.redirect(w, r, "/uploads/remotes")
		return
	}

	remote.Name = r.FormValue("name")
	remote.RcloneRemote = r.FormValue("rclone_remote")
	remote.Enabled = r.FormValue("enabled") == "on"
	remote.TransferOptions = parseTransferOptions(r)

	if err := h.db.UpdateRemote(remote); err != nil {
		log.Error().Err(err).Int64("remote_id", id).Msg("Failed to update remote")
		h.flashErr(w, "Failed to update remote")
		h.redirect(w, r, "/uploads/remotes/"+idStr)
		return
	}

	log.Info().Int64("remote_id", id).Msg("Remote updated")
	h.flash(w, "Remote updated successfully")
	h.redirect(w, r, "/uploads/remotes")
}

// RemoteDelete handles remote deletion
func (h *Handlers) RemoteDelete(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid remote ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteRemote(id); err != nil {
		log.Error().Err(err).Int64("remote_id", id).Msg("Failed to delete remote")
		h.jsonError(w, "Failed to delete remote", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("remote_id", id).Msg("Remote deleted")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Remote deleted successfully")
	h.redirect(w, r, "/uploads/remotes")
}

// RemoteTest tests connection to a remote
func (h *Handlers) RemoteTest(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid remote ID", http.StatusBadRequest)
		return
	}

	remote, err := h.db.GetRemote(id)
	if err != nil || remote == nil {
		h.jsonError(w, "Remote not found", http.StatusNotFound)
		return
	}

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Rclone is not running",
		})
		return
	}

	// Test by listing remotes and checking if the remote exists
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	remotes, err := h.rcloneMgr.Client().ListRemotes(ctx)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Failed to list rclone remotes: " + err.Error(),
		})
		return
	}

	// Check if our remote is in the list
	found := false
	for _, r := range remotes {
		if r+":" == remote.RcloneRemote || r == remote.RcloneRemote {
			found = true
			break
		}
	}

	if !found {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": "Remote not found in rclone configuration",
		})
		return
	}

	log.Info().Int64("remote_id", id).Msg("Remote test successful")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "Remote is configured and accessible",
	})
}

// RemoteListAvailable lists available rclone remotes from config
func (h *Handlers) RemoteListAvailable(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		h.jsonError(w, "Rclone is not running", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	remotes, err := h.rcloneMgr.Client().ListRemotes(ctx)
	if err != nil {
		h.jsonError(w, "Failed to list remotes: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(remotes)
}

// RemoteBackendOptionsPartial returns HTML partial with backend options for adding to form
func (h *Handlers) RemoteBackendOptionsPartial(w http.ResponseWriter, r *http.Request) {
	backendName := chi.URLParam(r, "backend")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Rclone is not running</div>`))
		return
	}

	// Use cached providers
	providers := h.rcloneMgr.Providers()
	if len(providers) == 0 {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-yellow-500 text-sm">Provider data is loading, please wait...</div>`))
		return
	}

	// Find the requested provider
	var provider *rclone.Provider
	for i := range providers {
		if providers[i].Name == backendName || providers[i].Prefix == backendName {
			provider = &providers[i]
			break
		}
	}

	if provider == nil {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Backend not found: ` + backendName + `</div>`))
		return
	}

	h.renderPartial(w, "uploads.html", "backend_options_panel", map[string]any{
		"Provider": provider,
	})
}

// RemoteAddOptionPartial returns HTML partial for a single option input field
func (h *Handlers) RemoteAddOptionPartial(w http.ResponseWriter, r *http.Request) {
	backendName := chi.URLParam(r, "backend")
	optionName := chi.URLParam(r, "option")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Rclone is not running</div>`))
		return
	}

	// Use cached providers
	providers := h.rcloneMgr.Providers()

	// Find the requested provider and option
	var option *rclone.Option
	for i := range providers {
		if providers[i].Name == backendName || providers[i].Prefix == backendName {
			for j := range providers[i].Options {
				if providers[i].Options[j].Name == optionName {
					option = &providers[i].Options[j]
					break
				}
			}
			break
		}
	}

	if option == nil {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Option not found</div>`))
		return
	}

	h.renderPartial(w, "uploads.html", "option_input_field", map[string]any{
		"Backend": backendName,
		"Option":  option,
	})
}

// RemoteGlobalOptionsPartial returns HTML partial with global transfer options
func (h *Handlers) RemoteGlobalOptionsPartial(w http.ResponseWriter, r *http.Request) {
	category := chi.URLParam(r, "category")
	if category == "" {
		category = "main"
	}

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Rclone is not running</div>`))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	globalOptions, err := h.rcloneMgr.Client().GetOptions(ctx)
	if err != nil {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Error: ` + err.Error() + `</div>`))
		return
	}

	categoryOptions, ok := globalOptions[category]
	if !ok {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<div class="text-red-500 text-sm">Category not found: ` + category + `</div>`))
		return
	}

	h.renderPartial(w, "uploads.html", "global_options_panel", map[string]any{
		"Category": category,
		"Options":  categoryOptions,
	})
}

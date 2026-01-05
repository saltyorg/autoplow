package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/saltyorg/autoplow/internal/database"
)

// PlexAutoLangPage renders the main Plex Auto Languages page
func (h *Handlers) PlexAutoLangPage(w http.ResponseWriter, r *http.Request) {
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "overview"
	}

	// Get all Plex targets for display
	targets, _ := h.db.ListEnabledTargets()
	var plexTargets []*database.Target
	for _, t := range targets {
		if t.Type == database.TargetTypePlex {
			plexTargets = append(plexTargets, t)
		}
	}

	// Get enabled targets
	enabledTargets, _ := h.db.ListPlexAutoLanguagesEnabledTargets()

	// Get history entries with pagination
	pageSize := 20
	history, historyTotal, _ := h.db.ListAllPlexAutoLanguagesHistoryFiltered(0, "", pageSize, 0)
	historyUsers, _ := h.db.GetDistinctPlexUsersFromHistory()
	historyTotalPages := max((historyTotal+pageSize-1)/pageSize, 1)

	// Get preferences count per target
	preferencesCount := make(map[int64]int)
	for _, t := range enabledTargets {
		if _, total, err := h.db.ListPlexAutoLanguagesPreferencesFiltered(t.ID, "", 1, 0); err == nil {
			preferencesCount[t.ID] = total
		}
	}

	// Manager status
	var isRunning bool
	if h.plexAutoLangMgr != nil {
		isRunning = h.plexAutoLangMgr.IsRunning()
	}

	h.render(w, r, "plexautolang.html", map[string]any{
		"Tab":               tab,
		"PlexTargets":       plexTargets,
		"EnabledTargets":    enabledTargets,
		"History":           history,
		"HistoryPage":       1,
		"HistoryPageSize":   pageSize,
		"HistoryTotalPages": historyTotalPages,
		"HistoryTotal":      historyTotal,
		"HistoryUsers":      historyUsers,
		"PreferencesCount":  preferencesCount,
		"IsRunning":         isRunning,
	})
}

// PlexAutoLangToggleTarget toggles Plex Auto Languages enabled state for a target
func (h *Handlers) PlexAutoLangToggleTarget(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil || target == nil {
		h.jsonError(w, "Target not found", http.StatusNotFound)
		return
	}

	// Toggle the enabled state
	target.PlexAutoLanguagesEnabled = !target.PlexAutoLanguagesEnabled

	if err := h.db.UpdateTarget(target); err != nil {
		h.jsonError(w, "Failed to update target: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If enabling, create default config if not exists
	if target.PlexAutoLanguagesEnabled {
		config := database.DefaultPlexAutoLanguagesConfig(target.ID)
		config.Enabled = true
		_ = h.db.UpsertPlexAutoLanguagesConfig(&config)
	}

	// Trigger page reload to update UI state
	w.Header().Set("HX-Refresh", "true")
	h.jsonSuccess(w, "Target updated")
}

// PlexAutoLangConfigGet returns the config for a target
func (h *Handlers) PlexAutoLangConfigGet(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	config, err := h.db.GetPlexAutoLanguagesConfig(id)
	if err != nil {
		h.jsonError(w, "Failed to get config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(config)
}

// PlexAutoLangConfigUpdate updates the config for a target
func (h *Handlers) PlexAutoLangConfigUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid ID")
		h.redirect(w, r, "/plex-auto-languages?tab=settings")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/plex-auto-languages?tab=settings")
		return
	}

	config, err := h.db.GetPlexAutoLanguagesConfig(id)
	if err != nil {
		h.flashErr(w, "Failed to get config")
		h.redirect(w, r, "/plex-auto-languages?tab=settings")
		return
	}

	// Update fields from form
	config.Enabled = r.FormValue("enabled") == "on"
	config.UpdateLevel = database.PlexAutoLanguagesUpdateLevel(r.FormValue("update_level"))
	config.UpdateStrategy = database.PlexAutoLanguagesUpdateStrategy(r.FormValue("update_strategy"))
	config.TriggerOnPlay = r.FormValue("trigger_on_play") == "on"
	config.TriggerOnScan = r.FormValue("trigger_on_scan") == "on"
	config.TriggerOnActivity = r.FormValue("trigger_on_activity") == "on"
	config.Schedule = r.FormValue("schedule")

	if err := h.db.UpsertPlexAutoLanguagesConfig(config); err != nil {
		h.flashErr(w, "Failed to save config: "+err.Error())
		h.redirect(w, r, "/plex-auto-languages?tab=settings")
		return
	}

	h.flash(w, "Configuration saved successfully")
	h.redirect(w, r, "/plex-auto-languages?tab=settings")
}

// PlexAutoLangPreferences returns the preferences for a target
func (h *Handlers) PlexAutoLangPreferences(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	prefs, err := h.db.ListPlexAutoLanguagesPreferences(id)
	if err != nil {
		h.jsonError(w, "Failed to get preferences: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(prefs)
}

// PlexAutoLangPreferencesPartial returns the preferences table as HTML
func (h *Handlers) PlexAutoLangPreferencesPartial(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	// Parse pagination and filter params
	page := 1
	pageSize := 20
	userFilter := r.URL.Query().Get("user")
	targetFilterStr := r.URL.Query().Get("target")
	var targetFilter int64
	if targetFilterStr != "" {
		if parsed, err := strconv.ParseInt(targetFilterStr, 10, 64); err == nil && parsed >= 0 {
			targetFilter = parsed
		}
	}

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 {
			pageSize = parsed
		}
	}

	offset := (page - 1) * pageSize
	targets, _ := h.db.ListPlexAutoLanguagesEnabledTargets()

	targetIDForQuery := id
	if targetFilter > 0 {
		targetIDForQuery = targetFilter
	} else if targetFilter == 0 {
		targetIDForQuery = 0
	}

	prefs, total, _ := h.db.ListPlexAutoLanguagesPreferencesFiltered(targetIDForQuery, userFilter, pageSize, offset)
	users, _ := h.db.GetDistinctPlexUsersFromPreferences(targetIDForQuery)

	totalPages := max((total+pageSize-1)/pageSize, 1)

	h.renderPartial(w, "plexautolang.html", "preferences_section", map[string]any{
		"Preferences":  prefs,
		"TargetID":     id,
		"TargetFilter": targetFilter,
		"Targets":      targets,
		"Page":         page,
		"PageSize":     pageSize,
		"TotalPages":   totalPages,
		"Total":        total,
		"UserFilter":   userFilter,
		"Users":        users,
	})
}

// PlexAutoLangDeletePreference deletes a preference
func (h *Handlers) PlexAutoLangDeletePreference(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeletePlexAutoLanguagesPreference(id); err != nil {
		h.jsonError(w, "Failed to delete: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty response - HTMX will remove the row
	w.WriteHeader(http.StatusOK)
}

// PlexAutoLangHistory returns history entries
func (h *Handlers) PlexAutoLangHistory(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	history, err := h.db.ListAllPlexAutoLanguagesHistory(limit, offset)
	if err != nil {
		h.jsonError(w, "Failed to get history: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(history)
}

// PlexAutoLangHistoryPartial returns the history table as HTML
func (h *Handlers) PlexAutoLangHistoryPartial(w http.ResponseWriter, r *http.Request) {
	// Parse pagination and filter params
	page := 1
	pageSize := 20
	userFilter := r.URL.Query().Get("user")
	targetFilterStr := r.URL.Query().Get("target")
	var targetFilter int64
	if targetFilterStr != "" {
		if parsed, err := strconv.ParseInt(targetFilterStr, 10, 64); err == nil && parsed >= 0 {
			targetFilter = parsed
		}
	}

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 {
			pageSize = parsed
		}
	}

	offset := (page - 1) * pageSize
	history, total, _ := h.db.ListAllPlexAutoLanguagesHistoryFiltered(targetFilter, userFilter, pageSize, offset)
	users, _ := h.db.GetDistinctPlexUsersFromHistory()
	targets, _ := h.db.ListPlexAutoLanguagesEnabledTargets()

	totalPages := max((total+pageSize-1)/pageSize, 1)

	h.renderPartial(w, "plexautolang.html", "history_section", map[string]any{
		"History":      history,
		"Page":         page,
		"PageSize":     pageSize,
		"TotalPages":   totalPages,
		"Total":        total,
		"UserFilter":   userFilter,
		"TargetFilter": targetFilter,
		"Targets":      targets,
		"Users":        users,
	})
}

// PlexAutoLangClearHistory clears all history
func (h *Handlers) PlexAutoLangClearHistory(w http.ResponseWriter, r *http.Request) {
	// Get target ID if provided (to clear only that target's history)
	targetIDStr := r.URL.Query().Get("target_id")

	if targetIDStr != "" {
		targetID, err := strconv.ParseInt(targetIDStr, 10, 64)
		if err != nil {
			h.flashErr(w, "Invalid target ID")
			h.redirect(w, r, "/plex-auto-languages?tab=history")
			return
		}
		if err := h.db.ClearPlexAutoLanguagesHistory(targetID); err != nil {
			h.flashErr(w, "Failed to clear history: "+err.Error())
			h.redirect(w, r, "/plex-auto-languages?tab=history")
			return
		}
	} else {
		// Clear all history - need to get all enabled targets
		targets, _ := h.db.ListPlexAutoLanguagesEnabledTargets()
		for _, t := range targets {
			_ = h.db.ClearPlexAutoLanguagesHistory(t.ID)
		}
	}

	h.flash(w, "History cleared")
	h.redirect(w, r, "/plex-auto-languages?tab=history")
}

// PlexAutoLangStatus returns the current status as JSON
func (h *Handlers) PlexAutoLangStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.plexAutoLangMgr == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"running": false,
			"error":   "Manager not initialized",
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"running": h.plexAutoLangMgr.IsRunning(),
	})
}

// PlexAutoLangStatusPartial returns the status section as HTML
func (h *Handlers) PlexAutoLangStatusPartial(w http.ResponseWriter, r *http.Request) {
	var isRunning bool
	if h.plexAutoLangMgr != nil {
		isRunning = h.plexAutoLangMgr.IsRunning()
	}

	// Get enabled targets count
	enabledTargets, _ := h.db.ListPlexAutoLanguagesEnabledTargets()

	h.renderPartial(w, "plexautolang.html", "status_section", map[string]any{
		"IsRunning":      isRunning,
		"EnabledTargets": enabledTargets,
	})
}

// PlexAutoLangTargetConfigPartial returns the config form for a target as HTML
func (h *Handlers) PlexAutoLangTargetConfigPartial(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	target, err := h.db.GetTarget(id)
	if err != nil || target == nil {
		http.Error(w, "Target not found", http.StatusNotFound)
		return
	}

	config, _ := h.db.GetPlexAutoLanguagesConfig(id)

	h.renderPartial(w, "plexautolang.html", "target_config_form", map[string]any{
		"Target": target,
		"Config": config,
	})
}

// PlexAutoLangRecentActivityPartial returns the recent activity section as HTML
func (h *Handlers) PlexAutoLangRecentActivityPartial(w http.ResponseWriter, r *http.Request) {
	history, _ := h.db.ListAllPlexAutoLanguagesHistory(5, 0)

	h.renderPartial(w, "plexautolang.html", "recent_activity", map[string]any{
		"History": history,
	})
}

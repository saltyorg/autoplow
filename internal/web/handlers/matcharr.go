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

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/matcharr"
)

// MatcharrPage renders the main matcharr page
func (h *Handlers) MatcharrPage(w http.ResponseWriter, r *http.Request) {
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "overview"
	}

	var status matcharr.ManagerStatus
	if h.matcharrMgr != nil {
		status = h.matcharrMgr.Status()
	}

	// Get Arr instances
	arrs, _ := h.db.ListMatcharrArrs()

	// Count enabled Arrs
	enabledArrs := 0
	for _, arr := range arrs {
		if arr.Enabled {
			enabledArrs++
		}
	}

	// Get latest run
	latestRun, _ := h.db.GetLatestMatcharrRun()

	// Get actionable mismatches from latest run (pending + failed)
	var pendingMismatches []*database.MatcharrMismatch
	var arrGaps []*database.MatcharrGap
	var targetGaps []*database.MatcharrGap
	if latestRun != nil {
		pendingMismatches, _ = h.db.GetActionableMatcharrMismatches(latestRun.ID)
		arrGaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceArr)
		targetGaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceTarget)
	}
	if pendingMismatches == nil {
		pendingMismatches = []*database.MatcharrMismatch{}
	}
	if arrGaps == nil {
		arrGaps = []*database.MatcharrGap{}
	}
	if targetGaps == nil {
		targetGaps = []*database.MatcharrGap{}
	}

	// Get run history
	runs, _ := h.db.ListMatcharrRuns(10, 0)

	// Get fix history (fixed/skipped/failed mismatches)
	fixHistory, _ := h.db.GetMatcharrFixHistory(50, 0)

	// Get all enabled targets for display in settings (to toggle matcharr on/off)
	targets, _ := h.db.ListEnabledTargets()

	// Get matcharr-enabled targets (for CanRun check)
	matcharrTargets, _ := h.db.ListMatcharrEnabledTargets()

	h.render(w, r, "matcharr.html", map[string]any{
		"Tab":               tab,
		"Status":            status,
		"Arrs":              arrs,
		"EnabledArrs":       enabledArrs,
		"LatestRun":         latestRun,
		"PendingMismatches": pendingMismatches,
		"ArrGaps":           arrGaps,
		"TargetGaps":        targetGaps,
		"Runs":              runs,
		"FixHistory":        fixHistory,
		"Targets":           targets,
		"MatcharrTargets":   len(matcharrTargets),
		"CanRun":            enabledArrs > 0 && len(matcharrTargets) > 0,
	})
}

// MatcharrArrNew renders the new Arr form
func (h *Handlers) MatcharrArrNew(w http.ResponseWriter, r *http.Request) {
	h.renderPartial(w, "matcharr.html", "arr_form", map[string]any{
		"IsNew": true,
		"Arr":   &database.MatcharrArr{Enabled: true},
	})
}

// MatcharrArrCreate creates a new Arr instance
func (h *Handlers) MatcharrArrCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	arr := &database.MatcharrArr{
		Name:    r.FormValue("name"),
		Type:    database.ArrType(r.FormValue("type")),
		URL:     strings.TrimSuffix(r.FormValue("url"), "/"),
		APIKey:  r.FormValue("api_key"),
		Enabled: r.FormValue("enabled") == "on",
	}

	// Parse path mappings
	arr.PathMappings = parseMatcharrPathMappings(r)

	// Validate
	if arr.Name == "" || arr.URL == "" || arr.APIKey == "" {
		h.flashErr(w, "Name, URL, and API key are required")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	if err := h.db.CreateMatcharrArr(arr); err != nil {
		h.flashErr(w, "Failed to create Arr instance: "+err.Error())
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	h.flash(w, "Arr instance created successfully")
	h.redirect(w, r, "/matcharr?tab=arrs")
}

// MatcharrArrEdit renders the edit form for an Arr instance
func (h *Handlers) MatcharrArrEdit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid ID")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	arr, err := h.db.GetMatcharrArr(id)
	if err != nil || arr == nil {
		h.flashErr(w, "Arr instance not found")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	h.renderPartial(w, "matcharr.html", "arr_form", map[string]any{
		"IsNew": false,
		"Arr":   arr,
	})
}

// MatcharrArrUpdate updates an Arr instance
func (h *Handlers) MatcharrArrUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid ID")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	arr, err := h.db.GetMatcharrArr(id)
	if err != nil || arr == nil {
		h.flashErr(w, "Arr instance not found")
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	arr.Name = r.FormValue("name")
	arr.Type = database.ArrType(r.FormValue("type"))
	arr.URL = strings.TrimSuffix(r.FormValue("url"), "/")
	arr.APIKey = r.FormValue("api_key")
	arr.Enabled = r.FormValue("enabled") == "on"
	arr.PathMappings = parseMatcharrPathMappings(r)

	if err := h.db.UpdateMatcharrArr(arr); err != nil {
		h.flashErr(w, "Failed to update Arr instance: "+err.Error())
		h.redirect(w, r, "/matcharr?tab=arrs")
		return
	}

	h.flash(w, "Arr instance updated successfully")
	h.redirect(w, r, "/matcharr?tab=arrs")
}

// MatcharrArrDelete deletes an Arr instance
func (h *Handlers) MatcharrArrDelete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteMatcharrArr(id); err != nil {
		h.jsonError(w, "Failed to delete: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If this was an htmx request, redirect back to the Arrs tab to refresh the list.
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/matcharr?tab=arrs")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.flash(w, "Arr instance deleted")
	h.redirect(w, r, "/matcharr?tab=arrs")
}

// MatcharrArrTest tests connection to an Arr instance
func (h *Handlers) MatcharrArrTest(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	arr, err := h.db.GetMatcharrArr(id)
	if err != nil || arr == nil {
		h.jsonError(w, "Arr instance not found", http.StatusNotFound)
		return
	}

	client := matcharr.NewArrClient(arr.URL, arr.APIKey, arr.Type)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := client.TestConnection(ctx); err != nil {
		h.jsonError(w, "Connection failed: "+err.Error(), http.StatusOK)
		return
	}

	h.jsonSuccess(w, "Connection successful")
}

// MatcharrArrTestRaw tests connection with raw credentials (before saving)
func (h *Handlers) MatcharrArrTestRaw(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.jsonError(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	url := strings.TrimSuffix(r.FormValue("url"), "/")
	apiKey := r.FormValue("api_key")
	arrType := database.ArrType(r.FormValue("type"))

	if url == "" || apiKey == "" {
		h.jsonError(w, "URL and API key are required", http.StatusBadRequest)
		return
	}

	client := matcharr.NewArrClient(url, apiKey, arrType)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := client.TestConnection(ctx); err != nil {
		h.jsonError(w, "Connection failed: "+err.Error(), http.StatusOK)
		return
	}

	h.jsonSuccess(w, "Connection successful")
}

// MatcharrRunNow triggers a manual comparison run
func (h *Handlers) MatcharrRunNow(w http.ResponseWriter, r *http.Request) {
	if h.matcharrMgr == nil {
		http.Error(w, "Matcharr manager not initialized", http.StatusInternalServerError)
		return
	}

	// Run comparison in background (manual runs don't auto-fix)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		_, _ = h.matcharrMgr.RunComparison(ctx, false, "manual")
	}()

	// Return 204 No Content - SSE events will update the UI
	w.WriteHeader(http.StatusNoContent)
}

// MatcharrRunStatus returns the current run status as JSON
func (h *Handlers) MatcharrRunStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.matcharrMgr == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"running": false,
			"error":   "Manager not initialized",
		})
		return
	}

	status := h.matcharrMgr.Status()
	_ = json.NewEncoder(w).Encode(status)
}

// MatcharrMismatchesPartial returns the mismatches table as HTML
func (h *Handlers) MatcharrMismatchesPartial(w http.ResponseWriter, r *http.Request) {
	latestRun, _ := h.db.GetLatestMatcharrRun()
	var mismatches []*database.MatcharrMismatch
	if latestRun != nil {
		// Only show actionable rows (pending + failed) so fixed/skipped don't reappear after SSE refresh
		mismatches, _ = h.db.GetActionableMatcharrMismatches(latestRun.ID)
	}
	if mismatches == nil {
		mismatches = []*database.MatcharrMismatch{}
	}

	h.renderPartial(w, "matcharr.html", "mismatches_block", map[string]any{
		"Mismatches": mismatches,
		"LatestRun":  latestRun,
	})
}

// MatcharrArrGapsPartial returns the Missing on Server section as HTML
func (h *Handlers) MatcharrArrGapsPartial(w http.ResponseWriter, r *http.Request) {
	latestRun, _ := h.db.GetLatestMatcharrRun()

	var gaps []*database.MatcharrGap
	if latestRun != nil {
		gaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceArr)
	}
	if gaps == nil {
		gaps = []*database.MatcharrGap{}
	}

	h.renderPartial(w, "matcharr.html", "arr_gaps_section", map[string]any{
		"Rows":      gaps,
		"IsPartial": true,
	})
}

// MatcharrTargetGapsPartial returns the Missing in Arrs section as HTML
func (h *Handlers) MatcharrTargetGapsPartial(w http.ResponseWriter, r *http.Request) {
	latestRun, _ := h.db.GetLatestMatcharrRun()

	var gaps []*database.MatcharrGap
	if latestRun != nil {
		gaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceTarget)
	}
	if gaps == nil {
		gaps = []*database.MatcharrGap{}
	}

	h.renderPartial(w, "matcharr.html", "target_gaps_section", map[string]any{
		"Rows":      gaps,
		"IsPartial": true,
	})
}

// MatcharrFixOne fixes a single mismatch
func (h *Handlers) MatcharrFixOne(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if h.matcharrMgr == nil {
		h.jsonError(w, "Manager not initialized", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := h.matcharrMgr.FixMismatchByID(ctx, id); err != nil {
		h.jsonError(w, "Fix failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get updated actionable count for OOB update (pending + failed)
	var pendingCount int
	if latestRun, _ := h.db.GetLatestMatcharrRun(); latestRun != nil {
		if mismatches, _ := h.db.GetActionableMatcharrMismatches(latestRun.ID); mismatches != nil {
			pendingCount = len(mismatches)
		}
	}

	// Return OOB swap to update the tab badge count (row is removed via empty primary swap)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<span id="mismatches-count" hx-swap-oob="true">%d</span>`, pendingCount)
}

// MatcharrSkipMismatch marks a mismatch as skipped
func (h *Handlers) MatcharrSkipMismatch(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if h.matcharrMgr == nil {
		h.jsonError(w, "Manager not initialized", http.StatusInternalServerError)
		return
	}

	if err := h.matcharrMgr.SkipMismatch(id); err != nil {
		h.jsonError(w, "Failed to skip: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get updated actionable count for OOB update (pending + failed)
	var pendingCount int
	if latestRun, _ := h.db.GetLatestMatcharrRun(); latestRun != nil {
		if mismatches, _ := h.db.GetActionableMatcharrMismatches(latestRun.ID); mismatches != nil {
			pendingCount = len(mismatches)
		}
	}

	// Return OOB swap to update the tab badge count (row is removed via empty primary swap)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<span id="mismatches-count" hx-swap-oob="true">%d</span>`, pendingCount)
}

// MatcharrFixAll fixes all pending mismatches
func (h *Handlers) MatcharrFixAll(w http.ResponseWriter, r *http.Request) {
	if h.matcharrMgr == nil {
		h.flashErr(w, "Manager not initialized")
		h.redirect(w, r, "/matcharr?tab=mismatches")
		return
	}

	// Run fix all in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		_, _ = h.matcharrMgr.FixAllPending(ctx)
	}()

	h.flash(w, "Fixing all pending mismatches...")
	h.redirect(w, r, "/matcharr?tab=mismatches")
}

// MatcharrSettingsUpdate updates matcharr settings
func (h *Handlers) MatcharrSettingsUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/matcharr?tab=settings")
		return
	}

	if h.matcharrMgr == nil {
		h.flashErr(w, "Manager not initialized")
		h.redirect(w, r, "/matcharr?tab=settings")
		return
	}

	config := matcharr.ManagerConfig{
		Enabled:  r.FormValue("enabled") == "on",
		Schedule: r.FormValue("schedule"),
		AutoFix:  r.FormValue("auto_fix") == "on",
	}

	if delayStr := r.FormValue("delay_between_fixes"); delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil {
			config.DelayBetweenFixes = time.Duration(delay) * time.Second
		}
	}

	if err := h.matcharrMgr.UpdateConfig(config); err != nil {
		h.flashErr(w, "Failed to update settings: "+err.Error())
		h.redirect(w, r, "/matcharr?tab=settings")
		return
	}

	h.flash(w, "Settings updated successfully")
	h.redirect(w, r, "/matcharr?tab=settings")
}

// MatcharrRunsPartial returns the runs history table as HTML
func (h *Handlers) MatcharrRunsPartial(w http.ResponseWriter, r *http.Request) {
	runs, _ := h.db.ListMatcharrRuns(20, 0)

	h.renderPartial(w, "matcharr.html", "runs_table", map[string]any{
		"Runs": runs,
	})
}

// MatcharrStatusPartial returns the status section as HTML
func (h *Handlers) MatcharrStatusPartial(w http.ResponseWriter, r *http.Request) {
	var status matcharr.ManagerStatus
	if h.matcharrMgr != nil {
		status = h.matcharrMgr.Status()
	}

	// Get actionable mismatches count for OOB update of tab badge (pending + failed)
	var pendingMismatchesCount int
	if latestRun, _ := h.db.GetLatestMatcharrRun(); latestRun != nil {
		if mismatches, _ := h.db.GetActionableMatcharrMismatches(latestRun.ID); mismatches != nil {
			pendingMismatchesCount = len(mismatches)
		}
	}

	h.renderPartial(w, "matcharr.html", "status_section", map[string]any{
		"Status":                 status,
		"PendingMismatchesCount": pendingMismatchesCount,
		"IsPartial":              true,
	})
}

// MatcharrLastRunPartial returns the last run section as HTML
func (h *Handlers) MatcharrLastRunPartial(w http.ResponseWriter, r *http.Request) {
	latestRun, _ := h.db.GetLatestMatcharrRun()

	h.renderPartial(w, "matcharr.html", "last_run_section", map[string]any{
		"LatestRun": latestRun,
	})
}

// MatcharrQuickActionsPartial returns the quick actions section as HTML
func (h *Handlers) MatcharrQuickActionsPartial(w http.ResponseWriter, r *http.Request) {
	var status matcharr.ManagerStatus
	if h.matcharrMgr != nil {
		status = h.matcharrMgr.Status()
	}

	// Get actionable mismatches from latest run (pending + failed)
	var pendingMismatches []*database.MatcharrMismatch
	if latestRun, _ := h.db.GetLatestMatcharrRun(); latestRun != nil {
		pendingMismatches, _ = h.db.GetActionableMatcharrMismatches(latestRun.ID)
	}
	if pendingMismatches == nil {
		pendingMismatches = []*database.MatcharrMismatch{}
	}

	h.renderPartial(w, "matcharr.html", "quick_actions", map[string]any{
		"Status":            status,
		"PendingMismatches": pendingMismatches,
	})
}

// MatcharrTabCounts returns OOB updates for tab badge counts so they stay fresh regardless of which tab is open
func (h *Handlers) MatcharrTabCounts(w http.ResponseWriter, r *http.Request) {
	arrs, _ := h.db.ListMatcharrArrs()
	matcharrTargets, _ := h.db.ListMatcharrEnabledTargets()

	var arrGapsCount, targetGapsCount, pendingMismatchesCount int
	if latestRun, _ := h.db.GetLatestMatcharrRun(); latestRun != nil {
		if gaps, _ := h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceArr); gaps != nil {
			arrGapsCount = len(gaps)
		}
		if gaps, _ := h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceTarget); gaps != nil {
			targetGapsCount = len(gaps)
		}
		if mismatches, _ := h.db.GetActionableMatcharrMismatches(latestRun.ID); mismatches != nil {
			pendingMismatchesCount = len(mismatches)
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<span id="arrs-count" hx-swap-oob="true">%d</span>`, len(arrs))
	fmt.Fprintf(w, `<span id="matcharr-targets-count" hx-swap-oob="true">%d</span>`, len(matcharrTargets))
	fmt.Fprintf(w, `<span id="arr-gaps-count" hx-swap-oob="true">%d</span>`, arrGapsCount)
	fmt.Fprintf(w, `<span id="target-gaps-count" hx-swap-oob="true">%d</span>`, targetGapsCount)
	fmt.Fprintf(w, `<span id="mismatches-count" hx-swap-oob="true">%d</span>`, pendingMismatchesCount)
}

// MatcharrToggleTarget toggles matcharr enabled state for a target
func (h *Handlers) MatcharrToggleTarget(w http.ResponseWriter, r *http.Request) {
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

	// Toggle the matcharr enabled state
	target.MatcharrEnabled = !target.MatcharrEnabled

	if err := h.db.UpdateTarget(target); err != nil {
		h.jsonError(w, "Failed to update target: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Trigger page reload to update UI state
	w.Header().Set("HX-Refresh", "true")
	h.jsonSuccess(w, "Target updated")
}

// MatcharrClearHistory clears all matcharr run history
func (h *Handlers) MatcharrClearHistory(w http.ResponseWriter, r *http.Request) {
	if err := h.db.ClearMatcharrHistory(); err != nil {
		h.flashErr(w, "Failed to clear history: "+err.Error())
		h.redirect(w, r, "/matcharr?tab=history")
		return
	}

	h.flash(w, "Run history cleared")
	h.redirect(w, r, "/matcharr?tab=history")
}

// MatcharrRunDetails returns the details of a specific run as a full page
func (h *Handlers) MatcharrRunDetails(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flash(w, "Invalid run ID")
		h.redirect(w, r, "/matcharr?tab=history")
		return
	}

	run, err := h.db.GetMatcharrRun(id)
	if err != nil || run == nil {
		h.flash(w, "Run not found")
		h.redirect(w, r, "/matcharr?tab=history")
		return
	}

	// Get mismatches for this run
	mismatches, _ := h.db.ListMatcharrMismatches(id)

	h.render(w, r, "matcharr_run.html", map[string]any{
		"Run":        run,
		"Mismatches": mismatches,
	})
}

// parseMatcharrPathMappings parses path mapping form fields for matcharr
func parseMatcharrPathMappings(r *http.Request) []database.MatcharrPathMapping {
	var mappings []database.MatcharrPathMapping

	arrPaths := r.Form["path_mapping_arr[]"]
	serverPaths := r.Form["path_mapping_server[]"]

	for i := 0; i < len(arrPaths) && i < len(serverPaths); i++ {
		arrPath := strings.TrimSpace(arrPaths[i])
		serverPath := strings.TrimSpace(serverPaths[i])
		if arrPath != "" && serverPath != "" {
			mappings = append(mappings, database.MatcharrPathMapping{
				ArrPath:    arrPath,
				ServerPath: serverPath,
			})
		}
	}

	return mappings
}

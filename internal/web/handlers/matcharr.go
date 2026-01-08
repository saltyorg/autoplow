package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/matcharr"
)

type mismatchTargetOption struct {
	ID   int64
	Name string
}

type mismatchArrOption struct {
	ID   int64
	Name string
}

type gapOption struct {
	ID   int64
	Name string
}

type fileMismatchDetailRow struct {
	ID              int64
	SeasonNumber    int
	EpisodeNumber   int
	ArrFileName     string
	TargetFileNames string
	ArrFilePath     string
	TargetFilePaths string
}

type fileMismatchRow struct {
	ID               int64
	ArrID            int64
	TargetID         int64
	ArrName          string
	TargetName       string
	MediaTitle       string
	ArrMediaID       int64
	TargetMetadataID string
	TargetItemPath   string
	MismatchCount    int
	Details          []fileMismatchDetailRow
}

// MatcharrPage renders the main matcharr page
func (h *Handlers) MatcharrPage(w http.ResponseWriter, r *http.Request) {
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "overview"
	}
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))

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
	filteredMismatches := filterMatcharrMismatches(pendingMismatches, arrIDFilter, targetFilterID)
	targetOptions := buildMismatchTargetOptions(pendingMismatches)
	arrOptions := buildMismatchArrOptions(pendingMismatches)
	filtersApplied := arrIDFilter > 0 || targetFilterID > 0
	if arrGaps == nil {
		arrGaps = []*database.MatcharrGap{}
	}
	if targetGaps == nil {
		targetGaps = []*database.MatcharrGap{}
	}

	filteredArrGaps := filterMatcharrGaps(arrGaps, arrIDFilter, targetFilterID)
	filteredTargetGaps := filterMatcharrGaps(targetGaps, arrIDFilter, targetFilterID)
	arrGapArrOptions := buildGapArrOptions(arrGaps)
	arrGapTargetOptions := buildGapTargetOptions(arrGaps)
	targetGapArrOptions := buildGapArrOptions(targetGaps)
	targetGapTargetOptions := buildGapTargetOptions(targetGaps)

	// File mismatches and ignores
	var fileMismatchRows []fileMismatchRow
	var fileMismatchArrOptions []gapOption
	var fileMismatchTargetOptions []gapOption
	var fileIgnoreRows []*database.MatcharrFileIgnore
	var fileIgnoreArrOptions []gapOption
	var fileIgnoreTargetOptions []gapOption
	if latestRun != nil {
		fileMismatches, _ := h.db.ListMatcharrFileMismatches(latestRun.ID)
		filteredFileMismatches := filterMatcharrFileMismatches(fileMismatches, arrIDFilter, targetFilterID)
		fileMismatchRows = buildFileMismatchRows(filteredFileMismatches)
		fileMismatchArrOptions = buildFileMismatchArrOptions(fileMismatches)
		fileMismatchTargetOptions = buildFileMismatchTargetOptions(fileMismatches)
	}
	if fileMismatchRows == nil {
		fileMismatchRows = []fileMismatchRow{}
	}

	fileIgnores, _ := h.db.ListMatcharrFileIgnores()
	if fileIgnores != nil {
		fileIgnoreRows = filterMatcharrFileIgnores(fileIgnores, arrIDFilter, targetFilterID)
		fileIgnoreArrOptions = buildFileIgnoreArrOptions(fileIgnores)
		fileIgnoreTargetOptions = buildFileIgnoreTargetOptions(fileIgnores)
	}
	if fileIgnoreRows == nil {
		fileIgnoreRows = []*database.MatcharrFileIgnore{}
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
		"Tab":                       tab,
		"Status":                    status,
		"Arrs":                      arrs,
		"EnabledArrs":               enabledArrs,
		"LatestRun":                 latestRun,
		"PendingMismatches":         pendingMismatches,
		"FilteredMismatches":        filteredMismatches,
		"ArrGaps":                   filteredArrGaps,
		"TargetGaps":                filteredTargetGaps,
		"FileMismatchRows":          fileMismatchRows,
		"FileMismatchCount":         len(fileMismatchRows),
		"FileIgnoreRows":            fileIgnoreRows,
		"FileIgnoreCount":           len(fileIgnoreRows),
		"Runs":                      runs,
		"FixHistory":                fixHistory,
		"Targets":                   targets,
		"MatcharrTargets":           len(matcharrTargets),
		"CanRun":                    enabledArrs > 0 && len(matcharrTargets) > 0,
		"SelectedArrID":             arrIDFilter,
		"SelectedTargetID":          targetFilterID,
		"TargetOptions":             targetOptions,
		"ArrOptions":                arrOptions,
		"ArrGapArrOptions":          arrGapArrOptions,
		"ArrGapTargetOptions":       arrGapTargetOptions,
		"TargetGapArrOptions":       targetGapArrOptions,
		"TargetGapTargetOptions":    targetGapTargetOptions,
		"FileMismatchArrOptions":    fileMismatchArrOptions,
		"FileMismatchTargetOptions": fileMismatchTargetOptions,
		"FileIgnoreArrOptions":      fileIgnoreArrOptions,
		"FileIgnoreTargetOptions":   fileIgnoreTargetOptions,
		"FiltersApplied":            filtersApplied,
	})
}

// MatcharrArrNew renders the new Arr form
func (h *Handlers) MatcharrArrNew(w http.ResponseWriter, r *http.Request) {
	h.renderPartial(w, "matcharr.html", "arr_form", map[string]any{
		"IsNew": true,
		"Arr":   &database.MatcharrArr{Enabled: true, FileConcurrency: matcharrDefaultFileConcurrency},
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
		Name:            r.FormValue("name"),
		Type:            database.ArrType(r.FormValue("type")),
		URL:             strings.TrimSuffix(r.FormValue("url"), "/"),
		APIKey:          r.FormValue("api_key"),
		Enabled:         r.FormValue("enabled") == "on",
		FileConcurrency: parseMatcharrFileConcurrency(r),
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
	arr.FileConcurrency = parseMatcharrFileConcurrency(r)
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
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))
	latestRun, _ := h.db.GetLatestMatcharrRun()
	var mismatches []*database.MatcharrMismatch
	if latestRun != nil {
		// Only show actionable rows (pending + failed) so fixed/skipped don't reappear after SSE refresh
		mismatches, _ = h.db.GetActionableMatcharrMismatches(latestRun.ID)
	}
	if mismatches == nil {
		mismatches = []*database.MatcharrMismatch{}
	}
	filteredMismatches := filterMatcharrMismatches(mismatches, arrIDFilter, targetFilterID)
	targetOptions := buildMismatchTargetOptions(mismatches)
	arrOptions := buildMismatchArrOptions(mismatches)
	filtersApplied := arrIDFilter > 0 || targetFilterID > 0

	h.renderPartial(w, "matcharr.html", "mismatches_block", map[string]any{
		"Mismatches":         mismatches,
		"PendingMismatches":  mismatches,
		"FilteredMismatches": filteredMismatches,
		"LatestRun":          latestRun,
		"SelectedArrID":      arrIDFilter,
		"SelectedTargetID":   targetFilterID,
		"TargetOptions":      targetOptions,
		"ArrOptions":         arrOptions,
		"FiltersApplied":     filtersApplied,
	})
}

func (h *Handlers) renderMatcharrArrGapsSection(w http.ResponseWriter, arrIDFilter int64, targetFilterID int64) {
	latestRun, _ := h.db.GetLatestMatcharrRun()
	scanningEnabled := h.isScanningEnabled()

	var gaps []*database.MatcharrGap
	if latestRun != nil {
		gaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceArr)
	}
	if gaps == nil {
		gaps = []*database.MatcharrGap{}
	}

	filteredGaps := filterMatcharrGaps(gaps, arrIDFilter, targetFilterID)
	arrOptions := buildGapArrOptions(gaps)
	targetOptions := buildGapTargetOptions(gaps)

	h.renderPartial(w, "matcharr.html", "arr_gaps_section", map[string]any{
		"Rows":             filteredGaps,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
		"ScanningEnabled":  scanningEnabled,
	})
}

// MatcharrArrGapsPartial returns the Missing on Server section as HTML
func (h *Handlers) MatcharrArrGapsPartial(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))
	h.renderMatcharrArrGapsSection(w, arrIDFilter, targetFilterID)
}

// MatcharrTargetGapsPartial returns the Missing in Arrs section as HTML
func (h *Handlers) MatcharrTargetGapsPartial(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))
	latestRun, _ := h.db.GetLatestMatcharrRun()
	scanningEnabled := h.isScanningEnabled()

	var gaps []*database.MatcharrGap
	if latestRun != nil {
		gaps, _ = h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceTarget)
	}
	if gaps == nil {
		gaps = []*database.MatcharrGap{}
	}

	filteredGaps := filterMatcharrGaps(gaps, arrIDFilter, targetFilterID)
	arrOptions := buildGapArrOptions(gaps)
	targetOptions := buildGapTargetOptions(gaps)

	h.renderPartial(w, "matcharr.html", "target_gaps_section", map[string]any{
		"Rows":             filteredGaps,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
		"ScanningEnabled":  scanningEnabled,
	})
}

// MatcharrFileMismatchesPartial returns the File Mismatches section as HTML.
func (h *Handlers) MatcharrFileMismatchesPartial(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))

	latestRun, _ := h.db.GetLatestMatcharrRun()
	scanningEnabled := h.isScanningEnabled()

	var mismatches []*database.MatcharrFileMismatch
	if latestRun != nil {
		mismatches, _ = h.db.ListMatcharrFileMismatches(latestRun.ID)
	}
	if mismatches == nil {
		mismatches = []*database.MatcharrFileMismatch{}
	}

	filtered := filterMatcharrFileMismatches(mismatches, arrIDFilter, targetFilterID)
	rows := buildFileMismatchRows(filtered)
	arrOptions := buildFileMismatchArrOptions(mismatches)
	targetOptions := buildFileMismatchTargetOptions(mismatches)

	h.renderPartial(w, "matcharr.html", "file_mismatches_section", map[string]any{
		"Rows":             rows,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
		"ScanningEnabled":  scanningEnabled,
	})
}

// MatcharrFileIgnoresPartial returns the File Ignores section as HTML.
func (h *Handlers) MatcharrFileIgnoresPartial(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.URL.Query().Get("arr_id"))
	targetFilterID := parseIDFilter(r.URL.Query().Get("target_id"))

	ignores, _ := h.db.ListMatcharrFileIgnores()
	if ignores == nil {
		ignores = []*database.MatcharrFileIgnore{}
	}

	filtered := filterMatcharrFileIgnores(ignores, arrIDFilter, targetFilterID)
	arrOptions := buildFileIgnoreArrOptions(ignores)
	targetOptions := buildFileIgnoreTargetOptions(ignores)

	h.renderPartial(w, "matcharr.html", "file_ignores_section", map[string]any{
		"Rows":             filtered,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
	})
}

// MatcharrFileMismatchScan triggers a media server scan for a file mismatch row.
func (h *Handlers) MatcharrFileMismatchScan(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if h.targetsMgr == nil {
		h.jsonError(w, "Targets manager not initialized", http.StatusInternalServerError)
		return
	}

	if !h.isScanningEnabled() {
		h.jsonError(w, "Scanning is disabled", http.StatusBadRequest)
		return
	}

	mismatch, err := h.db.GetMatcharrFileMismatch(id)
	if err != nil {
		h.jsonError(w, "Failed to load file mismatch", http.StatusInternalServerError)
		return
	}
	if mismatch == nil {
		h.jsonError(w, "File mismatch not found", http.StatusNotFound)
		return
	}

	scanPath := strings.TrimSpace(mismatch.TargetItemPath)
	pathFromFile := false
	if scanPath == "" {
		scanPath = strings.TrimSpace(mismatch.ArrFilePath)
		pathFromFile = scanPath != ""
	}
	if scanPath == "" {
		scanPath = strings.TrimSpace(strings.Split(mismatch.TargetFilePaths, ",")[0])
		pathFromFile = scanPath != ""
	}
	if scanPath == "" {
		h.jsonError(w, "No scan path available for this item", http.StatusBadRequest)
		return
	}
	if pathFromFile {
		if dir := filepath.Dir(scanPath); dir != "." && dir != "/" && dir != "" && scanPath != dir {
			scanPath = dir
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), config.GetTimeouts().ScanOperation)
	defer cancel()

	result, _ := h.targetsMgr.ScanTarget(ctx, mismatch.TargetID, scanPath, "")
	if !result.Success {
		h.jsonError(w, "Scan failed: "+result.Error, http.StatusInternalServerError)
		return
	}

	message := "Scan triggered on " + result.Message
	if result.Error != "" {
		message = "Scan skipped on " + result.Message + ": " + result.Error
	}
	h.jsonSuccess(w, message)
}

// MatcharrFileMismatchRecheck rechecks a single media item for filename mismatches.
func (h *Handlers) MatcharrFileMismatchRecheck(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.FormValue("arr_id"))
	targetFilterID := parseIDFilter(r.FormValue("target_id"))

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if h.matcharrMgr == nil {
		h.jsonError(w, "Manager not initialized", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), config.GetTimeouts().ScanOperation)
	defer cancel()

	if _, err := h.matcharrMgr.RecheckFileMismatch(ctx, id); err != nil {
		h.jsonError(w, "Recheck failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	latestRun, _ := h.db.GetLatestMatcharrRun()
	var mismatches []*database.MatcharrFileMismatch
	if latestRun != nil {
		mismatches, _ = h.db.ListMatcharrFileMismatches(latestRun.ID)
	}
	if mismatches == nil {
		mismatches = []*database.MatcharrFileMismatch{}
	}
	filtered := filterMatcharrFileMismatches(mismatches, arrIDFilter, targetFilterID)
	rows := buildFileMismatchRows(filtered)
	arrOptions := buildFileMismatchArrOptions(mismatches)
	targetOptions := buildFileMismatchTargetOptions(mismatches)

	h.renderPartial(w, "matcharr.html", "file_mismatches_section", map[string]any{
		"Rows":             rows,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
		"ScanningEnabled":  h.isScanningEnabled(),
	})
}

// MatcharrFileMismatchIgnore ignores a single filename mismatch entry.
func (h *Handlers) MatcharrFileMismatchIgnore(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.FormValue("arr_id"))
	targetFilterID := parseIDFilter(r.FormValue("target_id"))

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	mismatch, err := h.db.GetMatcharrFileMismatch(id)
	if err != nil {
		h.jsonError(w, "Failed to load file mismatch", http.StatusInternalServerError)
		return
	}
	if mismatch == nil {
		h.jsonError(w, "File mismatch not found", http.StatusNotFound)
		return
	}

	ignore := &database.MatcharrFileIgnore{
		ArrID:         mismatch.ArrID,
		TargetID:      mismatch.TargetID,
		ArrType:       mismatch.ArrType,
		ArrName:       mismatch.ArrName,
		TargetName:    mismatch.TargetName,
		MediaTitle:    mismatch.MediaTitle,
		ArrMediaID:    mismatch.ArrMediaID,
		SeasonNumber:  mismatch.SeasonNumber,
		EpisodeNumber: mismatch.EpisodeNumber,
		ArrFileName:   mismatch.ArrFileName,
		ArrFilePath:   mismatch.ArrFilePath,
	}

	if err := h.db.CreateMatcharrFileIgnore(ignore); err != nil {
		h.jsonError(w, "Failed to ignore file mismatch: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_ = h.db.DeleteMatcharrFileMismatch(id)

	latestRun, _ := h.db.GetLatestMatcharrRun()
	var mismatches []*database.MatcharrFileMismatch
	if latestRun != nil {
		mismatches, _ = h.db.ListMatcharrFileMismatches(latestRun.ID)
	}
	if mismatches == nil {
		mismatches = []*database.MatcharrFileMismatch{}
	}
	filtered := filterMatcharrFileMismatches(mismatches, arrIDFilter, targetFilterID)
	rows := buildFileMismatchRows(filtered)
	arrOptions := buildFileMismatchArrOptions(mismatches)
	targetOptions := buildFileMismatchTargetOptions(mismatches)

	h.renderPartial(w, "matcharr.html", "file_mismatches_section", map[string]any{
		"Rows":             rows,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
		"ScanningEnabled":  h.isScanningEnabled(),
	})
}

// MatcharrFileIgnoreRemove deletes a filename ignore rule.
func (h *Handlers) MatcharrFileIgnoreRemove(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.FormValue("arr_id"))
	targetFilterID := parseIDFilter(r.FormValue("target_id"))

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if err := h.db.DeleteMatcharrFileIgnore(id); err != nil {
		h.jsonError(w, "Failed to remove ignore: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ignores, _ := h.db.ListMatcharrFileIgnores()
	if ignores == nil {
		ignores = []*database.MatcharrFileIgnore{}
	}
	filtered := filterMatcharrFileIgnores(ignores, arrIDFilter, targetFilterID)
	arrOptions := buildFileIgnoreArrOptions(ignores)
	targetOptions := buildFileIgnoreTargetOptions(ignores)

	h.renderPartial(w, "matcharr.html", "file_ignores_section", map[string]any{
		"Rows":             filtered,
		"ArrOptions":       arrOptions,
		"TargetOptions":    targetOptions,
		"SelectedArrID":    arrIDFilter,
		"SelectedTargetID": targetFilterID,
		"IsPartial":        true,
	})
}

// MatcharrGapScan triggers a media server scan for a single gap row
func (h *Handlers) MatcharrGapScan(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	if h.targetsMgr == nil {
		h.jsonError(w, "Targets manager not initialized", http.StatusInternalServerError)
		return
	}

	if !h.isScanningEnabled() {
		h.jsonError(w, "Scanning is disabled", http.StatusBadRequest)
		return
	}

	gap, err := h.db.GetMatcharrGap(id)
	if err != nil {
		h.jsonError(w, "Failed to load gap", http.StatusInternalServerError)
		return
	}
	if gap == nil {
		h.jsonError(w, "Gap not found", http.StatusNotFound)
		return
	}
	if gap.TargetID == 0 {
		h.jsonError(w, "Target not found for this gap", http.StatusBadRequest)
		return
	}

	scanPath := matcharrGapScanPath(gap)
	if scanPath == "" {
		h.jsonError(w, "No scan path available for this gap", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), config.GetTimeouts().ScanOperation)
	defer cancel()

	result, _ := h.targetsMgr.ScanTarget(ctx, gap.TargetID, scanPath, "")
	if !result.Success {
		h.jsonError(w, "Scan failed: "+result.Error, http.StatusInternalServerError)
		return
	}

	message := "Scan triggered on " + result.Message
	if result.Error != "" {
		message = "Scan skipped on " + result.Message + ": " + result.Error
	}
	h.jsonSuccess(w, message)
}

// MatcharrGapRecheck checks whether a missing item now exists on the media server.
func (h *Handlers) MatcharrGapRecheck(w http.ResponseWriter, r *http.Request) {
	arrIDFilter := parseIDFilter(r.FormValue("arr_id"))
	targetFilterID := parseIDFilter(r.FormValue("target_id"))

	success := false
	message := ""

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		message = "Invalid ID"
	} else if h.matcharrMgr == nil {
		message = "Manager not initialized"
	} else {
		gap, err := h.db.GetMatcharrGap(id)
		if err != nil {
			message = "Failed to load gap"
		} else if gap == nil {
			message = "Gap not found"
		} else if gap.Source != database.MatcharrGapSourceArr {
			message = "Recheck is only available for Missing on Server gaps"
		} else if gap.TargetID == 0 {
			message = "Target not found for this gap"
		} else {
			checkPath := matcharrGapScanPath(gap)
			if checkPath == "" {
				message = "No path available for this gap"
			} else {
				ctx, cancel := context.WithTimeout(r.Context(), config.GetTimeouts().ScanOperation)
				defer cancel()

				found, err := h.matcharrMgr.CheckTargetPath(ctx, gap.TargetID, checkPath)
				if err != nil {
					message = "Recheck failed: " + err.Error()
				} else {
					targetName := gap.TargetName
					if targetName == "" {
						targetName = fmt.Sprintf("Target #%d", gap.TargetID)
					}
					if found {
						if err := h.db.DeleteMatcharrGap(gap.ID); err != nil {
							message = "Recheck found item but failed to update list: " + err.Error()
						} else {
							success = true
							message = "Found on " + targetName + ", removed from list"
						}
					} else {
						message = "Still missing on " + targetName
					}
				}
			}
		}
	}

	if message != "" {
		payload := map[string]any{
			"matcharrGapRecheck": map[string]any{
				"success": success,
				"message": message,
			},
		}
		if data, err := json.Marshal(payload); err == nil {
			w.Header().Set("HX-Trigger", string(data))
		}
	}

	h.renderMatcharrArrGapsSection(w, arrIDFilter, targetFilterID)
}

// MatcharrArrGapsScanAll triggers scans for all filtered Missing on Server gaps
func (h *Handlers) MatcharrArrGapsScanAll(w http.ResponseWriter, r *http.Request) {
	if h.targetsMgr == nil {
		h.jsonError(w, "Targets manager not initialized", http.StatusInternalServerError)
		return
	}

	if !h.isScanningEnabled() {
		h.jsonError(w, "Scanning is disabled", http.StatusBadRequest)
		return
	}

	arrIDFilter := parseIDFilter(r.FormValue("arr_id"))
	targetFilterID := parseIDFilter(r.FormValue("target_id"))
	latestRun, _ := h.db.GetLatestMatcharrRun()
	if latestRun == nil {
		h.jsonError(w, "No matcharr run data available", http.StatusBadRequest)
		return
	}

	gaps, _ := h.db.GetMatcharrGaps(latestRun.ID, database.MatcharrGapSourceArr)
	if len(gaps) == 0 {
		h.jsonSuccess(w, "No gaps to scan")
		return
	}

	filteredGaps := filterMatcharrGaps(gaps, arrIDFilter, targetFilterID)
	if len(filteredGaps) == 0 {
		h.jsonSuccess(w, "No gaps to scan")
		return
	}

	type scanTask struct {
		TargetID   int64
		TargetName string
		Path       string
	}

	seen := make(map[string]struct{})
	tasks := make([]scanTask, 0, len(filteredGaps))
	for _, gap := range filteredGaps {
		if gap.TargetID == 0 {
			continue
		}
		scanPath := matcharrGapScanPath(gap)
		if scanPath == "" {
			continue
		}
		targetName := gap.TargetName
		if targetName == "" {
			targetName = fmt.Sprintf("Target #%d", gap.TargetID)
		}
		key := fmt.Sprintf("%d:%s", gap.TargetID, scanPath)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		tasks = append(tasks, scanTask{
			TargetID:   gap.TargetID,
			TargetName: targetName,
			Path:       scanPath,
		})
	}

	if len(tasks) == 0 {
		h.jsonSuccess(w, "No gaps to scan")
		return
	}

	go func(tasks []scanTask) {
		for _, task := range tasks {
			ctx, cancel := context.WithTimeout(context.Background(), config.GetTimeouts().ScanOperation)
			result, _ := h.targetsMgr.ScanTarget(ctx, task.TargetID, task.Path, "")
			cancel()

			if !result.Success {
				log.Warn().
					Str("target", task.TargetName).
					Str("path", task.Path).
					Str("error", result.Error).
					Msg("Matcharr scan failed")
				continue
			}
			if result.Error != "" {
				log.Debug().
					Str("target", task.TargetName).
					Str("path", task.Path).
					Str("note", result.Error).
					Msg("Matcharr scan skipped")
			} else {
				log.Info().
					Str("target", task.TargetName).
					Str("path", task.Path).
					Msg("Matcharr scan triggered")
			}
		}
	}(tasks)

	h.jsonSuccess(w, fmt.Sprintf("Scan all started for %d item(s)", len(tasks)))
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
	var fileMismatchCount, fileIgnoreCount int
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
		if fileMismatches, _ := h.db.ListMatcharrFileMismatches(latestRun.ID); fileMismatches != nil {
			fileMismatchCount = len(buildFileMismatchRows(fileMismatches))
		}
	}
	if ignores, _ := h.db.ListMatcharrFileIgnores(); ignores != nil {
		fileIgnoreCount = len(ignores)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<span id="arrs-count" hx-swap-oob="true">%d</span>`, len(arrs))
	fmt.Fprintf(w, `<span id="matcharr-targets-count" hx-swap-oob="true">%d</span>`, len(matcharrTargets))
	fmt.Fprintf(w, `<span id="arr-gaps-count" hx-swap-oob="true">%d</span>`, arrGapsCount)
	fmt.Fprintf(w, `<span id="target-gaps-count" hx-swap-oob="true">%d</span>`, targetGapsCount)
	fmt.Fprintf(w, `<span id="mismatches-count" hx-swap-oob="true">%d</span>`, pendingMismatchesCount)
	fmt.Fprintf(w, `<span id="file-mismatches-count" hx-swap-oob="true">%d</span>`, fileMismatchCount)
	fmt.Fprintf(w, `<span id="file-ignores-count" hx-swap-oob="true">%d</span>`, fileIgnoreCount)
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

// MatcharrUpdateTargetIgnorePaths updates matcharr ignore paths for a target
func (h *Handlers) MatcharrUpdateTargetIgnorePaths(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.jsonError(w, "Invalid form data", http.StatusBadRequest)
		return
	}

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

	mappings, mappingErrors := parsePathMappings(r)
	if len(mappingErrors) > 0 {
		h.jsonError(w, strings.Join(mappingErrors, "; "), http.StatusBadRequest)
		return
	}

	target.Config.PathMappings = mappings
	target.Config.MatcharrExcludePaths = parseMatcharrExcludePaths(r)
	target.Config.MatcharrFileConcurrency = parseMatcharrFileConcurrency(r)

	if err := h.db.UpdateTarget(target); err != nil {
		h.jsonError(w, "Failed to update target: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Invalidate library cache for this target since path mappings may have changed
	if err := h.db.DeleteCachedLibraries(id); err != nil {
		log.Warn().Err(err).Int64("target_id", id).Msg("Failed to invalidate library cache")
	}

	// Refresh page to reflect updated paths
	w.Header().Set("HX-Refresh", "true")
	h.jsonSuccess(w, "Target settings updated")
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

func parseIDFilter(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if id, err := strconv.ParseInt(value, 10, 64); err == nil && id > 0 {
		return id
	}
	return 0
}

func (h *Handlers) isScanningEnabled() bool {
	if val, _ := h.db.GetSetting("scanning.enabled"); val == "false" {
		return false
	}
	return true
}

func matcharrGapScanPath(gap *database.MatcharrGap) string {
	if gap == nil {
		return ""
	}
	if path := strings.TrimSpace(gap.TargetPath); path != "" {
		return path
	}
	return strings.TrimSpace(gap.ArrPath)
}

func filterMatcharrMismatches(mismatches []*database.MatcharrMismatch, arrID int64, targetID int64) []*database.MatcharrMismatch {
	if arrID == 0 && targetID == 0 {
		return mismatches
	}

	filtered := make([]*database.MatcharrMismatch, 0, len(mismatches))
	for _, mismatch := range mismatches {
		if arrID > 0 && mismatch.ArrID != arrID {
			continue
		}
		if targetID > 0 && mismatch.TargetID != targetID {
			continue
		}
		filtered = append(filtered, mismatch)
	}

	return filtered
}

func buildMismatchTargetOptions(mismatches []*database.MatcharrMismatch) []mismatchTargetOption {
	targets := make(map[int64]string)
	for _, mismatch := range mismatches {
		if mismatch.TargetID == 0 {
			continue
		}
		if _, exists := targets[mismatch.TargetID]; exists {
			continue
		}
		name := mismatch.TargetName
		if name == "" {
			name = fmt.Sprintf("Target #%d", mismatch.TargetID)
		}
		targets[mismatch.TargetID] = name
	}

	options := make([]mismatchTargetOption, 0, len(targets))
	for id, name := range targets {
		options = append(options, mismatchTargetOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func buildMismatchArrOptions(mismatches []*database.MatcharrMismatch) []mismatchArrOption {
	arrs := make(map[int64]string)
	for _, mismatch := range mismatches {
		if mismatch.ArrID == 0 {
			continue
		}
		if _, exists := arrs[mismatch.ArrID]; exists {
			continue
		}
		name := mismatch.ArrName
		if name == "" {
			name = fmt.Sprintf("Arr #%d", mismatch.ArrID)
		}
		arrs[mismatch.ArrID] = name
	}

	options := make([]mismatchArrOption, 0, len(arrs))
	for id, name := range arrs {
		options = append(options, mismatchArrOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func filterMatcharrGaps(gaps []*database.MatcharrGap, arrID int64, targetID int64) []*database.MatcharrGap {
	if arrID == 0 && targetID == 0 {
		return gaps
	}

	filtered := make([]*database.MatcharrGap, 0, len(gaps))
	for _, gap := range gaps {
		if arrID > 0 && gap.ArrID != arrID {
			continue
		}
		if targetID > 0 && gap.TargetID != targetID {
			continue
		}
		filtered = append(filtered, gap)
	}

	return filtered
}

func buildGapArrOptions(gaps []*database.MatcharrGap) []gapOption {
	arrs := make(map[int64]string)
	for _, gap := range gaps {
		if gap.ArrID == 0 {
			continue
		}
		if _, exists := arrs[gap.ArrID]; exists {
			continue
		}
		name := gap.ArrName
		if name == "" {
			name = fmt.Sprintf("Arr #%d", gap.ArrID)
		}
		arrs[gap.ArrID] = name
	}

	options := make([]gapOption, 0, len(arrs))
	for id, name := range arrs {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func buildGapTargetOptions(gaps []*database.MatcharrGap) []gapOption {
	targets := make(map[int64]string)
	for _, gap := range gaps {
		if gap.TargetID == 0 {
			continue
		}
		if _, exists := targets[gap.TargetID]; exists {
			continue
		}
		name := gap.TargetName
		if name == "" {
			name = fmt.Sprintf("Target #%d", gap.TargetID)
		}
		targets[gap.TargetID] = name
	}

	options := make([]gapOption, 0, len(targets))
	for id, name := range targets {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func filterMatcharrFileMismatches(mismatches []*database.MatcharrFileMismatch, arrID int64, targetID int64) []*database.MatcharrFileMismatch {
	if arrID == 0 && targetID == 0 {
		return mismatches
	}

	filtered := make([]*database.MatcharrFileMismatch, 0, len(mismatches))
	for _, mismatch := range mismatches {
		if arrID > 0 && mismatch.ArrID != arrID {
			continue
		}
		if targetID > 0 && mismatch.TargetID != targetID {
			continue
		}
		filtered = append(filtered, mismatch)
	}

	return filtered
}

func buildFileMismatchRows(mismatches []*database.MatcharrFileMismatch) []fileMismatchRow {
	rowsMap := make(map[string]*fileMismatchRow)
	order := make([]string, 0)

	for _, mismatch := range mismatches {
		key := fmt.Sprintf("%d:%d:%d", mismatch.ArrID, mismatch.TargetID, mismatch.ArrMediaID)
		row, exists := rowsMap[key]
		if !exists {
			row = &fileMismatchRow{
				ID:               mismatch.ID,
				ArrID:            mismatch.ArrID,
				TargetID:         mismatch.TargetID,
				ArrName:          mismatch.ArrName,
				TargetName:       mismatch.TargetName,
				MediaTitle:       mismatch.MediaTitle,
				ArrMediaID:       mismatch.ArrMediaID,
				TargetMetadataID: mismatch.TargetMetadataID,
				TargetItemPath:   mismatch.TargetItemPath,
			}
			rowsMap[key] = row
			order = append(order, key)
		}
		if row.TargetItemPath == "" {
			row.TargetItemPath = mismatch.TargetItemPath
		}

		row.MismatchCount++
		row.Details = append(row.Details, fileMismatchDetailRow{
			ID:              mismatch.ID,
			SeasonNumber:    mismatch.SeasonNumber,
			EpisodeNumber:   mismatch.EpisodeNumber,
			ArrFileName:     mismatch.ArrFileName,
			TargetFileNames: mismatch.TargetFileNames,
			ArrFilePath:     mismatch.ArrFilePath,
			TargetFilePaths: mismatch.TargetFilePaths,
		})
	}

	rows := make([]fileMismatchRow, 0, len(order))
	for _, key := range order {
		row := rowsMap[key]
		sort.Slice(row.Details, func(i, j int) bool {
			if row.Details[i].SeasonNumber != row.Details[j].SeasonNumber {
				return row.Details[i].SeasonNumber < row.Details[j].SeasonNumber
			}
			if row.Details[i].EpisodeNumber != row.Details[j].EpisodeNumber {
				return row.Details[i].EpisodeNumber < row.Details[j].EpisodeNumber
			}
			return strings.ToLower(row.Details[i].ArrFileName) < strings.ToLower(row.Details[j].ArrFileName)
		})
		rows = append(rows, *row)
	}

	return rows
}

func buildFileMismatchArrOptions(mismatches []*database.MatcharrFileMismatch) []gapOption {
	arrs := make(map[int64]string)
	for _, mismatch := range mismatches {
		if mismatch.ArrID == 0 {
			continue
		}
		if _, exists := arrs[mismatch.ArrID]; exists {
			continue
		}
		name := mismatch.ArrName
		if name == "" {
			name = fmt.Sprintf("Arr #%d", mismatch.ArrID)
		}
		arrs[mismatch.ArrID] = name
	}

	options := make([]gapOption, 0, len(arrs))
	for id, name := range arrs {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func buildFileMismatchTargetOptions(mismatches []*database.MatcharrFileMismatch) []gapOption {
	targets := make(map[int64]string)
	for _, mismatch := range mismatches {
		if mismatch.TargetID == 0 {
			continue
		}
		if _, exists := targets[mismatch.TargetID]; exists {
			continue
		}
		name := mismatch.TargetName
		if name == "" {
			name = fmt.Sprintf("Target #%d", mismatch.TargetID)
		}
		targets[mismatch.TargetID] = name
	}

	options := make([]gapOption, 0, len(targets))
	for id, name := range targets {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func filterMatcharrFileIgnores(ignores []*database.MatcharrFileIgnore, arrID int64, targetID int64) []*database.MatcharrFileIgnore {
	if arrID == 0 && targetID == 0 {
		return ignores
	}

	filtered := make([]*database.MatcharrFileIgnore, 0, len(ignores))
	for _, ignore := range ignores {
		if arrID > 0 && ignore.ArrID != arrID {
			continue
		}
		if targetID > 0 && ignore.TargetID != targetID {
			continue
		}
		filtered = append(filtered, ignore)
	}

	return filtered
}

func buildFileIgnoreArrOptions(ignores []*database.MatcharrFileIgnore) []gapOption {
	arrs := make(map[int64]string)
	for _, ignore := range ignores {
		if ignore.ArrID == 0 {
			continue
		}
		if _, exists := arrs[ignore.ArrID]; exists {
			continue
		}
		name := ignore.ArrName
		if name == "" {
			name = fmt.Sprintf("Arr #%d", ignore.ArrID)
		}
		arrs[ignore.ArrID] = name
	}

	options := make([]gapOption, 0, len(arrs))
	for id, name := range arrs {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

func buildFileIgnoreTargetOptions(ignores []*database.MatcharrFileIgnore) []gapOption {
	targets := make(map[int64]string)
	for _, ignore := range ignores {
		if ignore.TargetID == 0 {
			continue
		}
		if _, exists := targets[ignore.TargetID]; exists {
			continue
		}
		name := ignore.TargetName
		if name == "" {
			name = fmt.Sprintf("Target #%d", ignore.TargetID)
		}
		targets[ignore.TargetID] = name
	}

	options := make([]gapOption, 0, len(targets))
	for id, name := range targets {
		options = append(options, gapOption{ID: id, Name: name})
	}

	sort.Slice(options, func(i, j int) bool {
		if options[i].Name == options[j].Name {
			return options[i].ID < options[j].ID
		}
		return strings.ToLower(options[i].Name) < strings.ToLower(options[j].Name)
	})

	return options
}

// parseMatcharrExcludePaths extracts ignore paths for Matcharr
func parseMatcharrExcludePaths(r *http.Request) []string {
	rawPaths := r.Form["matcharr_exclude_paths[]"]
	var paths []string
	for _, path := range rawPaths {
		path = strings.TrimSpace(path)
		if path != "" {
			paths = append(paths, path)
		}
	}
	return paths
}

const matcharrDefaultFileConcurrency = 4

func parseMatcharrFileConcurrency(r *http.Request) int {
	raw := strings.TrimSpace(r.FormValue("matcharr_file_concurrency"))
	if raw == "" {
		return matcharrDefaultFileConcurrency
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return matcharrDefaultFileConcurrency
	}
	return value
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

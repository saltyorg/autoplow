package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// UploadsPage renders the uploads queue page
func (h *Handlers) UploadsPage(w http.ResponseWriter, r *http.Request) {
	// Parse pagination params
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	pageSize := 20 // Default page size
	if ps, err := strconv.Atoi(r.URL.Query().Get("pageSize")); err == nil && ps > 0 {
		// Allow specific page sizes
		switch ps {
		case 10, 20, 50, 100:
			pageSize = ps
		}
	}

	offset := (page - 1) * pageSize

	// Get paginated uploads (sorted: active first)
	uploads, err := h.db.ListUploadsPaginated(pageSize, offset)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list uploads")
		h.flashErr(w, "Failed to load uploads")
		h.redirect(w, r, "/")
		return
	}

	// Get total count for pagination
	totalCount, _ := h.db.CountAllUploads()
	totalPages := max((totalCount+pageSize-1)/pageSize, 1)

	// Get counts (completed uploads are not shown in queue, they go to history)
	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)
	failedCount, _ := h.db.CountUploads(database.UploadStatusFailed)

	// Get upload manager stats if available
	var uploadStats map[string]any
	if h.uploadMgr != nil {
		stats := h.uploadMgr.Stats()
		uploadStats = map[string]any{
			"active_uploads":  stats.ActiveUploads,
			"pending_uploads": stats.PendingUploads,
			"queued_uploads":  stats.QueuedUploads,
			"current_speed":   stats.CurrentSpeed,
		}
	}

	// Get active transfers for display
	var activeTransfers any
	var isPaused bool
	if h.uploadMgr != nil {
		activeTransfers = h.uploadMgr.GetActiveTransfers()
		isPaused = h.uploadMgr.IsPaused()
	}

	h.render(w, r, "uploads.html", map[string]any{
		"Uploads":         uploads,
		"Tab":             "queue",
		"ActiveCount":     activeCount,
		"QueuedCount":     queuedCount,
		"PendingCount":    pendingCount,
		"FailedCount":     failedCount,
		"UploadStats":     uploadStats,
		"Page":            page,
		"PageSize":        pageSize,
		"TotalPages":      totalPages,
		"TotalCount":      totalCount,
		"ActiveTransfers": activeTransfers,
		"IsPaused":        isPaused,
	})
}

// UploadCancel cancels an active or queued upload
func (h *Handlers) UploadCancel(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid upload ID", http.StatusBadRequest)
		return
	}

	if h.uploadMgr == nil {
		h.jsonError(w, "Upload manager not available", http.StatusServiceUnavailable)
		return
	}

	if err := h.uploadMgr.CancelUpload(id); err != nil {
		log.Error().Err(err).Int64("upload_id", id).Msg("Failed to cancel upload")
		h.jsonError(w, "Failed to cancel upload", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("upload_id", id).Msg("Upload cancelled")

	// For HTMX requests, return empty response to remove the row
	if r.Header.Get("HX-Request") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.flash(w, "Upload cancelled")
	h.redirect(w, r, "/uploads")
}

// UploadRetry retries a failed upload
func (h *Handlers) UploadRetry(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid upload ID", http.StatusBadRequest)
		return
	}

	if h.uploadMgr == nil {
		h.jsonError(w, "Upload manager not available", http.StatusServiceUnavailable)
		return
	}

	if err := h.uploadMgr.RetryUpload(id); err != nil {
		log.Error().Err(err).Int64("upload_id", id).Msg("Failed to retry upload")
		h.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Info().Int64("upload_id", id).Msg("Upload retry queued")

	if r.Header.Get("HX-Request") == "true" {
		h.jsonSuccess(w, "Upload queued for retry")
		return
	}

	h.flash(w, "Upload queued for retry")
	h.redirect(w, r, "/uploads")
}

// UploadClearCompleted clears completed uploads from queue
func (h *Handlers) UploadClearCompleted(w http.ResponseWriter, r *http.Request) {
	count, err := h.db.DeleteCompletedUploads()
	if err != nil {
		log.Error().Err(err).Msg("Failed to clear completed uploads")
		h.jsonError(w, "Failed to clear completed uploads", http.StatusInternalServerError)
		return
	}

	log.Info().Int64("count", count).Msg("Cleared completed uploads")
	h.flash(w, "Cleared completed uploads")
	h.redirect(w, r, "/uploads")
}

// UploadProgress returns progress for HTMX polling (partial template)
func (h *Handlers) UploadProgress(w http.ResponseWriter, r *http.Request) {
	uploads, err := h.db.ListActiveUploads()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list active uploads")
		h.jsonError(w, "Failed to get upload progress", http.StatusInternalServerError)
		return
	}

	// Calculate progress percentage for each upload
	type uploadProgress struct {
		ID         int64  `json:"id"`
		LocalPath  string `json:"local_path"`
		RemoteName string `json:"remote_name"`
		Progress   int64  `json:"progress"`
		Total      int64  `json:"total"`
		Percentage int    `json:"percentage"`
	}

	var progress []uploadProgress
	for _, u := range uploads {
		p := uploadProgress{
			ID:         u.ID,
			LocalPath:  u.LocalPath,
			RemoteName: u.RemoteName,
			Progress:   u.ProgressBytes,
		}
		if u.SizeBytes != nil {
			p.Total = *u.SizeBytes
			if p.Total > 0 {
				p.Percentage = int(p.Progress * 100 / p.Total)
			}
		}
		progress = append(progress, p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(progress)
}

// UploadStats returns current upload statistics
func (h *Handlers) UploadStats(w http.ResponseWriter, r *http.Request) {
	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)

	stats := map[string]any{
		"active":  activeCount,
		"queued":  queuedCount,
		"pending": pendingCount,
	}

	if h.uploadMgr != nil {
		mgrstats := h.uploadMgr.Stats()
		stats["current_speed"] = mgrstats.CurrentSpeed
		stats["paused"] = h.uploadMgr.IsPaused()
		stats["database_error"] = h.uploadMgr.HasDatabaseError()
		if dbErr := h.uploadMgr.GetDatabaseError(); dbErr != nil {
			stats["database_error_message"] = dbErr.Error()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// UploadQueueStatsPartial returns the upload queue stats partial
func (h *Handlers) UploadQueueStatsPartial(w http.ResponseWriter, r *http.Request) {
	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)

	data := map[string]any{
		"ActiveCount":  activeCount,
		"QueuedCount":  queuedCount,
		"PendingCount": pendingCount,
	}
	h.renderPartial(w, "uploads.html", "upload_queue_stats", data)
}

// UploadPause pauses the upload manager
func (h *Handlers) UploadPause(w http.ResponseWriter, r *http.Request) {
	if h.uploadMgr == nil {
		h.flashErr(w, "Upload manager not available")
		h.redirect(w, r, "/uploads")
		return
	}

	h.uploadMgr.Pause()
	log.Info().Msg("Upload manager paused via API")

	h.flash(w, "Upload manager paused")
	h.redirect(w, r, "/uploads")
}

// UploadResume resumes the upload manager
func (h *Handlers) UploadResume(w http.ResponseWriter, r *http.Request) {
	if h.uploadMgr == nil {
		h.flashErr(w, "Upload manager not available")
		h.redirect(w, r, "/uploads")
		return
	}

	// Check if there's a database error - cannot resume in that case
	if h.uploadMgr.HasDatabaseError() {
		h.flashErr(w, "Cannot resume: database error requires application restart")
		h.redirect(w, r, "/uploads")
		return
	}

	h.uploadMgr.Resume()
	log.Info().Msg("Upload manager resumed via API")

	h.flash(w, "Upload manager resumed")
	h.redirect(w, r, "/uploads")
}

// UploadPauseBtn returns the context-aware pause/resume button HTML fragment
func (h *Handlers) UploadPauseBtn(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	if h.uploadMgr == nil {
		w.Write([]byte(`<button type="button" disabled class="inline-flex items-center px-3 py-2 border border-gray-300 dark:border-gray-600 shadow-sm text-sm leading-4 font-medium rounded-md text-gray-400 dark:text-gray-500 bg-gray-100 dark:bg-gray-700 cursor-not-allowed">Not Available</button>`))
		return
	}

	if h.uploadMgr.HasDatabaseError() {
		// Database error - hide button completely
		return
	}

	if h.uploadMgr.IsPaused() {
		// Show Resume button
		w.Write([]byte(`<button hx-post="/uploads/resume" hx-swap="none" hx-on::after-request="htmx.trigger('#upload-pause-controls', 'sse-refresh'); htmx.trigger('#upload-status-banner', 'sse-refresh')" class="inline-flex items-center px-3 py-2 border border-green-300 dark:border-green-600 shadow-sm text-sm leading-4 font-medium rounded-md text-green-700 dark:text-green-300 bg-green-50 dark:bg-green-900/30 hover:bg-green-100 dark:hover:bg-green-900/50">
			<svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
			Resume
		</button>`))
	} else {
		// Show Pause button
		w.Write([]byte(`<button hx-post="/uploads/pause" hx-swap="none" hx-on::after-request="htmx.trigger('#upload-pause-controls', 'sse-refresh'); htmx.trigger('#upload-status-banner', 'sse-refresh')" class="inline-flex items-center px-3 py-2 border border-yellow-300 dark:border-yellow-600 shadow-sm text-sm leading-4 font-medium rounded-md text-yellow-700 dark:text-yellow-300 bg-yellow-50 dark:bg-yellow-900/30 hover:bg-yellow-100 dark:hover:bg-yellow-900/50">
			<svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
			Pause
		</button>`))
	}
}

// UploadStatusBanner returns the status banner HTML fragment
func (h *Handlers) UploadStatusBanner(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	if h.uploadMgr == nil {
		return
	}

	if h.uploadMgr.HasDatabaseError() {
		errMsg := "Unknown error"
		if dbErr := h.uploadMgr.GetDatabaseError(); dbErr != nil {
			errMsg = dbErr.Error()
		}
		w.Write([]byte(`<div class="mt-3 p-3 rounded-md bg-red-50 dark:bg-red-900/30">
			<p class="text-sm font-medium text-red-800 dark:text-red-200">Database Error: ` + errMsg + `. Application restart required.</p>
		</div>`))
		return
	}

	if h.uploadMgr.IsPaused() {
		w.Write([]byte(`<div class="mt-3 p-3 rounded-md bg-yellow-50 dark:bg-yellow-900/30">
			<p class="text-sm font-medium text-yellow-800 dark:text-yellow-200">Upload manager is paused. New uploads will not start until resumed.</p>
		</div>`))
		return
	}

	// Not paused, no error - return empty (no banner)
}

// UploadsQueuePartial returns the upload queue table body (for SSE refresh)
func (h *Handlers) UploadsQueuePartial(w http.ResponseWriter, r *http.Request) {
	// Parse pagination params (same as main page)
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	pageSize := 20
	if ps, err := strconv.Atoi(r.URL.Query().Get("pageSize")); err == nil && ps > 0 {
		switch ps {
		case 10, 20, 50, 100:
			pageSize = ps
		}
	}

	offset := (page - 1) * pageSize

	uploads, err := h.db.ListUploadsPaginated(pageSize, offset)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list uploads for partial")
		http.Error(w, "Failed to load uploads", http.StatusInternalServerError)
		return
	}
	h.renderPartial(w, "uploads.html", "upload_queue", uploads)
}

// UploadsActivePartial returns the active transfers section (for SSE refresh)
func (h *Handlers) UploadsActivePartial(w http.ResponseWriter, r *http.Request) {
	var activeTransfers any
	if h.uploadMgr != nil {
		activeTransfers = h.uploadMgr.GetActiveTransfers()
	}
	h.renderPartial(w, "uploads.html", "active_transfers", activeTransfers)
}

// UploadQueuePaginationPartial returns the pagination section (for SSE refresh)
func (h *Handlers) UploadQueuePaginationPartial(w http.ResponseWriter, r *http.Request) {
	// Parse pagination params (same as main page)
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	pageSize := 20
	if ps, err := strconv.Atoi(r.URL.Query().Get("pageSize")); err == nil && ps > 0 {
		switch ps {
		case 10, 20, 50, 100:
			pageSize = ps
		}
	}

	// Get total count for pagination
	totalCount, _ := h.db.CountAllUploads()
	totalPages := max((totalCount+pageSize-1)/pageSize, 1)

	data := map[string]any{
		"Page":       page,
		"PageSize":   pageSize,
		"TotalPages": totalPages,
		"TotalCount": totalCount,
	}
	h.renderPartial(w, "uploads.html", "upload_queue_pagination", data)
}

// UploadHistoryPage renders the upload history page
func (h *Handlers) UploadHistoryPage(w http.ResponseWriter, r *http.Request) {
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	limit := 50
	offset := (page - 1) * limit

	history, err := h.db.ListUploadHistory(limit, offset)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list upload history")
		h.flashErr(w, "Failed to load upload history")
		h.redirect(w, r, "/uploads")
		return
	}

	totalCount, _ := h.db.CountUploadHistory()
	totalPages := (totalCount + limit - 1) / limit

	totalUploads, totalBytes, _ := h.db.GetUploadStats()

	h.render(w, r, "uploads.html", map[string]any{
		"History":      history,
		"Tab":          "history",
		"Page":         page,
		"TotalPages":   totalPages,
		"TotalUploads": totalUploads,
		"TotalBytes":   totalBytes,
	})
}

package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
)

// ScanHistoryItem represents a scan for the history page
type ScanHistoryItem struct {
	ID          int64
	Path        string
	TriggerName string
	Status      string
	CreatedAt   time.Time
	CompletedAt *time.Time
	Error       string
}

func (h *Handlers) buildHistoryScansData(r *http.Request) (map[string]any, error) {
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	status := r.URL.Query().Get("status")
	limit := 50
	offset := (page - 1) * limit

	scans, err := h.db.ListScansFiltered(status, limit, offset)
	if err != nil {
		return nil, err
	}

	triggers := make(map[int64]string)
	triggerList, _ := h.db.ListTriggers()
	for _, t := range triggerList {
		triggers[t.ID] = t.Name
	}

	var items []ScanHistoryItem
	for _, s := range scans {
		item := ScanHistoryItem{
			ID:          s.ID,
			Path:        s.Path,
			Status:      string(s.Status),
			CreatedAt:   s.CreatedAt,
			CompletedAt: s.CompletedAt,
			Error:       s.LastError,
		}
		if s.TriggerID != nil {
			if name, ok := triggers[*s.TriggerID]; ok {
				item.TriggerName = name
			} else {
				item.TriggerName = "Unknown"
			}
		} else {
			item.TriggerName = "Manual"
		}
		items = append(items, item)
	}

	totalCount, _ := h.db.CountScansFiltered(status)
	totalPages := (totalCount + limit - 1) / limit

	stats, _ := h.db.GetScanStatsByStatus()

	return map[string]any{
		"Scans":      items,
		"Page":       page,
		"TotalPages": totalPages,
		"TotalCount": totalCount,
		"Status":     status,
		"Stats":      stats,
		"HasPrev":    page > 1,
		"HasNext":    page < totalPages,
		"PrevPage":   page - 1,
		"NextPage":   page + 1,
	}, nil
}

// HistoryScans renders the scan history page
func (h *Handlers) HistoryScans(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildHistoryScansData(r)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list scans")
		h.flashErr(w, "Failed to load scan history")
		h.redirect(w, r, "/")
		return
	}

	h.render(w, r, "history_scans.html", data)
}

// HistoryScansStatsPartial returns the scan stats section for HTMX refresh
func (h *Handlers) HistoryScansStatsPartial(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetScanStatsByStatus()
	if err != nil {
		log.Error().Err(err).Msg("Failed to load scan stats")
		stats = nil
	}

	h.renderPartial(w, "history_scans.html", "scan_history_stats", map[string]any{
		"Stats": stats,
	})
}

// HistoryScansTablePartial returns the scan history table for HTMX refresh
func (h *Handlers) HistoryScansTablePartial(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildHistoryScansData(r)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load scan history")
		http.Error(w, "Failed to load scan history", http.StatusInternalServerError)
		return
	}
	h.renderPartial(w, "history_scans.html", "scan_history_table", data)
}

// RetryScan queues a new scan using the same parameters as a historical scan.
func (h *Handlers) RetryScan(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid scan ID")
		h.redirect(w, r, "/history/scans")
		return
	}

	// Get the scan
	scan, err := h.db.GetScan(id)
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to get scan for retry")
		h.flashErr(w, "Scan not found")
		h.redirect(w, r, "/history/scans")
		return
	}
	if scan == nil {
		h.flashErr(w, "Scan not found")
		h.redirect(w, r, "/history/scans")
		return
	}

	if h.processor == nil {
		h.flashErr(w, "Scan processor unavailable")
		h.redirect(w, r, "/history/scans")
		return
	}

	h.processor.QueueScan(processor.ScanRequest{
		Path:      scan.Path,
		TriggerID: scan.TriggerID,
		Priority:  scan.Priority,
		EventType: scan.EventType,
		FilePaths: scan.FilePaths,
	})

	h.flash(w, "Scan queued for retry")
	h.redirect(w, r, "/history/scans")
}

// HistoryUploads renders the upload history page
func (h *Handlers) HistoryUploads(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildHistoryUploadsData(r)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list upload history")
		h.flashErr(w, "Failed to load upload history")
		h.redirect(w, r, "/")
		return
	}

	h.render(w, r, "history_uploads.html", data)
}

// HistoryUploadsStatsPartial returns the upload stats section for HTMX refresh
func (h *Handlers) HistoryUploadsStatsPartial(w http.ResponseWriter, r *http.Request) {
	remote := r.URL.Query().Get("remote")

	totalUploads, totalBytes, _ := h.db.GetUploadStats()
	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)

	h.renderPartial(w, "history_uploads.html", "upload_history_stats", map[string]any{
		"Remote":       remote,
		"TotalUploads": totalUploads,
		"TotalBytes":   totalBytes,
		"ActiveCount":  activeCount,
		"QueuedCount":  queuedCount,
	})
}

// HistoryUploadsTablePartial returns the upload history table for HTMX refresh
func (h *Handlers) HistoryUploadsTablePartial(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildHistoryUploadsData(r)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load upload history")
		http.Error(w, "Failed to load upload history", http.StatusInternalServerError)
		return
	}
	h.renderPartial(w, "history_uploads.html", "upload_history_table", data)
}

func (h *Handlers) buildHistoryUploadsData(r *http.Request) (map[string]any, error) {
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	remote := r.URL.Query().Get("remote")

	limit := 50
	offset := (page - 1) * limit

	history, err := h.db.ListUploadHistoryFiltered(remote, limit, offset)
	if err != nil {
		return nil, err
	}

	type UploadHistoryItem struct {
		ID          int64
		LocalPath   string
		RemoteName  string
		RemotePath  string
		SizeBytes   int64
		CompletedAt time.Time
	}

	var items []UploadHistoryItem
	for _, hist := range history {
		item := UploadHistoryItem{
			ID:          hist.ID,
			LocalPath:   hist.LocalPath,
			RemoteName:  hist.RemoteName,
			RemotePath:  hist.RemotePath,
			CompletedAt: hist.CompletedAt,
		}
		if hist.SizeBytes != nil {
			item.SizeBytes = *hist.SizeBytes
		}
		items = append(items, item)
	}

	totalCount, _ := h.db.CountUploadHistoryFiltered(remote)
	totalPages := (totalCount + limit - 1) / limit
	totalUploads, totalBytes, _ := h.db.GetUploadStats()

	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)

	remotes, _ := h.db.ListUploadRemotes()

	return map[string]any{
		"Uploads":      items,
		"Page":         page,
		"TotalPages":   totalPages,
		"TotalCount":   totalCount,
		"Remote":       remote,
		"Remotes":      remotes,
		"TotalUploads": totalUploads,
		"TotalBytes":   totalBytes,
		"ActiveCount":  activeCount,
		"QueuedCount":  queuedCount,
		"PendingCount": pendingCount,
		"HasPrev":      page > 1,
		"HasNext":      page < totalPages,
		"PrevPage":     page - 1,
		"NextPage":     page + 1,
	}, nil
}

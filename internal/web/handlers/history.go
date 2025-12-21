package handlers

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// ScanHistoryItem represents a scan for the history page
type ScanHistoryItem struct {
	ID          int64
	Path        string
	TriggerName string
	Status      string
	CreatedAt   string
	CompletedAt string
	Duration    string
	Error       string
}

// HistoryScans renders the scan history page
func (h *Handlers) HistoryScans(w http.ResponseWriter, r *http.Request) {
	// Parse query params
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	status := r.URL.Query().Get("status")
	limit := 50
	offset := (page - 1) * limit

	// Get scans with filtering
	scans, err := h.db.ListScansFiltered(status, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list scans")
		h.flashErr(w, "Failed to load scan history")
		h.redirect(w, r, "/")
		return
	}

	// Get trigger names for each scan
	triggers := make(map[int64]string)
	triggerList, _ := h.db.ListTriggers()
	for _, t := range triggerList {
		triggers[t.ID] = t.Name
	}

	// Convert to history items
	var items []ScanHistoryItem
	for _, s := range scans {
		item := ScanHistoryItem{
			ID:        s.ID,
			Path:      s.Path,
			Status:    string(s.Status),
			CreatedAt: s.CreatedAt.Format("2006-01-02 15:04:05"),
			Error:     s.LastError,
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
		if s.CompletedAt != nil {
			item.CompletedAt = s.CompletedAt.Format("2006-01-02 15:04:05")
			if s.StartedAt != nil {
				duration := s.CompletedAt.Sub(*s.StartedAt)
				item.Duration = duration.Round(1e9).String() // Round to seconds
			}
		}
		items = append(items, item)
	}

	// Get total count for pagination
	totalCount, _ := h.db.CountScansFiltered(status)
	totalPages := (totalCount + limit - 1) / limit

	// Get stats by status
	stats, _ := h.db.GetScanStatsByStatus()

	h.render(w, r, "history_scans.html", map[string]any{
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
	})
}

// RetryScan retries a failed scan
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

	// Only allow retry of failed scans
	if scan.Status != database.ScanStatusFailed {
		h.flashErr(w, "Only failed scans can be retried")
		h.redirect(w, r, "/history/scans")
		return
	}

	// Reset the scan status to pending
	err = h.db.UpdateScanStatus(id, database.ScanStatusPending)
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to reset scan status")
		h.flashErr(w, "Failed to retry scan")
		h.redirect(w, r, "/history/scans")
		return
	}

	// Clear the error
	if err := h.db.UpdateScanError(id, ""); err != nil {
		log.Warn().Err(err).Int64("id", id).Msg("Failed to clear scan error during retry")
	}

	h.flash(w, "Scan queued for retry")
	h.redirect(w, r, "/history/scans")
}

// HistoryUploads renders the upload history page
func (h *Handlers) HistoryUploads(w http.ResponseWriter, r *http.Request) {
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
		h.redirect(w, r, "/")
		return
	}

	// Convert to display items
	type UploadHistoryItem struct {
		ID          int64
		LocalPath   string
		RemoteName  string
		RemotePath  string
		SizeBytes   int64
		CompletedAt string
	}

	var items []UploadHistoryItem
	for _, hist := range history {
		item := UploadHistoryItem{
			ID:          hist.ID,
			LocalPath:   hist.LocalPath,
			RemoteName:  hist.RemoteName,
			RemotePath:  hist.RemotePath,
			CompletedAt: hist.CompletedAt.Format("2006-01-02 15:04:05"),
		}
		if hist.SizeBytes != nil {
			item.SizeBytes = *hist.SizeBytes
		}
		items = append(items, item)
	}

	totalCount, _ := h.db.CountUploadHistory()
	totalPages := (totalCount + limit - 1) / limit
	totalUploads, totalBytes, _ := h.db.GetUploadStats()

	// Get queue stats
	activeCount, _ := h.db.CountUploads(database.UploadStatusUploading)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)

	h.render(w, r, "history_uploads.html", map[string]any{
		"Uploads":      items,
		"Page":         page,
		"TotalPages":   totalPages,
		"TotalCount":   totalCount,
		"TotalUploads": totalUploads,
		"TotalBytes":   totalBytes,
		"ActiveCount":  activeCount,
		"QueuedCount":  queuedCount,
		"PendingCount": pendingCount,
		"HasPrev":      page > 1,
		"HasNext":      page < totalPages,
		"PrevPage":     page - 1,
		"NextPage":     page + 1,
	})
}

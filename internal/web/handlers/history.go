package handlers

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// ScanHistoryItem represents a scan for the history page
type ScanHistoryItem struct {
	ID          int64
	Path        string
	TriggerPath string
	TriggerName string
	Status      string
	CreatedAt   time.Time
	CompletedAt *time.Time
	Error       string
	TargetPaths []ScanHistoryTargetPath
}

// ScanHistoryTargetPath represents a scan path sent to a target.
type ScanHistoryTargetPath struct {
	TargetID   int64
	TargetName string
	Path       string
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

	scanIDs := make([]int64, 0, len(scans))
	for _, s := range scans {
		scanIDs = append(scanIDs, s.ID)
	}

	triggers := make(map[int64]string)
	triggerList, _ := h.db.ListTriggers()
	for _, t := range triggerList {
		triggers[t.ID] = t.Name
	}

	targetNames := make(map[int64]string)
	targetList, _ := h.db.ListTargets()
	for _, target := range targetList {
		targetNames[target.ID] = target.Name
	}

	targetPathsByScan, err := h.db.ListScanTargetPaths(scanIDs)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load scan target paths")
		targetPathsByScan = make(map[int64][]database.ScanTargetPath)
	}

	plexInfo := h.plexScanInfo()

	var items []ScanHistoryItem
	for _, s := range scans {
		status := string(s.Status)
		if s.Status == database.ScanStatusRetry {
			status = string(database.ScanStatusFailed)
		}
		if plexInfo.matcher != nil && s.Status == database.ScanStatusCompleted && plexInfo.matcher.HasPending(s.Path) {
			status = string(database.ScanStatusScanning)
		}
		triggerPath := s.TriggerPath
		if triggerPath == "" {
			triggerPath = s.Path
		}
		targetPaths := make([]ScanHistoryTargetPath, 0, len(targetPathsByScan[s.ID]))
		for _, targetPath := range targetPathsByScan[s.ID] {
			name := targetNames[targetPath.TargetID]
			if name == "" {
				name = fmt.Sprintf("Target #%d", targetPath.TargetID)
			}
			targetPaths = append(targetPaths, ScanHistoryTargetPath{
				TargetID:   targetPath.TargetID,
				TargetName: name,
				Path:       targetPath.ScanPath,
			})
		}
		sort.Slice(targetPaths, func(i, j int) bool {
			if targetPaths[i].TargetName == targetPaths[j].TargetName {
				return targetPaths[i].TargetID < targetPaths[j].TargetID
			}
			return targetPaths[i].TargetName < targetPaths[j].TargetName
		})
		item := ScanHistoryItem{
			ID:          s.ID,
			Path:        s.Path,
			TriggerPath: triggerPath,
			Status:      status,
			CreatedAt:   s.CreatedAt,
			CompletedAt: s.CompletedAt,
			Error:       s.LastError,
			TargetPaths: targetPaths,
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
		"Scans":               items,
		"Page":                page,
		"TotalPages":          totalPages,
		"TotalCount":          totalCount,
		"Status":              status,
		"Stats":               stats,
		"PlexTrackingEnabled": plexInfo.enabled,
		"PlexScanningCount":   plexInfo.pendingCount,
		"HasPrev":             page > 1,
		"HasNext":             page < totalPages,
		"PrevPage":            page - 1,
		"NextPage":            page + 1,
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
	plexInfo := h.plexScanInfo()

	h.renderPartial(w, "history_scans.html", "scan_history_stats", map[string]any{
		"Stats":               stats,
		"PlexTrackingEnabled": plexInfo.enabled,
		"PlexScanningCount":   plexInfo.pendingCount,
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

// DeleteScanHistoryItem removes a scan history entry.
func (h *Handlers) DeleteScanHistoryItem(w http.ResponseWriter, r *http.Request) {
	redirectURL := "/history/scans"
	params := make([]string, 0, 2)
	if page := r.URL.Query().Get("page"); page != "" {
		if p, err := strconv.Atoi(page); err == nil && p > 0 {
			params = append(params, "page="+strconv.Itoa(p))
		}
	}
	if status := r.URL.Query().Get("status"); status != "" {
		params = append(params, "status="+url.QueryEscape(status))
	}
	if len(params) > 0 {
		redirectURL += "?" + strings.Join(params, "&")
	}

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid scan ID")
		h.redirect(w, r, redirectURL)
		return
	}

	scan, err := h.db.GetScan(id)
	if err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to get scan for delete")
		h.flashErr(w, "Scan not found")
		h.redirect(w, r, redirectURL)
		return
	}
	if scan == nil {
		h.flashErr(w, "Scan not found")
		h.redirect(w, r, redirectURL)
		return
	}

	if err := h.db.DeleteScan(id); err != nil {
		log.Error().Err(err).Int64("id", id).Msg("Failed to delete scan")
		h.flashErr(w, "Failed to delete scan")
		h.redirect(w, r, redirectURL)
		return
	}

	h.flash(w, "Scan deleted")
	h.redirect(w, r, redirectURL)
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
	pendingCount, _ := h.db.CountUploads(database.UploadStatusPending)
	queuedCount, _ := h.db.CountUploads(database.UploadStatusQueued)

	h.renderPartial(w, "history_uploads.html", "upload_history_stats", map[string]any{
		"Remote":       remote,
		"TotalUploads": totalUploads,
		"TotalBytes":   totalBytes,
		"ActiveCount":  activeCount,
		"PendingCount": pendingCount,
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

package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

// ActivityLogEntry represents an activity entry for display
type ActivityLogEntry struct {
	ID        int64
	Type      string // "scan", "upload", "notification"
	Status    string
	Path      string
	Message   string
	CreatedAt time.Time
}

// LogsPage renders the activity logs page
func (h *Handlers) LogsPage(w http.ResponseWriter, r *http.Request) {
	// Parse query params
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	logType := r.URL.Query().Get("type")
	limit := 50

	var activities []ActivityLogEntry

	// Get recent scans
	if logType == "" || logType == "all" || logType == "scan" {
		scans, err := h.db.ListRecentScans(limit)
		if err == nil {
			for _, scan := range scans {
				msg := ""
				if scan.LastError != "" {
					msg = scan.LastError
				}
				activities = append(activities, ActivityLogEntry{
					ID:        scan.ID,
					Type:      "scan",
					Status:    string(scan.Status),
					Path:      scan.Path,
					Message:   msg,
					CreatedAt: scan.CreatedAt,
				})
			}
		}
	}

	// Get recent uploads
	if logType == "" || logType == "all" || logType == "upload" {
		uploads, err := h.db.ListRecentUploads(limit)
		if err == nil {
			for _, upload := range uploads {
				msg := ""
				if upload.LastError != "" {
					msg = upload.LastError
				}
				activities = append(activities, ActivityLogEntry{
					ID:        upload.ID,
					Type:      "upload",
					Status:    string(upload.Status),
					Path:      upload.LocalPath,
					Message:   msg,
					CreatedAt: upload.CreatedAt,
				})
			}
		}
	}

	// Get recent notifications
	if logType == "" || logType == "all" || logType == "notification" {
		logs, err := h.db.ListNotificationLogs(limit)
		if err == nil {
			for _, entry := range logs {
				msg := ""
				if entry.Error != "" {
					msg = entry.Error
				}
				activities = append(activities, ActivityLogEntry{
					ID:        entry.ID,
					Type:      "notification",
					Status:    entry.Status,
					Path:      entry.Provider + ": " + entry.EventType,
					Message:   msg,
					CreatedAt: entry.CreatedAt,
				})
			}
		} else {
			log.Debug().Err(err).Msg("Failed to query notification logs")
		}
	}

	// Get counts
	scanCount, _ := h.db.CountScansFiltered("")
	uploadCount, _ := h.db.CountUploadsTotal()
	notificationCount, _ := h.db.CountNotificationLogs()

	totalCount := scanCount + uploadCount + notificationCount
	totalPages := max((totalCount+limit-1)/limit, 1)

	h.render(w, r, "logs.html", map[string]any{
		"Activities":        activities,
		"Page":              page,
		"TotalPages":        totalPages,
		"TotalCount":        totalCount,
		"Type":              logType,
		"ScanCount":         scanCount,
		"UploadCount":       uploadCount,
		"NotificationCount": notificationCount,
		"HasPrev":           page > 1,
		"HasNext":           page < totalPages,
		"PrevPage":          page - 1,
		"NextPage":          page + 1,
	})
}

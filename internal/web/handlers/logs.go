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
		rows, err := h.db.Query(`
			SELECT id, status, path, last_error, created_at
			FROM scans
			ORDER BY created_at DESC
			LIMIT ?
		`, limit)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id int64
				var status, path string
				var lastError *string
				var createdAt time.Time
				if err := rows.Scan(&id, &status, &path, &lastError, &createdAt); err == nil {
					msg := ""
					if lastError != nil && *lastError != "" {
						msg = *lastError
					}
					activities = append(activities, ActivityLogEntry{
						ID:        id,
						Type:      "scan",
						Status:    status,
						Path:      path,
						Message:   msg,
						CreatedAt: createdAt,
					})
				}
			}
		}
	}

	// Get recent uploads
	if logType == "" || logType == "all" || logType == "upload" {
		rows, err := h.db.Query(`
			SELECT id, status, local_path, last_error, created_at
			FROM uploads
			ORDER BY created_at DESC
			LIMIT ?
		`, limit)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id int64
				var status, path string
				var lastError *string
				var createdAt time.Time
				if err := rows.Scan(&id, &status, &path, &lastError, &createdAt); err == nil {
					msg := ""
					if lastError != nil && *lastError != "" {
						msg = *lastError
					}
					activities = append(activities, ActivityLogEntry{
						ID:        id,
						Type:      "upload",
						Status:    status,
						Path:      path,
						Message:   msg,
						CreatedAt: createdAt,
					})
				}
			}
		}
	}

	// Get recent notifications
	if logType == "" || logType == "all" || logType == "notification" {
		rows, err := h.db.Query(`
			SELECT id, event_type, provider, status, error, created_at
			FROM notification_log
			ORDER BY created_at DESC
			LIMIT ?
		`, limit)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id int64
				var eventType, provider, status string
				var errorMsg *string
				var createdAt time.Time
				if err := rows.Scan(&id, &eventType, &provider, &status, &errorMsg, &createdAt); err == nil {
					msg := ""
					if errorMsg != nil && *errorMsg != "" {
						msg = *errorMsg
					}
					activities = append(activities, ActivityLogEntry{
						ID:        id,
						Type:      "notification",
						Status:    status,
						Path:      provider + ": " + eventType,
						Message:   msg,
						CreatedAt: createdAt,
					})
				}
			}
		} else {
			log.Debug().Err(err).Msg("Failed to query notification logs")
		}
	}

	// Get counts
	var scanCount, uploadCount, notificationCount int
	h.db.QueryRow("SELECT COUNT(*) FROM scans").Scan(&scanCount)
	h.db.QueryRow("SELECT COUNT(*) FROM uploads").Scan(&uploadCount)
	h.db.QueryRow("SELECT COUNT(*) FROM notification_log").Scan(&notificationCount)

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

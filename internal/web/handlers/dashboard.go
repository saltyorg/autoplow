package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// DashboardData contains data for the dashboard page
type DashboardData struct {
	Stats           DashboardStats
	RecentScans     []ScanSummary
	RecentUploads   []UploadSummary
	ActiveSessions  []SessionSummary
	ScanningEnabled bool
	UploadsEnabled  bool
	ThrottleStatus  *ThrottleStatusData
}

// ThrottleStatusData contains throttle status for the dashboard
type ThrottleStatusData struct {
	Enabled         bool
	CurrentLimit    string
	MaxBandwidth    string
	StreamBandwidth string
	SessionCount    int
	StatusText      string
	StatusClass     string
}

// DashboardStats contains summary statistics
type DashboardStats struct {
	// Scan stats
	PendingScans   int
	CompletedScans int
	FailedScans    int
	// Upload stats
	ActiveUploads    int
	QueuedUploads    int
	CompletedUploads int
	FailedUploads    int
	// Sessions
	ActiveSessions int
	// Feature flags
	UploadsEnabled bool
}

// ScanSummary is a brief scan record for the dashboard
type ScanSummary struct {
	ID        int64
	Path      string
	Status    string
	Trigger   string
	CreatedAt string
}

// UploadSummary is a brief upload record for the dashboard
type UploadSummary struct {
	ID        int64
	Path      string
	Status    string
	Progress  int
	Remote    string
	CreatedAt string
}

// SessionSummary is a brief session record
type SessionSummary struct {
	ID      string
	Server  string
	User    string
	Title   string
	Format  string
	Bitrate string
}

// Dashboard renders the main dashboard
func (h *Handlers) Dashboard(w http.ResponseWriter, r *http.Request) {
	data := DashboardData{}

	// Check if scanning is enabled
	scanningEnabled := true
	if val, _ := h.db.GetSetting("scanning.enabled"); val == "false" {
		scanningEnabled = false
	}
	data.ScanningEnabled = scanningEnabled

	// Check if uploads are enabled
	uploadsEnabled := true
	if val, _ := h.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}
	data.UploadsEnabled = uploadsEnabled
	data.Stats.UploadsEnabled = uploadsEnabled

	// Scan stats - all time totals
	data.Stats.PendingScans, _ = h.db.CountScansFiltered(string(database.ScanStatusPending))
	data.Stats.CompletedScans, _ = h.db.CountScansFiltered(string(database.ScanStatusCompleted))
	data.Stats.FailedScans, _ = h.db.CountScansFiltered(string(database.ScanStatusFailed))

	// Upload stats - all time totals (only if enabled)
	if uploadsEnabled {
		data.Stats.ActiveUploads, _ = h.db.CountUploads(database.UploadStatusUploading)
		data.Stats.QueuedUploads, _ = h.db.CountUploads(database.UploadStatusQueued)
		data.Stats.CompletedUploads, _ = h.db.CountUploads(database.UploadStatusCompleted)
		data.Stats.FailedUploads, _ = h.db.CountUploads(database.UploadStatusFailed)
	}

	// Get active sessions count
	data.Stats.ActiveSessions, _ = h.db.GetActiveSessionCount()

	// Get recent scans
	scans, err := h.db.ListRecentScans(10)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get recent scans")
	} else {
		for _, scan := range scans {
			triggerName := "Unknown"
			if scan.TriggerID != nil {
				if trigger, err := h.db.GetTrigger(*scan.TriggerID); err == nil && trigger != nil {
					triggerName = trigger.Name
				}
			}
			data.RecentScans = append(data.RecentScans, ScanSummary{
				ID:        scan.ID,
				Path:      scan.Path,
				Status:    string(scan.Status),
				Trigger:   triggerName,
				CreatedAt: scan.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	// Get recent uploads
	uploads, err := h.db.ListRecentUploads(10)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get recent uploads")
	} else {
		for _, upload := range uploads {
			progress := 0
			if upload.SizeBytes != nil && *upload.SizeBytes > 0 {
				progress = int(upload.ProgressBytes * 100 / *upload.SizeBytes)
			}
			data.RecentUploads = append(data.RecentUploads, UploadSummary{
				ID:        upload.ID,
				Path:      upload.LocalPath,
				Status:    string(upload.Status),
				Progress:  progress,
				Remote:    upload.RemoteName,
				CreatedAt: upload.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	// Check display units preference
	useBinary := true
	if val, _ := h.db.GetSetting("display.use_binary_units"); val == "false" {
		useBinary = false
	}
	useBits := true
	if val, _ := h.db.GetSetting("display.use_bits_for_bitrate"); val == "false" {
		useBits = false
	}

	// Get active sessions
	sessions, err := h.db.ListActiveSessions()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get active sessions")
	} else {
		for _, session := range sessions {
			data.ActiveSessions = append(data.ActiveSessions, SessionSummary{
				ID:      session.ID,
				Server:  session.ServerType,
				User:    session.Username,
				Title:   session.MediaTitle,
				Format:  session.Format,
				Bitrate: formatBitrateWithOptions(session.Bitrate, useBinary, useBits),
			})
		}
	}

	// Get throttle status if available
	if h.throttleMgr != nil {
		stats := h.throttleMgr.Stats()
		config := h.throttleMgr.GetConfig()

		var statusClass, statusText string
		if !stats.Enabled {
			statusClass = "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300"
			statusText = "Disabled"
		} else if stats.SessionCount > 0 {
			statusClass = "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200"
			statusText = "Throttling"
		} else {
			statusClass = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
			statusText = "Active"
		}

		data.ThrottleStatus = &ThrottleStatusData{
			Enabled:         stats.Enabled,
			CurrentLimit:    formatBandwidthWithOptions(stats.CurrentLimit, useBinary, useBits),
			MaxBandwidth:    formatBandwidthWithOptions(config.MaxBandwidth, useBinary, useBits),
			StreamBandwidth: formatBandwidthWithOptions(config.StreamBandwidth, useBinary, useBits),
			SessionCount:    stats.SessionCount,
			StatusText:      statusText,
			StatusClass:     statusClass,
		}
	}

	h.render(w, r, "dashboard.html", data)
}

// DashboardStatsPartial returns just the scan stats section (for HTMX polling)
func (h *Handlers) DashboardStatsPartial(w http.ResponseWriter, r *http.Request) {
	stats := DashboardStats{}

	// Scan stats - all time totals
	stats.PendingScans, _ = h.db.CountScansFiltered(string(database.ScanStatusPending))
	stats.CompletedScans, _ = h.db.CountScansFiltered(string(database.ScanStatusCompleted))
	stats.FailedScans, _ = h.db.CountScansFiltered(string(database.ScanStatusFailed))

	h.renderPartial(w, "dashboard.html", "scan_stats", stats)
}

// DashboardUploadStatsPartial returns just the upload stats section (for HTMX polling)
func (h *Handlers) DashboardUploadStatsPartial(w http.ResponseWriter, r *http.Request) {
	stats := DashboardStats{}

	// Upload stats - all time totals
	stats.ActiveUploads, _ = h.db.CountUploads(database.UploadStatusUploading)
	stats.QueuedUploads, _ = h.db.CountUploads(database.UploadStatusQueued)
	stats.CompletedUploads, _ = h.db.CountUploads(database.UploadStatusCompleted)
	stats.FailedUploads, _ = h.db.CountUploads(database.UploadStatusFailed)

	h.renderPartial(w, "dashboard.html", "upload_stats", stats)
}

// DashboardScansPartial returns recent scans (for HTMX polling)
func (h *Handlers) DashboardScansPartial(w http.ResponseWriter, r *http.Request) {
	var scans []ScanSummary

	recentScans, err := h.db.ListRecentScans(10)
	if err == nil {
		for _, scan := range recentScans {
			triggerName := "Unknown"
			if scan.TriggerID != nil {
				if trigger, err := h.db.GetTrigger(*scan.TriggerID); err == nil && trigger != nil {
					triggerName = trigger.Name
				}
			}
			scans = append(scans, ScanSummary{
				ID:        scan.ID,
				Path:      scan.Path,
				Status:    string(scan.Status),
				Trigger:   triggerName,
				CreatedAt: scan.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	h.renderPartial(w, "dashboard.html", "recent_scans", scans)
}

// DashboardUploadsPartial returns recent uploads (for HTMX polling)
func (h *Handlers) DashboardUploadsPartial(w http.ResponseWriter, r *http.Request) {
	var uploads []UploadSummary

	recentUploads, err := h.db.ListRecentUploads(10)
	if err == nil {
		for _, upload := range recentUploads {
			progress := 0
			if upload.SizeBytes != nil && *upload.SizeBytes > 0 {
				progress = int(upload.ProgressBytes * 100 / *upload.SizeBytes)
			}
			uploads = append(uploads, UploadSummary{
				ID:        upload.ID,
				Path:      upload.LocalPath,
				Status:    string(upload.Status),
				Progress:  progress,
				Remote:    upload.RemoteName,
				CreatedAt: upload.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	h.renderPartial(w, "dashboard.html", "recent_uploads", uploads)
}

// DashboardSessionsPartial returns active sessions (for HTMX polling)
func (h *Handlers) DashboardSessionsPartial(w http.ResponseWriter, r *http.Request) {
	var sessions []SessionSummary

	// Check display units preference
	useBinary := true
	if val, _ := h.db.GetSetting("display.use_binary_units"); val == "false" {
		useBinary = false
	}
	useBits := true
	if val, _ := h.db.GetSetting("display.use_bits_for_bitrate"); val == "false" {
		useBits = false
	}

	activeSessions, err := h.db.ListActiveSessions()
	if err == nil {
		for _, session := range activeSessions {
			sessions = append(sessions, SessionSummary{
				ID:      session.ID,
				Server:  session.ServerType,
				User:    session.Username,
				Title:   session.MediaTitle,
				Format:  session.Format,
				Bitrate: formatBitrateWithOptions(session.Bitrate, useBinary, useBits),
			})
		}
	}

	h.renderPartial(w, "dashboard.html", "active_sessions", sessions)
}

// DashboardThrottleStatusPartial returns throttle status (for HTMX polling)
func (h *Handlers) DashboardThrottleStatusPartial(w http.ResponseWriter, r *http.Request) {
	// Return empty if uploads are disabled (throttle section won't be shown anyway)
	if val, _ := h.db.GetSetting("uploads.enabled"); val == "false" {
		w.Header().Set("Content-Type", "text/html")
		return
	}

	if h.throttleMgr == nil {
		w.Header().Set("Content-Type", "text/html")
		return
	}

	// Check display units preference
	useBinary := true
	if val, _ := h.db.GetSetting("display.use_binary_units"); val == "false" {
		useBinary = false
	}
	useBits := true
	if val, _ := h.db.GetSetting("display.use_bits_for_bitrate"); val == "false" {
		useBits = false
	}

	stats := h.throttleMgr.Stats()
	config := h.throttleMgr.GetConfig()

	var statusClass, statusText string
	if !stats.Enabled {
		statusClass = "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300"
		statusText = "Disabled"
	} else if stats.SessionCount > 0 {
		statusClass = "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200"
		statusText = "Throttling"
	} else {
		statusClass = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
		statusText = "Active"
	}

	data := &ThrottleStatusData{
		Enabled:         stats.Enabled,
		CurrentLimit:    formatBandwidthWithOptions(stats.CurrentLimit, useBinary, useBits),
		MaxBandwidth:    formatBandwidthWithOptions(config.MaxBandwidth, useBinary, useBits),
		StreamBandwidth: formatBandwidthWithOptions(config.StreamBandwidth, useBinary, useBits),
		SessionCount:    stats.SessionCount,
		StatusText:      statusText,
		StatusClass:     statusClass,
	}

	h.renderPartial(w, "dashboard.html", "throttle_status", data)
}

// formatBitrateWithOptions converts bits per second to human-readable format
// useBinary: true = 1024-based, false = 1000-based (only applies when useBits=false)
// useBits: true = display as Mbps (bits), false = display as MiB/s (bytes)
func formatBitrateWithOptions(bps int64, useBinary bool, useBits bool) string {
	if bps == 0 {
		return ""
	}

	// If displaying as bits (Mbps), use decimal (1000-based) which is industry standard
	if useBits {
		var base float64 = 1000
		units := []string{"bps", "Kbps", "Mbps", "Gbps"}

		value := float64(bps)
		unitIdx := 0
		for value >= base && unitIdx < len(units)-1 {
			value /= base
			unitIdx++
		}

		if unitIdx == 0 {
			return fmt.Sprintf("%d %s", bps, units[unitIdx])
		}
		return fmt.Sprintf("%.1f %s", value, units[unitIdx])
	}

	// Display as bytes per second
	bytesPerSec := bps / 8

	var base int64
	var units []string
	if useBinary {
		base = 1024
		units = []string{"B/s", "KiB/s", "MiB/s", "GiB/s"}
	} else {
		base = 1000
		units = []string{"B/s", "KB/s", "MB/s", "GB/s"}
	}

	value := float64(bytesPerSec)
	unitIdx := 0
	for value >= float64(base) && unitIdx < len(units)-1 {
		value /= float64(base)
		unitIdx++
	}

	if unitIdx == 0 {
		return fmt.Sprintf("%d %s", bytesPerSec, units[unitIdx])
	}
	return fmt.Sprintf("%.1f %s", value, units[unitIdx])
}

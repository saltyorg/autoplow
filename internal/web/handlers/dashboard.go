package handlers

import (
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
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
	PendingScans    int
	ActiveScans     int
	CompletedScans  int
	FailedScans     int
	TotalScansToday int
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

	// Scan stats (only if enabled)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM scans WHERE status = 'pending'").Scan(&data.Stats.PendingScans)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM scans WHERE status = 'scanning'").Scan(&data.Stats.ActiveScans)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM scans WHERE status = 'completed' AND date(completed_at) = date('now')`).Scan(&data.Stats.CompletedScans)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM scans WHERE status = 'failed' AND date(created_at) = date('now')`).Scan(&data.Stats.FailedScans)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM scans WHERE date(created_at) = date('now')`).Scan(&data.Stats.TotalScansToday)

	// Upload stats (only if enabled)
	if uploadsEnabled {
		_ = h.db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status = 'uploading'").Scan(&data.Stats.ActiveUploads)
		_ = h.db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status = 'queued'").Scan(&data.Stats.QueuedUploads)
		_ = h.db.QueryRow(`SELECT COUNT(*) FROM uploads WHERE status = 'completed' AND date(completed_at) = date('now')`).Scan(&data.Stats.CompletedUploads)
		_ = h.db.QueryRow(`SELECT COUNT(*) FROM uploads WHERE status = 'failed' AND date(created_at) = date('now')`).Scan(&data.Stats.FailedUploads)
	}

	// Get active sessions count
	_ = h.db.QueryRow("SELECT COUNT(*) FROM active_sessions").Scan(&data.Stats.ActiveSessions)

	// Get recent scans
	rows, err := h.db.Query(`
		SELECT s.id, s.path, s.status, COALESCE(t.name, 'Unknown') as trigger_name, s.created_at
		FROM scans s
		LEFT JOIN triggers t ON s.trigger_id = t.id
		ORDER BY s.created_at DESC
		LIMIT 10
	`)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get recent scans")
	} else {
		defer rows.Close()
		for rows.Next() {
			var scan ScanSummary
			if err := rows.Scan(&scan.ID, &scan.Path, &scan.Status, &scan.Trigger, &scan.CreatedAt); err != nil {
				log.Error().Err(err).Msg("Failed to scan row")
				continue
			}
			data.RecentScans = append(data.RecentScans, scan)
		}
	}

	// Get recent uploads
	rows, err = h.db.Query(`
		SELECT id, local_path, status,
			CASE WHEN size_bytes > 0 THEN (progress_bytes * 100 / size_bytes) ELSE 0 END as progress,
			remote_name, created_at
		FROM uploads
		ORDER BY created_at DESC
		LIMIT 10
	`)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get recent uploads")
	} else {
		defer rows.Close()
		for rows.Next() {
			var upload UploadSummary
			if err := rows.Scan(&upload.ID, &upload.Path, &upload.Status, &upload.Progress, &upload.Remote, &upload.CreatedAt); err != nil {
				log.Error().Err(err).Msg("Failed to scan row")
				continue
			}
			data.RecentUploads = append(data.RecentUploads, upload)
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
	rows, err = h.db.Query(`
		SELECT id, server_type, username, media_title, resolution, bitrate
		FROM active_sessions
		ORDER BY updated_at DESC
	`)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get active sessions")
	} else {
		defer rows.Close()
		for rows.Next() {
			var session SessionSummary
			var bitrate int64
			if err := rows.Scan(&session.ID, &session.Server, &session.User, &session.Title, &session.Format, &bitrate); err != nil {
				log.Error().Err(err).Msg("Failed to scan row")
				continue
			}
			session.Bitrate = formatBitrateWithOptions(bitrate, useBinary, useBits)
			data.ActiveSessions = append(data.ActiveSessions, session)
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

	// Scan stats
	_ = h.db.QueryRow("SELECT COUNT(*) FROM scans WHERE status = 'pending'").Scan(&stats.PendingScans)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM scans WHERE status = 'scanning'").Scan(&stats.ActiveScans)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM scans WHERE status = 'completed' AND date(completed_at) = date('now')`).Scan(&stats.CompletedScans)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM scans WHERE status = 'failed' AND date(created_at) = date('now')`).Scan(&stats.FailedScans)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM active_sessions").Scan(&stats.ActiveSessions)

	h.renderPartial(w, "dashboard.html", "scan_stats", stats)
}

// DashboardUploadStatsPartial returns just the upload stats section (for HTMX polling)
func (h *Handlers) DashboardUploadStatsPartial(w http.ResponseWriter, r *http.Request) {
	stats := DashboardStats{}

	_ = h.db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status = 'uploading'").Scan(&stats.ActiveUploads)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM uploads WHERE status = 'queued'").Scan(&stats.QueuedUploads)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM uploads WHERE status = 'completed' AND date(completed_at) = date('now')`).Scan(&stats.CompletedUploads)
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM uploads WHERE status = 'failed' AND date(created_at) = date('now')`).Scan(&stats.FailedUploads)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM active_sessions").Scan(&stats.ActiveSessions)

	h.renderPartial(w, "dashboard.html", "upload_stats", stats)
}

// DashboardScansPartial returns recent scans (for HTMX polling)
func (h *Handlers) DashboardScansPartial(w http.ResponseWriter, r *http.Request) {
	var scans []ScanSummary

	rows, err := h.db.Query(`
		SELECT s.id, s.path, s.status, COALESCE(t.name, 'Unknown') as trigger_name, s.created_at
		FROM scans s
		LEFT JOIN triggers t ON s.trigger_id = t.id
		ORDER BY s.created_at DESC
		LIMIT 10
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var scan ScanSummary
			if err := rows.Scan(&scan.ID, &scan.Path, &scan.Status, &scan.Trigger, &scan.CreatedAt); err == nil {
				scans = append(scans, scan)
			}
		}
	}

	h.renderPartial(w, "dashboard.html", "recent_scans", scans)
}

// DashboardUploadsPartial returns recent uploads (for HTMX polling)
func (h *Handlers) DashboardUploadsPartial(w http.ResponseWriter, r *http.Request) {
	var uploads []UploadSummary

	rows, err := h.db.Query(`
		SELECT id, local_path, status,
			CASE WHEN size_bytes > 0 THEN (progress_bytes * 100 / size_bytes) ELSE 0 END as progress,
			remote_name, created_at
		FROM uploads
		ORDER BY created_at DESC
		LIMIT 10
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var upload UploadSummary
			if err := rows.Scan(&upload.ID, &upload.Path, &upload.Status, &upload.Progress, &upload.Remote, &upload.CreatedAt); err == nil {
				uploads = append(uploads, upload)
			}
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

	rows, err := h.db.Query(`
		SELECT id, server_type, username, media_title, resolution, bitrate
		FROM active_sessions
		ORDER BY updated_at DESC
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var session SessionSummary
			var bitrate int64
			if err := rows.Scan(&session.ID, &session.Server, &session.User, &session.Title, &session.Format, &bitrate); err == nil {
				session.Bitrate = formatBitrateWithOptions(bitrate, useBinary, useBits)
				sessions = append(sessions, session)
			}
		}
	}

	h.renderPartial(w, "dashboard.html", "active_sessions", sessions)
}

// DashboardThrottleStatusPartial returns throttle status (for HTMX polling)
func (h *Handlers) DashboardThrottleStatusPartial(w http.ResponseWriter, r *http.Request) {
	if h.throttleMgr == nil {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-sm text-gray-500 dark:text-gray-400">Throttle not initialized</div>`))
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

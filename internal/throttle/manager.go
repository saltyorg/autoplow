package throttle

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/rclone"
	"github.com/saltyorg/autoplow/internal/targets"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

// Config holds throttle manager configuration
type Config struct {
	// PollInterval is how often to check for active sessions
	PollInterval time.Duration

	// MaxBandwidth is the maximum upload bandwidth in bytes/sec (0 = unlimited)
	MaxBandwidth int64

	// StreamBandwidth is bandwidth to reserve per active stream in bytes/sec
	StreamBandwidth int64

	// MinBandwidth is the minimum bandwidth to allow even with active streams
	MinBandwidth int64

	// SkipUploadsBelow is the bandwidth threshold below which new uploads are skipped (0 = disabled)
	// When the calculated bandwidth falls below this value, new uploads won't be queued
	// Existing uploads continue at the throttled speed
	SkipUploadsBelow int64

	// Enabled controls whether throttling is active
	Enabled bool

	// Schedules for time-based throttling (optional)
	Schedules []Schedule
}

// Schedule represents a time-based bandwidth schedule
type Schedule struct {
	Name         string
	StartHour    int            // 0-23
	EndHour      int            // 0-23
	MaxBandwidth int64          // Max bandwidth during this period (0 = unlimited)
	Days         []time.Weekday // Days this schedule applies (empty = all days)
}

// DefaultConfig returns default throttle configuration
func DefaultConfig() Config {
	return Config{
		PollInterval:    30 * time.Second,
		MaxBandwidth:    0,        // Unlimited
		StreamBandwidth: 25000000, // ~25 MiB/s per stream (~200 Mbps)
		MinBandwidth:    1000000,  // ~1 MiB/s minimum
		Enabled:         false,
	}
}

// Throttle settings database keys
const (
	throttleEnabledKey          = "throttle.enabled"
	throttleMaxBandwidthKey     = "throttle.max_bandwidth"
	throttleStreamBandwidthKey  = "throttle.stream_bandwidth"
	throttleMinBandwidthKey     = "throttle.min_bandwidth"
	throttleSkipUploadsBelowKey = "throttle.skip_uploads_below"
	throttlePollIntervalKey     = "throttle.poll_interval"
)

// LoadConfigFromDB loads throttle configuration from the database
func LoadConfigFromDB(db *database.DB) Config {
	defaults := DefaultConfig()
	loader := config.NewLoader(db)

	return Config{
		PollInterval:     loader.DurationSeconds(throttlePollIntervalKey, int(defaults.PollInterval.Seconds())),
		MaxBandwidth:     loader.Int64(throttleMaxBandwidthKey, defaults.MaxBandwidth),
		StreamBandwidth:  loader.Int64(throttleStreamBandwidthKey, defaults.StreamBandwidth),
		MinBandwidth:     loader.Int64(throttleMinBandwidthKey, defaults.MinBandwidth),
		SkipUploadsBelow: loader.Int64(throttleSkipUploadsBelowKey, defaults.SkipUploadsBelow),
		Enabled:          loader.Bool(throttleEnabledKey, defaults.Enabled),
	}
}

// Manager handles bandwidth throttling based on active media sessions
type Manager struct {
	db         *database.DB
	targetsMgr *targets.Manager
	rcloneMgr  *rclone.Manager
	config     Config
	sseBroker  *sse.Broker

	mu              sync.RWMutex
	activeSessions  []*database.ActiveSession
	currentLimit    int64 // Current bandwidth limit in bytes/sec
	skippingUploads bool  // True when bandwidth is below skip threshold

	// Session data from WebSocket watchers (managed by targets.Manager)
	wsSessions   map[int64][]targets.Session // targetID -> sessions from WebSocket
	wsSessionsMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new throttle manager
func New(db *database.DB, targetsMgr *targets.Manager, rcloneMgr *rclone.Manager, config Config) *Manager {
	m := &Manager{
		db:         db,
		targetsMgr: targetsMgr,
		rcloneMgr:  rcloneMgr,
		config:     config,
		wsSessions: make(map[int64][]targets.Session),
	}

	// Register callback to receive session updates from WebSocket watchers
	targetsMgr.RegisterSessionCallback(m.handleWebSocketSessions)

	return m
}

// Start begins the throttle monitoring loop
func (m *Manager) Start() {
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.wg.Add(1)
	go m.pollLoop()

	log.Info().
		Dur("poll_interval", m.config.PollInterval).
		Bool("enabled", m.config.Enabled).
		Msg("Throttle manager started")
}

// Stop stops the throttle manager
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	// Clear state to allow clean restart
	m.mu.Lock()
	m.activeSessions = nil
	m.currentLimit = 0
	m.skippingUploads = false
	m.mu.Unlock()

	m.wsSessionsMu.Lock()
	m.wsSessions = make(map[int64][]targets.Session)
	m.wsSessionsMu.Unlock()

	log.Info().Msg("Throttle manager stopped")
}

// SetConfig updates the throttle configuration
func (m *Manager) SetConfig(config Config) {
	m.mu.Lock()
	m.config = config
	m.mu.Unlock()

	// Recalculate bandwidth immediately
	m.recalculateBandwidth()
}

// SetSSEBroker sets the SSE broker for broadcasting events
func (m *Manager) SetSSEBroker(broker *sse.Broker) {
	m.sseBroker = broker
}

// broadcastEvent broadcasts an SSE event if the broker is configured
func (m *Manager) broadcastEvent(eventType sse.EventType, data map[string]any) {
	if m.sseBroker != nil {
		m.sseBroker.Broadcast(sse.Event{Type: eventType, Data: data})
	}
}

// GetConfig returns the current configuration
func (m *Manager) GetConfig() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// GetActiveSessions returns current active sessions
func (m *Manager) GetActiveSessions() []*database.ActiveSession {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy
	sessions := make([]*database.ActiveSession, len(m.activeSessions))
	copy(sessions, m.activeSessions)
	return sessions
}

// GetCurrentLimit returns the current bandwidth limit
func (m *Manager) GetCurrentLimit() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentLimit
}

// ShouldSkipNewUploads returns true if new uploads should be skipped due to low bandwidth
func (m *Manager) ShouldSkipNewUploads() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.skippingUploads
}

// pollLoop periodically polls media servers for active sessions
func (m *Manager) pollLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()

	// Initial poll
	m.pollSessions()
	m.recalculateBandwidth()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.pollSessions()
			m.recalculateBandwidth()
		}
	}
}

// pollSessions polls media servers configured for polling mode for active sessions
// Targets using WebSocket mode are skipped as they receive real-time updates
func (m *Manager) pollSessions() {
	// Use aggregateAndUpdateSessions which handles both WebSocket and polling targets
	m.aggregateAndUpdateSessions()
}

// recalculateBandwidth calculates and applies the appropriate bandwidth limit
func (m *Manager) recalculateBandwidth() {
	m.mu.Lock()
	config := m.config
	sessionCount := len(m.activeSessions)
	m.mu.Unlock()

	// Start with max bandwidth (or schedule-based limit)
	// MaxBandwidth is always applied as the baseline, even when throttling is disabled
	limit := config.MaxBandwidth

	if !config.Enabled {
		// Throttling disabled - only apply MaxBandwidth as a ceiling (no session-based reduction)
		m.mu.Lock()
		oldLimit := m.currentLimit
		m.currentLimit = limit
		m.skippingUploads = false
		m.mu.Unlock()

		// Apply to rclone if limit changed
		if limit != oldLimit {
			m.applyBandwidthLimit(limit)
		}
		return
	}

	// Check for time-based schedule
	scheduleLimit := m.getScheduleLimit()
	if scheduleLimit > 0 && (limit == 0 || scheduleLimit < limit) {
		limit = scheduleLimit
	}

	// Subtract bandwidth for active streams
	if sessionCount > 0 && config.StreamBandwidth > 0 {
		streamReservation := int64(sessionCount) * config.StreamBandwidth
		if limit > 0 {
			limit -= streamReservation
		} else {
			// If unlimited, just set to stream reservation as ceiling
			limit = -streamReservation // Negative means reduce from unlimited
		}
	}

	// Ensure minimum bandwidth
	if limit > 0 && limit < config.MinBandwidth {
		limit = config.MinBandwidth
	}
	if limit < 0 {
		// For negative (reduce from unlimited), convert to absolute limit
		// This is a heuristic - assume we want at least some upload
		limit = config.MinBandwidth
	}

	// Check if we should skip new uploads (before applying min bandwidth floor)
	// Skip threshold must be >= min bandwidth to have any effect
	skipping := config.SkipUploadsBelow > 0 && limit <= config.SkipUploadsBelow

	m.mu.Lock()
	oldLimit := m.currentLimit
	oldSkipping := m.skippingUploads
	m.currentLimit = limit
	m.skippingUploads = skipping
	m.mu.Unlock()

	// Log skip state changes
	if skipping != oldSkipping {
		if skipping {
			log.Info().
				Int64("limit", limit).
				Int64("threshold", config.SkipUploadsBelow).
				Msg("Bandwidth below threshold, skipping new uploads")
		} else {
			log.Info().
				Int64("limit", limit).
				Int64("threshold", config.SkipUploadsBelow).
				Msg("Bandwidth above threshold, resuming new uploads")
		}
	}

	// Apply to rclone if limit changed
	if limit != oldLimit {
		m.applyBandwidthLimit(limit)
		m.broadcastEvent(sse.EventThrottleChanged, map[string]any{
			"limit":            limit,
			"session_count":    sessionCount,
			"skipping_uploads": skipping,
		})
	}
}

// getScheduleLimit returns the bandwidth limit based on current time and schedules
func (m *Manager) getScheduleLimit() int64 {
	now := time.Now()
	currentHour := now.Hour()
	currentDay := now.Weekday()

	for _, schedule := range m.config.Schedules {
		// Check if current day matches
		if len(schedule.Days) > 0 {
			dayMatch := slices.Contains(schedule.Days, currentDay)
			if !dayMatch {
				continue
			}
		}

		// Check if current hour is within schedule
		if schedule.StartHour <= schedule.EndHour {
			// Normal range (e.g., 9-17)
			if currentHour >= schedule.StartHour && currentHour < schedule.EndHour {
				return schedule.MaxBandwidth
			}
		} else {
			// Overnight range (e.g., 22-6)
			if currentHour >= schedule.StartHour || currentHour < schedule.EndHour {
				return schedule.MaxBandwidth
			}
		}
	}

	return 0 // No schedule applies
}

// applyBandwidthLimit sets the rclone bandwidth limit
func (m *Manager) applyBandwidthLimit(limitBytes int64) {
	if m.rcloneMgr == nil || !m.rcloneMgr.IsRunning() {
		return
	}

	client := m.rcloneMgr.Client()
	if client == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var limitStr string
	if limitBytes <= 0 {
		limitStr = "off"
	} else {
		// Convert to human-readable format for rclone
		limitStr = formatBandwidth(limitBytes)
	}

	if err := client.SetBandwidthLimit(ctx, limitStr); err != nil {
		log.Error().Err(err).Str("limit", limitStr).Msg("Failed to set bandwidth limit")
	} else {
		log.Info().
			Str("limit", limitStr).
			Int64("bytes_per_sec", limitBytes).
			Msg("Bandwidth limit updated")
	}
}

// formatBandwidth converts bytes/sec to rclone bandwidth format
func formatBandwidth(bytesPerSec int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytesPerSec >= GB:
		return fmt.Sprintf("%.1fG", float64(bytesPerSec)/float64(GB))
	case bytesPerSec >= MB:
		return fmt.Sprintf("%.1fM", float64(bytesPerSec)/float64(MB))
	case bytesPerSec >= KB:
		return fmt.Sprintf("%.1fK", float64(bytesPerSec)/float64(KB))
	default:
		return fmt.Sprintf("%d", bytesPerSec)
	}
}

// Stats returns throttle statistics
type Stats struct {
	Enabled          bool
	SessionCount     int
	CurrentLimit     int64
	MaxBandwidth     int64
	StreamBandwidth  int64
	MinBandwidth     int64
	SkipUploadsBelow int64
	SkippingUploads  bool
}

// Stats returns current throttle statistics
func (m *Manager) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Stats{
		Enabled:          m.config.Enabled,
		SessionCount:     len(m.activeSessions),
		CurrentLimit:     m.currentLimit,
		MaxBandwidth:     m.config.MaxBandwidth,
		StreamBandwidth:  m.config.StreamBandwidth,
		MinBandwidth:     m.config.MinBandwidth,
		SkipUploadsBelow: m.config.SkipUploadsBelow,
		SkippingUploads:  m.skippingUploads,
	}
}

// handleWebSocketSessions processes session updates from a WebSocket watcher
func (m *Manager) handleWebSocketSessions(targetID int64, targetName string, sessions []targets.Session) {
	// Store sessions for this target
	m.wsSessionsMu.Lock()
	m.wsSessions[targetID] = sessions
	m.wsSessionsMu.Unlock()

	// Aggregate all sessions and update
	m.aggregateAndUpdateSessions()
}

// aggregateAndUpdateSessions aggregates sessions from all sources (WebSocket + polling) and updates state
func (m *Manager) aggregateAndUpdateSessions() {
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	var allSessions []*database.ActiveSession

	// Get all enabled targets
	dbTargets, err := m.db.ListEnabledTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets for session aggregation")
		return
	}

	// Collect sessions from WebSocket watchers
	m.wsSessionsMu.RLock()
	for _, dbTarget := range dbTargets {
		if sessions, exists := m.wsSessions[dbTarget.ID]; exists {
			serverID := fmt.Sprintf("%d", dbTarget.ID)
			for _, s := range sessions {
				dbSession := &database.ActiveSession{
					ID:          fmt.Sprintf("%s-%s-%s", dbTarget.Type, serverID, s.ID),
					ServerType:  string(dbTarget.Type),
					ServerID:    serverID,
					Username:    s.Username,
					MediaTitle:  s.MediaTitle,
					MediaType:   s.MediaType,
					Format:      s.Format,
					Bitrate:     s.Bitrate,
					Transcoding: s.Transcoding,
					Player:      s.Player,
					UpdatedAt:   time.Now(),
				}
				allSessions = append(allSessions, dbSession)
			}
		}
	}
	m.wsSessionsMu.RUnlock()

	// For targets using polling mode, get sessions from the last poll
	// (polling is handled in pollSessions, we just need to include those in our total)
	for _, dbTarget := range dbTargets {
		if m.isTargetUsingWebSocket(dbTarget) {
			continue // Already handled above
		}

		// Get sessions from polling for this target
		targetInstance, err := m.targetsMgr.GetTargetForDBTarget(dbTarget)
		if err != nil {
			continue
		}

		if !targetInstance.SupportsSessionMonitoring() {
			continue
		}

		sessions, err := targetInstance.GetSessions(ctx)
		if err != nil {
			log.Debug().Err(err).Str("target", dbTarget.Name).Msg("Failed to get sessions from target")
			continue
		}

		serverID := fmt.Sprintf("%d", dbTarget.ID)
		for _, s := range sessions {
			dbSession := &database.ActiveSession{
				ID:          fmt.Sprintf("%s-%s-%s", dbTarget.Type, serverID, s.ID),
				ServerType:  string(dbTarget.Type),
				ServerID:    serverID,
				Username:    s.Username,
				MediaTitle:  s.MediaTitle,
				MediaType:   s.MediaType,
				Format:      s.Format,
				Bitrate:     s.Bitrate,
				Transcoding: s.Transcoding,
				Player:      s.Player,
				UpdatedAt:   time.Now(),
			}
			allSessions = append(allSessions, dbSession)
		}
	}

	// Upsert all sessions to database
	for _, session := range allSessions {
		if err := m.db.UpsertActiveSession(session); err != nil {
			log.Error().Err(err).Str("session_id", session.ID).Msg("Failed to upsert session")
		}
	}

	// Sync database - delete sessions that are no longer active
	currentIDs := make(map[string]bool)
	for _, s := range allSessions {
		currentIDs[s.ID] = true
	}

	if dbSessions, err := m.db.ListActiveSessions(); err == nil {
		for _, dbSession := range dbSessions {
			if !currentIDs[dbSession.ID] {
				if err := m.db.DeleteActiveSession(dbSession.ID); err != nil {
					log.Error().Err(err).Str("session_id", dbSession.ID).Msg("Failed to delete inactive session")
				} else {
					log.Debug().Str("session_id", dbSession.ID).Msg("Deleted inactive session")
				}
			}
		}
	}

	// Update local cache and check for changes
	m.mu.Lock()
	oldSessionCount := len(m.activeSessions)
	m.activeSessions = allSessions
	newSessionCount := len(allSessions)
	m.mu.Unlock()

	// Broadcast session change events if count changed
	if newSessionCount != oldSessionCount {
		if newSessionCount > oldSessionCount {
			m.broadcastEvent(sse.EventSessionStarted, map[string]any{"session_count": newSessionCount})
		} else {
			m.broadcastEvent(sse.EventSessionEnded, map[string]any{"session_count": newSessionCount})
		}
	}

	// Recalculate bandwidth based on new session count
	m.recalculateBandwidth()

	if len(allSessions) > 0 {
		log.Debug().Int("count", len(allSessions)).Msg("Active sessions updated")
	}
}

// isTargetUsingWebSocket checks if a target is configured to use WebSocket mode
func (m *Manager) isTargetUsingWebSocket(dbTarget *database.Target) bool {
	// Check if target is configured for WebSocket mode (default is WebSocket)
	sessionMode := dbTarget.Config.SessionMode
	if sessionMode == "" {
		sessionMode = database.SessionModeWebSocket // Default to WebSocket
	}
	return sessionMode == database.SessionModeWebSocket
}

// RefreshWebSocketWatchers refreshes WebSocket watchers based on current target configurations
// Call this when target configurations change
func (m *Manager) RefreshWebSocketWatchers() {
	// Clear stale session data for targets that may have been removed
	dbTargets, err := m.db.ListEnabledTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets for session cleanup")
		return
	}

	// Create map of current target IDs
	currentTargets := make(map[int64]bool)
	for _, t := range dbTargets {
		currentTargets[t.ID] = true
	}

	// Clear session data for targets that no longer exist
	m.wsSessionsMu.Lock()
	for targetID := range m.wsSessions {
		if !currentTargets[targetID] {
			delete(m.wsSessions, targetID)
		}
	}
	m.wsSessionsMu.Unlock()
}

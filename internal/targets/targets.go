package targets

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// Target is the interface that all media server targets must implement
type Target interface {
	// Name returns the target name for logging
	Name() string

	// Type returns the target type (plex, emby, jellyfin)
	Type() database.TargetType

	// Scan triggers a library scan for the given path
	Scan(ctx context.Context, path string) error

	// TestConnection verifies the connection to the media server
	TestConnection(ctx context.Context) error

	// GetLibraries returns available libraries for configuration
	GetLibraries(ctx context.Context) ([]Library, error)

	// GetSessions returns active playback sessions
	// Returns nil, nil for targets that don't support session monitoring
	GetSessions(ctx context.Context) ([]Session, error)

	// SupportsSessionMonitoring returns true if this target supports session monitoring
	// Only media servers (Plex, Emby, Jellyfin) support this; Tdarr and Autoplow do not
	SupportsSessionMonitoring() bool
}

// SessionWatcher is an optional interface for real-time session monitoring via WebSocket
// Targets that support WebSocket connections can implement this interface
type SessionWatcher interface {
	// SupportsWebSocket returns true if this target supports WebSocket monitoring
	SupportsWebSocket() bool

	// WatchSessions starts a WebSocket connection and calls the callback on session changes
	// The callback receives the updated list of sessions whenever there's a change
	// The function blocks until the context is cancelled or an unrecoverable error occurs
	// It should handle reconnection internally with exponential backoff
	WatchSessions(ctx context.Context, callback func(sessions []Session)) error
}

// ScanCompletionWaiter is an optional interface for targets that can detect scan completion
// via activity monitoring (e.g., Plex WebSocket notifications)
type ScanCompletionWaiter interface {
	// WaitForScanCompletion waits for a library scan to complete for the given path.
	// It attempts to match the scan by item name (extracted from path).
	// Returns nil when complete or timed out, or context error if cancelled.
	WaitForScanCompletion(ctx context.Context, path string, timeout time.Duration) error
}

// ScanCompletionPreparer registers scan tracking before a scan is triggered.
type ScanCompletionPreparer interface {
	PrepareScanCompletion(path string) error
}

// ScanCompletionCanceler removes prepared scan tracking when a scan fails.
type ScanCompletionCanceler interface {
	CancelPreparedScanCompletion(path string)
}

// Session represents an active playback session on a media server
type Session struct {
	ID          string // Unique session ID
	Username    string // User playing the media
	MediaTitle  string // Title of media being played
	MediaType   string // movie, episode, track, etc.
	Format      string // Video format, e.g. "1080p (H.264)"
	Bitrate     int64  // Streaming bitrate in bits per second
	Transcoding bool   // Whether the stream is being transcoded
	Player      string // Player/client name
}

// Library represents a media server library
type Library struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Type  string   `json:"type"`            // movie, show, music, etc.
	Path  string   `json:"path,omitempty"`  // First path (for backwards compatibility)
	Paths []string `json:"paths,omitempty"` // All paths for this library
}

// ScanResult represents the result of a scan operation
type ScanResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ScanCompletionInfo holds information needed to track scan completion on a target
type ScanCompletionInfo struct {
	TargetID    int64
	TargetName  string
	ScanPath    string
	TimeoutSecs int
	Target      Target // The actual target instance for smart wait support
}

// ScanAllResult holds the results from ScanAll including completion info for scan tracking
type ScanAllResult struct {
	Results         []ScanResult
	CompletionInfos []ScanCompletionInfo
}

// SessionCallback is called when sessions are updated from a WebSocket watcher
type SessionCallback func(targetID int64, targetName string, sessions []Session)

// Manager manages multiple targets
type Manager struct {
	db      *database.Manager
	targets map[int64]Target

	// WebSocket management
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	wsWatchers       map[int64]context.CancelFunc
	wsWatchersMu     sync.Mutex
	sessionCallbacks []SessionCallback
	sessionCbMu      sync.RWMutex
}

// NewManager creates a new target manager
func NewManager(db *database.Manager) *Manager {
	return &Manager{
		db:         db,
		targets:    make(map[int64]Target),
		wsWatchers: make(map[int64]context.CancelFunc),
	}
}

// GetTarget returns a target for the given target ID, creating one if needed
func (m *Manager) GetTarget(targetID int64) (Target, error) {
	// Check cache
	if target, ok := m.targets[targetID]; ok {
		return target, nil
	}

	// Get target from database
	dbTarget, err := m.db.GetTarget(targetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get target: %w", err)
	}
	if dbTarget == nil {
		return nil, fmt.Errorf("target not found: %d", targetID)
	}

	// Create appropriate target
	target, err := m.createTarget(dbTarget)
	if err != nil {
		return nil, err
	}

	// Cache target
	m.targets[targetID] = target

	return target, nil
}

// GetTargetAny returns a target for the given target ID as an interface{} (any)
// This allows callers to type-assert to specific interfaces like matcharr.TargetFixer
func (m *Manager) GetTargetAny(targetID int64) (any, error) {
	return m.GetTarget(targetID)
}

// GetTargetForDBTarget returns a target for the given database target
func (m *Manager) GetTargetForDBTarget(dbTarget *database.Target) (Target, error) {
	// Check cache
	if target, ok := m.targets[dbTarget.ID]; ok {
		return target, nil
	}

	// Create appropriate target
	target, err := m.createTarget(dbTarget)
	if err != nil {
		return nil, err
	}

	// Cache target
	m.targets[dbTarget.ID] = target

	return target, nil
}

// createTarget creates a target based on target type
func (m *Manager) createTarget(dbTarget *database.Target) (Target, error) {
	switch dbTarget.Type {
	case database.TargetTypePlex:
		return NewPlexTarget(dbTarget), nil
	case database.TargetTypeEmby:
		return NewEmbyTarget(dbTarget), nil
	case database.TargetTypeJellyfin:
		return NewJellyfinTarget(dbTarget), nil
	case database.TargetTypeAutoplow:
		return NewAutoplowTarget(dbTarget), nil
	case database.TargetTypeTdarr:
		return NewTdarrTarget(dbTarget), nil
	default:
		return nil, fmt.Errorf("unknown target type: %s", dbTarget.Type)
	}
}

// InvalidateCache removes a target from the cache (call when target is updated)
func (m *Manager) InvalidateCache(targetID int64) {
	delete(m.targets, targetID)
}

// ScanTarget triggers a scan on a single target for the given path.
// triggerName is optional - if provided, targets with this trigger in their exclude list will be skipped.
// Returns scan result and optional completion info for Plex scan tracking.
func (m *Manager) ScanTarget(ctx context.Context, targetID int64, path string, triggerName string) (ScanResult, *ScanCompletionInfo) {
	dbTarget, err := m.db.GetTarget(targetID)
	if err != nil {
		return ScanResult{
			Success: false,
			Message: fmt.Sprintf("target %d", targetID),
			Error:   err.Error(),
		}, nil
	}
	if dbTarget == nil {
		return ScanResult{
			Success: false,
			Message: fmt.Sprintf("target %d", targetID),
			Error:   "target not found",
		}, nil
	}
	if !dbTarget.Enabled {
		return ScanResult{
			Success: false,
			Message: dbTarget.Name,
			Error:   "target disabled",
		}, nil
	}

	return m.scanTarget(ctx, dbTarget, path, triggerName)
}

func (m *Manager) scanTarget(ctx context.Context, dbTarget *database.Target, path string, triggerName string) (ScanResult, *ScanCompletionInfo) {
	// Check if trigger should be excluded for this target
	if triggerName != "" && dbTarget.Config.ShouldExcludeTrigger(triggerName) {
		log.Debug().
			Str("target", dbTarget.Name).
			Str("trigger", triggerName).
			Str("path", path).
			Msg("Trigger excluded by target exclude rules, skipping")
		return ScanResult{
			Success: true,
			Message: dbTarget.Name,
			Error:   "trigger excluded by target rules",
		}, nil
	}

	// Check if path should be excluded for this target
	if dbTarget.Config.ShouldExcludePath(path) {
		log.Debug().
			Str("target", dbTarget.Name).
			Str("path", path).
			Msg("Path excluded by target exclude rules, skipping")
		return ScanResult{
			Success: true,
			Message: dbTarget.Name,
			Error:   "path excluded by target rules",
		}, nil
	}

	target, err := m.GetTargetForDBTarget(dbTarget)
	if err != nil {
		return ScanResult{
			Success: false,
			Message: dbTarget.Name,
			Error:   err.Error(),
		}, nil
	}

	// Apply target-level scan delay if configured
	if dbTarget.Config.ScanDelay > 0 {
		log.Debug().
			Str("target", dbTarget.Name).
			Int("delay_seconds", dbTarget.Config.ScanDelay).
			Str("path", path).
			Msg("Applying target scan delay")

		select {
		case <-ctx.Done():
			return ScanResult{
				Success: false,
				Message: dbTarget.Name,
				Error:   "context cancelled during scan delay",
			}, nil
		case <-time.After(time.Duration(dbTarget.Config.ScanDelay) * time.Second):
			// Delay complete, proceed with scan
		}
	}

	// Apply target-level path mapping before sending to media server
	scanPath := ApplyPathMappings(path, dbTarget.Config.PathMappings)

	log.Trace().
		Int64("target_id", dbTarget.ID).
		Str("target", dbTarget.Name).
		Str("target_type", string(dbTarget.Type)).
		Str("path", path).
		Str("scan_path", scanPath).
		Str("trigger", triggerName).
		Msg("Target scan request")

	prepared := false
	if preparer, ok := target.(ScanCompletionPreparer); ok {
		if err := preparer.PrepareScanCompletion(scanPath); err != nil {
			log.Debug().
				Str("target", dbTarget.Name).
				Str("path", scanPath).
				Err(err).
				Msg("Failed to prepare scan completion tracking")
		} else {
			prepared = true
		}
	}

	if err := target.Scan(ctx, scanPath); err != nil {
		if prepared {
			if canceler, ok := target.(ScanCompletionCanceler); ok {
				canceler.CancelPreparedScanCompletion(scanPath)
			}
		}
		return ScanResult{
			Success: false,
			Message: dbTarget.Name,
			Error:   err.Error(),
		}, nil
	}

	result := ScanResult{
		Success: true,
		Message: dbTarget.Name,
	}

	// Collect completion info for Plex scan tracking (upload-side)
	if _, hasSmartDetection := target.(ScanCompletionWaiter); hasSmartDetection {
		return result, &ScanCompletionInfo{
			TargetID:    dbTarget.ID,
			TargetName:  dbTarget.Name,
			ScanPath:    scanPath,
			TimeoutSecs: 0,
			Target:      target,
		}
	}

	return result, nil
}

// ScanAll triggers a scan on all enabled targets for the given path
// triggerName is optional - if provided, targets with this trigger in their exclude list will be skipped
// Returns scan results and completion info for Plex scan tracking.
func (m *Manager) ScanAll(ctx context.Context, path string, triggerName string) ScanAllResult {
	dbTargets, err := m.db.ListEnabledTargets()
	if err != nil {
		return ScanAllResult{
			Results: []ScanResult{{
				Success: false,
				Error:   fmt.Sprintf("failed to list targets: %v", err),
			}},
		}
	}

	results := make([]ScanResult, 0, len(dbTargets))
	completionInfos := make([]ScanCompletionInfo, 0)

	for _, dbTarget := range dbTargets {
		result, completionInfo := m.scanTarget(ctx, dbTarget, path, triggerName)
		results = append(results, result)
		if completionInfo != nil {
			completionInfos = append(completionInfos, *completionInfo)
		}
	}

	return ScanAllResult{
		Results:         results,
		CompletionInfos: completionInfos,
	}
}

// ApplyPathMappings applies path mapping rules to convert a local path to a media server path
func ApplyPathMappings(path string, mappings []database.TargetPathMapping) string {
	if len(mappings) == 0 {
		return path
	}

	for _, mapping := range mappings {
		if after, ok := strings.CutPrefix(path, mapping.From); ok {
			return mapping.To + after
		}
	}

	return path
}

// RefreshAllLibraryCache fetches and caches libraries for all enabled targets.
// Should be called on application startup.
func (m *Manager) RefreshAllLibraryCache(ctx context.Context) error {
	dbTargets, err := m.db.ListEnabledTargets()
	if err != nil {
		return fmt.Errorf("failed to list targets: %w", err)
	}

	var lastErr error
	refreshed := 0

	for _, dbTarget := range dbTargets {
		// Only refresh targets that can have libraries
		if dbTarget.Type != database.TargetTypePlex &&
			dbTarget.Type != database.TargetTypeEmby &&
			dbTarget.Type != database.TargetTypeJellyfin {
			continue
		}

		if err := m.refreshLibraryCacheForTarget(ctx, dbTarget); err != nil {
			log.Warn().Err(err).
				Int64("target_id", dbTarget.ID).
				Str("target", dbTarget.Name).
				Msg("Failed to refresh library cache for target")
			lastErr = err
			continue
		}
		refreshed++
	}

	log.Info().Int("count", refreshed).Msg("Refreshed library cache for targets")
	return lastErr
}

// refreshLibraryCacheForTarget fetches libraries from a target and updates the cache
func (m *Manager) refreshLibraryCacheForTarget(ctx context.Context, dbTarget *database.Target) error {
	target, err := m.GetTargetForDBTarget(dbTarget)
	if err != nil {
		return fmt.Errorf("failed to get target instance: %w", err)
	}

	libraries, err := target.GetLibraries(ctx)
	if err != nil {
		return fmt.Errorf("failed to get libraries: %w", err)
	}

	// Convert to database format - one entry per path
	var dbLibraries []database.TargetLibrary
	for _, lib := range libraries {
		// Handle libraries with multiple paths
		if len(lib.Paths) > 0 {
			for _, path := range lib.Paths {
				dbLibraries = append(dbLibraries, database.TargetLibrary{
					TargetID:  dbTarget.ID,
					LibraryID: lib.ID,
					Name:      lib.Name,
					Type:      lib.Type,
					Path:      path,
				})
			}
		} else if lib.Path != "" {
			// Fallback to single path field
			dbLibraries = append(dbLibraries, database.TargetLibrary{
				TargetID:  dbTarget.ID,
				LibraryID: lib.ID,
				Name:      lib.Name,
				Type:      lib.Type,
				Path:      lib.Path,
			})
		}
	}

	if err := m.db.SetCachedLibraries(dbTarget.ID, dbLibraries); err != nil {
		return fmt.Errorf("failed to cache libraries: %w", err)
	}

	log.Debug().
		Int64("target_id", dbTarget.ID).
		Str("target", dbTarget.Name).
		Int("library_count", len(libraries)).
		Int("path_count", len(dbLibraries)).
		Msg("Refreshed library cache")

	return nil
}

// RegisterSessionCallback adds a callback to be invoked when sessions are updated
// from WebSocket watchers. Callbacks are invoked synchronously, so they should be fast.
func (m *Manager) RegisterSessionCallback(cb SessionCallback) {
	m.sessionCbMu.Lock()
	m.sessionCallbacks = append(m.sessionCallbacks, cb)
	m.sessionCbMu.Unlock()
}

// StartWebSocketWatchers starts WebSocket watchers for all enabled targets that support it.
// This should be called after the manager is created to enable real-time monitoring.
func (m *Manager) StartWebSocketWatchers(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	dbTargets, err := m.db.ListEnabledTargets()
	if err != nil {
		return fmt.Errorf("failed to list targets: %w", err)
	}

	started := 0
	for _, dbTarget := range dbTargets {
		target, err := m.GetTargetForDBTarget(dbTarget)
		if err != nil {
			log.Warn().Err(err).
				Int64("target_id", dbTarget.ID).
				Str("target", dbTarget.Name).
				Msg("Failed to get target for WebSocket watcher")
			continue
		}

		// Check if target supports WebSocket monitoring
		watcher, ok := target.(SessionWatcher)
		if !ok || !watcher.SupportsWebSocket() {
			continue
		}

		m.startWebSocketWatcher(dbTarget, watcher)
		started++
	}

	if started > 0 {
		log.Info().Int("count", started).Msg("Started WebSocket watchers for targets")
	}

	return nil
}

// startWebSocketWatcher starts a WebSocket watcher for a specific target
func (m *Manager) startWebSocketWatcher(dbTarget *database.Target, watcher SessionWatcher) {
	m.wsWatchersMu.Lock()
	// Check if watcher already exists
	if _, exists := m.wsWatchers[dbTarget.ID]; exists {
		m.wsWatchersMu.Unlock()
		return
	}

	// Create context for this watcher
	watcherCtx, watcherCancel := context.WithCancel(m.ctx)
	m.wsWatchers[dbTarget.ID] = watcherCancel
	m.wsWatchersMu.Unlock()

	// Start WebSocket watcher goroutine
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().
					Interface("panic", r).
					Str("target", dbTarget.Name).
					Msg("WebSocket watcher panicked")
			}
			m.wsWatchersMu.Lock()
			delete(m.wsWatchers, dbTarget.ID)
			m.wsWatchersMu.Unlock()
		}()

		log.Info().
			Str("target", dbTarget.Name).
			Int64("target_id", dbTarget.ID).
			Msg("Starting WebSocket watcher")

		callback := func(sessions []Session) {
			m.dispatchSessions(dbTarget.ID, dbTarget.Name, sessions)
		}

		err := watcher.WatchSessions(watcherCtx, callback)
		if err != nil && watcherCtx.Err() == nil {
			log.Error().
				Err(err).
				Str("target", dbTarget.Name).
				Msg("WebSocket watcher exited with error")
		}

		log.Info().
			Str("target", dbTarget.Name).
			Msg("WebSocket watcher stopped")
	})
}

// dispatchSessions stores sessions to database and invokes all registered session callbacks
func (m *Manager) dispatchSessions(targetID int64, targetName string, sessions []Session) {
	// Get target info for database storage
	dbTarget, err := m.db.GetTarget(targetID)
	if err != nil {
		log.Warn().Err(err).Int64("target_id", targetID).Msg("Failed to get target for session storage")
	} else if dbTarget != nil {
		// Store sessions to database
		m.storeSessions(dbTarget, sessions)
	}

	// Invoke callbacks (e.g., throttle manager for bandwidth calculations)
	m.sessionCbMu.RLock()
	callbacks := m.sessionCallbacks
	m.sessionCbMu.RUnlock()

	for _, cb := range callbacks {
		cb(targetID, targetName, sessions)
	}
}

// storeSessions stores session data to the database
func (m *Manager) storeSessions(dbTarget *database.Target, sessions []Session) {
	serverID := fmt.Sprintf("%d", dbTarget.ID)

	// Build set of current session IDs for this target
	currentIDs := make(map[string]bool)

	// Upsert all sessions
	for _, s := range sessions {
		sessionID := fmt.Sprintf("%s-%s-%s", dbTarget.Type, serverID, s.ID)
		currentIDs[sessionID] = true

		dbSession := &database.ActiveSession{
			ID:          sessionID,
			ServerType:  string(dbTarget.Type),
			ServerID:    serverID,
			Username:    s.Username,
			MediaTitle:  s.MediaTitle,
			MediaType:   s.MediaType,
			Format:      s.Format,
			Bitrate:     s.Bitrate,
			Transcoding: s.Transcoding,
			Player:      s.Player,
		}

		if err := m.db.UpsertActiveSession(dbSession); err != nil {
			log.Error().Err(err).Str("session_id", sessionID).Msg("Failed to upsert session")
		}
	}

	// Delete sessions for this target that are no longer active
	if dbSessions, err := m.db.ListActiveSessions(); err == nil {
		for _, dbSession := range dbSessions {
			// Only delete sessions belonging to this target
			if dbSession.ServerID == serverID && !currentIDs[dbSession.ID] {
				if err := m.db.DeleteActiveSession(dbSession.ID); err != nil {
					log.Error().Err(err).Str("session_id", dbSession.ID).Msg("Failed to delete inactive session")
				}
			}
		}
	}
}

// StopWebSocketWatchers stops all WebSocket watchers
func (m *Manager) StopWebSocketWatchers() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// RestartWebSocketWatcher restarts the WebSocket watcher for a specific target.
// This is useful after target configuration changes.
func (m *Manager) RestartWebSocketWatcher(targetID int64) error {
	// Stop existing watcher if any
	m.wsWatchersMu.Lock()
	if cancel, exists := m.wsWatchers[targetID]; exists {
		cancel()
		delete(m.wsWatchers, targetID)
	}
	m.wsWatchersMu.Unlock()

	// Invalidate cache to get fresh target
	m.InvalidateCache(targetID)

	// Get target from database
	dbTarget, err := m.db.GetTarget(targetID)
	if err != nil {
		return fmt.Errorf("failed to get target: %w", err)
	}
	if dbTarget == nil || !dbTarget.Enabled {
		return nil // Target doesn't exist or is disabled
	}

	target, err := m.GetTargetForDBTarget(dbTarget)
	if err != nil {
		return fmt.Errorf("failed to get target instance: %w", err)
	}

	// Check if target supports WebSocket monitoring
	watcher, ok := target.(SessionWatcher)
	if !ok || !watcher.SupportsWebSocket() {
		return nil
	}

	m.startWebSocketWatcher(dbTarget, watcher)
	return nil
}

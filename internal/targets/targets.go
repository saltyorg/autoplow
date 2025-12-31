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

// ScanCompletionInfo holds information needed to wait for scan completion on a target
type ScanCompletionInfo struct {
	TargetID    int64
	TargetName  string
	ScanPath    string
	TimeoutSecs int
	Target      Target // The actual target instance for smart wait support
}

// ScanAllResult holds the results from ScanAll including completion info for upload delay
type ScanAllResult struct {
	Results         []ScanResult
	CompletionInfos []ScanCompletionInfo
}

// Manager manages multiple targets
type Manager struct {
	db      *database.DB
	targets map[int64]Target
}

// NewManager creates a new target manager
func NewManager(db *database.DB) *Manager {
	return &Manager{
		db:      db,
		targets: make(map[int64]Target),
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

// ScanAll triggers a scan on all enabled targets for the given path
// triggerName is optional - if provided, targets with this trigger in their exclude list will be skipped
// Returns scan results and completion info for targets that need upload delay waiting
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
		// Check if trigger should be excluded for this target
		if triggerName != "" && dbTarget.Config.ShouldExcludeTrigger(triggerName) {
			log.Debug().
				Str("target", dbTarget.Name).
				Str("trigger", triggerName).
				Str("path", path).
				Msg("Trigger excluded by target exclude rules, skipping")
			results = append(results, ScanResult{
				Success: true,
				Message: dbTarget.Name,
				Error:   "trigger excluded by target rules",
			})
			continue
		}

		// Check if path should be excluded for this target
		if dbTarget.Config.ShouldExcludePath(path) {
			log.Debug().
				Str("target", dbTarget.Name).
				Str("path", path).
				Msg("Path excluded by target exclude rules, skipping")
			results = append(results, ScanResult{
				Success: true,
				Message: dbTarget.Name,
				Error:   "path excluded by target rules",
			})
			continue
		}

		target, err := m.GetTargetForDBTarget(dbTarget)
		if err != nil {
			results = append(results, ScanResult{
				Success: false,
				Message: dbTarget.Name,
				Error:   err.Error(),
			})
			continue
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
				results = append(results, ScanResult{
					Success: false,
					Message: dbTarget.Name,
					Error:   "context cancelled during scan delay",
				})
				continue
			case <-time.After(time.Duration(dbTarget.Config.ScanDelay) * time.Second):
				// Delay complete, proceed with scan
			}
		}

		// Apply target-level path mapping before sending to media server
		scanPath := ApplyPathMappings(path, dbTarget.Config.PathMappings)

		if err := target.Scan(ctx, scanPath); err != nil {
			results = append(results, ScanResult{
				Success: false,
				Message: dbTarget.Name,
				Error:   err.Error(),
			})
		} else {
			results = append(results, ScanResult{
				Success: true,
				Message: dbTarget.Name,
			})

			// Collect completion info for targets with ScanCompletionSeconds configured
			// The actual waiting happens in WaitForAllCompletions() after all scans are triggered
			if dbTarget.Config.ScanCompletionSeconds > 0 {
				completionInfos = append(completionInfos, ScanCompletionInfo{
					TargetID:    dbTarget.ID,
					TargetName:  dbTarget.Name,
					ScanPath:    scanPath,
					TimeoutSecs: dbTarget.Config.ScanCompletionSeconds,
					Target:      target,
				})
			}
		}
	}

	return ScanAllResult{
		Results:         results,
		CompletionInfos: completionInfos,
	}
}

// WaitForAllCompletions waits for all scan completions in parallel.
// For Plex targets, uses smart WebSocket-based detection that can complete early.
// For other targets, uses fixed delay based on ScanCompletionSeconds.
// This controls the upload delay - uploads are queued after all waits complete.
func (m *Manager) WaitForAllCompletions(ctx context.Context, infos []ScanCompletionInfo) {
	if len(infos) == 0 {
		return
	}

	// Find max timeout for overall deadline
	maxTimeout := 0
	for _, info := range infos {
		if info.TimeoutSecs > maxTimeout {
			maxTimeout = info.TimeoutSecs
		}
	}

	// Create context with overall timeout
	ctx, cancel := context.WithTimeout(ctx, time.Duration(maxTimeout)*time.Second)
	defer cancel()

	log.Debug().
		Int("target_count", len(infos)).
		Int("max_timeout_secs", maxTimeout).
		Msg("Waiting for scan completions (upload delay)")

	var wg sync.WaitGroup
	for _, info := range infos {
		wg.Add(1)
		go func(info ScanCompletionInfo) {
			defer wg.Done()

			timeout := time.Duration(info.TimeoutSecs) * time.Second

			// Use smart completion detection if target supports it (e.g., Plex)
			if waiter, ok := info.Target.(ScanCompletionWaiter); ok {
				log.Debug().
					Str("target", info.TargetName).
					Str("path", info.ScanPath).
					Int("timeout_secs", info.TimeoutSecs).
					Msg("Waiting for scan completion (smart detection)")

				if err := waiter.WaitForScanCompletion(ctx, info.ScanPath, timeout); err != nil {
					if ctx.Err() == nil {
						log.Info().
							Str("target", info.TargetName).
							Str("path", info.ScanPath).
							Msg("Upload delay ready (wait ended early)")
					}
					return
				}
				log.Info().
					Str("target", info.TargetName).
					Str("path", info.ScanPath).
					Msg("Upload delay ready (scan detected)")
				return
			}

			// Fixed delay for targets without smart completion
			log.Debug().
				Str("target", info.TargetName).
				Str("path", info.ScanPath).
				Int("seconds", info.TimeoutSecs).
				Msg("Waiting for scan completion (fixed delay)")

			select {
			case <-ctx.Done():
				log.Info().
					Str("target", info.TargetName).
					Str("path", info.ScanPath).
					Msg("Upload delay ready (wait ended early)")
			case <-time.After(timeout):
				log.Info().
					Str("target", info.TargetName).
					Str("path", info.ScanPath).
					Msg("Upload delay ready")
			}
		}(info)
	}

	wg.Wait()
	log.Debug().Msg("All scan completion waits finished")
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

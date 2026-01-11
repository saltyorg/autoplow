package scantracker

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/targets"
)

// PlexPathStatus captures scan tracking state for a given path/target.
type PlexPathStatus struct {
	Matched bool
	Pending bool
	Ready   bool
}

// PlexTracker tracks Plex scan completion for upload gating.
type PlexTracker struct {
	db      *database.Manager
	mu      sync.RWMutex
	records map[scanKey]*scanRecord
}

// NewPlexTracker creates a new Plex scan tracker.
func NewPlexTracker(db *database.Manager) *PlexTracker {
	return &PlexTracker{
		db:      db,
		records: make(map[scanKey]*scanRecord),
	}
}

type scanKey struct {
	destinationID int64
	targetID      int64
	scanPath      string // Mapped path used by the target for scan tracking
}

type scanRecord struct {
	pending     bool
	waiting     bool
	completedAt time.Time
}

// TrackScan registers a scan for Plex completion tracking.
func (t *PlexTracker) TrackScan(scan *database.Scan, infos []targets.ScanCompletionInfo) {
	if t == nil || scan == nil || len(infos) == 0 {
		return
	}

	localScanPath := filepath.Clean(scan.Path)
	if localScanPath == "" || localScanPath == "." {
		return
	}

	destinations, err := t.db.ListDestinationsWithPlexTracking()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list Plex tracking destinations")
		return
	}
	if len(destinations) == 0 {
		return
	}

	// Prefer the most specific destination match.
	sort.Slice(destinations, func(i, j int) bool {
		return len(destinations[i].LocalPath) > len(destinations[j].LocalPath)
	})

	dest := matchDestination(destinations, localScanPath)
	if dest == nil || !dest.UsePlexTracking || len(dest.PlexTargets) == 0 {
		return
	}

	infoByTarget := make(map[int64]targets.ScanCompletionInfo, len(infos))
	for _, info := range infos {
		infoByTarget[info.TargetID] = info
	}

	for _, plexTarget := range dest.PlexTargets {
		info, ok := infoByTarget[plexTarget.TargetID]
		if !ok {
			continue
		}
		mappedScanPath := filepath.Clean(info.ScanPath)
		if mappedScanPath == "" || mappedScanPath == "." {
			mappedScanPath = localScanPath
		}
		t.trackScan(dest.ID, plexTarget, mappedScanPath, info)
	}
}

func (t *PlexTracker) trackScan(destinationID int64, plexTarget *database.DestinationPlexTarget, mappedScanPath string, info targets.ScanCompletionInfo) {
	if plexTarget == nil || info.Target == nil {
		return
	}

	const minIdleThreshold = 60 * time.Second

	idleThreshold := time.Duration(plexTarget.IdleThresholdSeconds) * time.Second
	if idleThreshold < minIdleThreshold {
		idleThreshold = minIdleThreshold
	}

	key := scanKey{
		destinationID: destinationID,
		targetID:      plexTarget.TargetID,
		scanPath:      mappedScanPath,
	}

	startWait := false
	t.mu.Lock()
	record := t.records[key]
	if record == nil {
		record = &scanRecord{}
		t.records[key] = record
	}
	record.pending = true
	record.completedAt = time.Time{}
	if !record.waiting {
		record.waiting = true
		startWait = true
	}
	t.mu.Unlock()

	if startWait {
		go t.waitForCompletion(key, info.Target, info.ScanPath, idleThreshold)
	}
}

func (t *PlexTracker) waitForCompletion(key scanKey, target targets.Target, scanPath string, idleThreshold time.Duration) {
	if scanPath == "" || target == nil {
		t.finishRecord(key)
		return
	}

	ctx := context.Background()

	var err error
	if waiter, ok := target.(interface {
		WaitForScanCompletionWithIdle(context.Context, string, time.Duration) error
	}); ok {
		err = waiter.WaitForScanCompletionWithIdle(ctx, scanPath, idleThreshold)
	} else if waiter, ok := target.(interface {
		WaitForScanCompletion(context.Context, string, time.Duration) error
	}); ok {
		err = waiter.WaitForScanCompletion(ctx, scanPath, 0)
	} else {
		err = nil
	}

	if err != nil {
		log.Debug().
			Str("target", target.Name()).
			Str("path", scanPath).
			Err(err).
			Msg("Plex scan completion wait ended with error")
	} else {
		log.Debug().
			Str("target", target.Name()).
			Str("path", scanPath).
			Msg("Plex scan completion detected")
	}

	t.finishRecord(key)
}

func (t *PlexTracker) finishRecord(key scanKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	record := t.records[key]
	if record == nil {
		return
	}
	record.pending = false
	record.waiting = false
	record.completedAt = time.Now()
}

// CheckPath returns the readiness status for a path on a destination/target pair.
func (t *PlexTracker) CheckPath(destinationID int64, targetID int64, localPath string) PlexPathStatus {
	if t == nil || localPath == "" {
		return PlexPathStatus{}
	}

	path := filepath.Clean(localPath)
	if path == "" || path == "." {
		return PlexPathStatus{}
	}

	mappedPath := t.mapPathForTarget(targetID, path)
	if mappedPath == "" || mappedPath == "." {
		mappedPath = path
	}

	status := PlexPathStatus{}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for key, record := range t.records {
		if key.destinationID != destinationID || key.targetID != targetID {
			continue
		}
		if !isUnderPath(mappedPath, key.scanPath) {
			continue
		}
		status.Matched = true
		if record.pending {
			status.Pending = true
			status.Ready = false
			return status
		}
		status.Ready = true
	}

	return status
}

func (t *PlexTracker) mapPathForTarget(targetID int64, localPath string) string {
	if t == nil || targetID == 0 {
		return localPath
	}

	target, err := t.db.GetTarget(targetID)
	if err != nil {
		log.Error().Err(err).Int64("target_id", targetID).Msg("Failed to load target for path mapping")
		return localPath
	}
	if target == nil || len(target.Config.PathMappings) == 0 {
		return localPath
	}

	return targets.ApplyPathMappings(localPath, target.Config.PathMappings)
}

func matchDestination(destinations []*database.Destination, path string) *database.Destination {
	for _, dest := range destinations {
		if isUnderPath(path, filepath.Clean(dest.LocalPath)) {
			return dest
		}
	}
	return nil
}

func isUnderPath(childPath string, parentPath string) bool {
	if parentPath == "" {
		return false
	}
	rel, err := filepath.Rel(parentPath, childPath)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

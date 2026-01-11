package handlers

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/scantracker"
	"github.com/saltyorg/autoplow/internal/targets"
)

type plexScanInfo struct {
	enabled      bool
	matcher      *plexScanMatcher
	pendingCount int
}

type plexScanMatcher struct {
	pending          []scantracker.PendingScan
	destinationPaths map[int64]string
	targetMappings   map[int64][]database.TargetPathMapping
}

func (h *Handlers) plexScanInfo() plexScanInfo {
	info := plexScanInfo{
		enabled: h.plexTrackingEnabled(),
	}

	if !info.enabled || h.plexTracker == nil {
		return info
	}

	info.matcher = h.newPlexScanMatcher()
	if info.matcher != nil {
		info.pendingCount = info.matcher.PendingCount()
	}

	return info
}

func (h *Handlers) plexTrackingEnabled() bool {
	targetsList, err := h.db.ListEnabledTargets()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list targets for Plex scan tracking check")
		return false
	}

	for _, target := range targetsList {
		if target.Type == database.TargetTypePlex && target.Config.PlexScanTrackingEnabledValue() {
			return true
		}
	}

	return false
}

func (h *Handlers) newPlexScanMatcher() *plexScanMatcher {
	if h.plexTracker == nil {
		return nil
	}

	pending := h.plexTracker.PendingScans()
	if len(pending) == 0 {
		return nil
	}

	destinations, err := h.db.ListDestinationsWithPlexTracking()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list destinations for Plex scan matching")
		return nil
	}

	destinationPaths := make(map[int64]string, len(destinations))
	for _, dest := range destinations {
		destinationPaths[dest.ID] = filepath.Clean(dest.LocalPath)
	}

	targetMappings := make(map[int64][]database.TargetPathMapping)
	seenTargets := make(map[int64]struct{})
	for _, record := range pending {
		if _, ok := seenTargets[record.TargetID]; ok {
			continue
		}
		seenTargets[record.TargetID] = struct{}{}

		target, err := h.db.GetTarget(record.TargetID)
		if err != nil {
			log.Warn().Err(err).Int64("target_id", record.TargetID).Msg("Failed to load Plex target for scan matching")
			continue
		}
		if target == nil {
			continue
		}
		targetMappings[record.TargetID] = target.Config.PathMappings
	}

	return &plexScanMatcher{
		pending:          pending,
		destinationPaths: destinationPaths,
		targetMappings:   targetMappings,
	}
}

func (m *plexScanMatcher) PendingCount() int {
	if m == nil {
		return 0
	}

	seen := make(map[string]struct{})
	for _, record := range m.pending {
		destPath, ok := m.destinationPaths[record.DestinationID]
		if !ok || destPath == "" {
			continue
		}
		scanPath := filepath.Clean(record.ScanPath)
		key := fmt.Sprintf("%d:%s", record.DestinationID, scanPath)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
	}

	return len(seen)
}

func (m *plexScanMatcher) HasPending(localPath string) bool {
	if m == nil || localPath == "" {
		return false
	}

	cleanPath := filepath.Clean(localPath)
	if cleanPath == "" || cleanPath == "." {
		return false
	}

	mappedByTarget := make(map[int64]string)
	for _, record := range m.pending {
		destPath, ok := m.destinationPaths[record.DestinationID]
		if !ok || destPath == "" {
			continue
		}
		if !isUnderPath(cleanPath, destPath) {
			continue
		}

		mapped, ok := mappedByTarget[record.TargetID]
		if !ok {
			mappings := m.targetMappings[record.TargetID]
			if len(mappings) > 0 {
				mapped = targets.ApplyPathMappings(cleanPath, mappings)
			} else {
				mapped = cleanPath
			}
			mappedByTarget[record.TargetID] = mapped
		}

		if mapped == "" || mapped == "." {
			continue
		}
		if isUnderPath(mapped, filepath.Clean(record.ScanPath)) {
			return true
		}
	}

	return false
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

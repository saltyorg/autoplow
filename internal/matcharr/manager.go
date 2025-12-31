package matcharr

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

// TargetGetter provides access to targets for matcharr operations
type TargetGetter interface {
	// GetTargetAny returns a target by ID, allowing type assertion to TargetFixer
	GetTargetAny(targetID int64) (any, error)
}

// Manager manages matcharr comparison and fix operations
type Manager struct {
	db           *database.DB
	targetGetter TargetGetter
	config       ManagerConfig
	cron         *cron.Cron
	cronEntryID  cron.EntryID
	sseBroker    *sse.Broker
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	running      bool
	currentRunID *int64
	isComparing  bool
}

// NewManager creates a new matcharr manager
func NewManager(db *database.DB, targetGetter TargetGetter) *Manager {
	return &Manager{
		db:           db,
		targetGetter: targetGetter,
		config:       DefaultConfig(),
		cron:         cron.New(),
	}
}

// SetSSEBroker sets the SSE broker for broadcasting events
func (m *Manager) SetSSEBroker(broker *sse.Broker) {
	m.sseBroker = broker
}

// broadcastEvent sends an SSE event if the broker is configured
func (m *Manager) broadcastEvent(eventType sse.EventType, data any) {
	if m.sseBroker != nil {
		m.sseBroker.Broadcast(sse.Event{Type: eventType, Data: data})
	}
}

// Start starts the matcharr manager and scheduler
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.running = true

	// Load config from database
	m.loadConfigFromDB()

	// Start cron scheduler
	m.cron.Start()

	// Update schedule if enabled
	if m.config.Enabled && m.config.Schedule != "" {
		if err := m.updateSchedule(m.config.Schedule); err != nil {
			log.Warn().Err(err).Str("schedule", m.config.Schedule).Msg("Failed to set matcharr schedule")
		}
	}

	log.Info().
		Bool("enabled", m.config.Enabled).
		Str("schedule", m.config.Schedule).
		Bool("auto_fix", m.config.AutoFix).
		Msg("Matcharr manager started")

	return nil
}

// Stop stops the matcharr manager and scheduler
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	// Cancel any running comparison
	if m.cancel != nil {
		m.cancel()
	}

	// Stop cron scheduler
	ctx := m.cron.Stop()
	<-ctx.Done()

	m.running = false
	log.Info().Msg("Matcharr manager stopped")

	return nil
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// Status returns the current manager status
func (m *Manager) Status() ManagerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := ManagerStatus{
		Running:      m.running,
		Enabled:      m.config.Enabled,
		Schedule:     m.config.Schedule,
		AutoFix:      m.config.AutoFix,
		IsComparing:  m.isComparing,
		CurrentRunID: m.currentRunID,
	}

	// Get last run time from database
	if lastRun, err := m.db.GetLatestMatcharrRun(); err == nil && lastRun != nil {
		status.LastRun = &lastRun.StartedAt
	}

	// Get next scheduled run time from cron
	if m.cronEntryID != 0 {
		entry := m.cron.Entry(m.cronEntryID)
		if !entry.Next.IsZero() {
			status.NextRun = &entry.Next
		}
	}

	return status
}

// GetConfig returns the current configuration
func (m *Manager) GetConfig() ManagerConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// UpdateConfig updates the manager configuration
func (m *Manager) UpdateConfig(config ManagerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldSchedule := m.config.Schedule
	m.config = config

	// Save to database
	m.saveConfigToDB()

	// Update cron schedule if changed
	if config.Schedule != oldSchedule {
		if config.Enabled && config.Schedule != "" {
			if err := m.updateSchedule(config.Schedule); err != nil {
				return fmt.Errorf("invalid schedule: %w", err)
			}
		} else {
			m.removeSchedule()
		}
	} else if !config.Enabled {
		m.removeSchedule()
	} else if config.Enabled && config.Schedule != "" && m.cronEntryID == 0 {
		if err := m.updateSchedule(config.Schedule); err != nil {
			return fmt.Errorf("invalid schedule: %w", err)
		}
	}

	log.Info().
		Bool("enabled", config.Enabled).
		Str("schedule", config.Schedule).
		Bool("auto_fix", config.AutoFix).
		Msg("Matcharr configuration updated")

	return nil
}

// Phase 1 result types - metadata only

// arrMetadataResult holds root folders fetched from an Arr instance
type arrMetadataResult struct {
	arr         *database.MatcharrArr
	rootFolders []RootFolder
	err         error
}

// targetMetadataResult holds library list fetched from a target
type targetMetadataResult struct {
	target    *database.Target
	fixer     TargetFixer
	libraries []Library
	err       error
}

// Phase 2 result type - which libraries to fetch

// libraryFetchSpec specifies a library that needs to be fetched
type libraryFetchSpec struct {
	target  *database.Target
	fixer   TargetFixer
	library Library
}

// Phase 3 result types - actual data

// arrFetchResult holds the result of fetching media from an Arr instance
type arrFetchResult struct {
	arr   *database.MatcharrArr
	media []ArrMedia
	err   error
}

// targetFetchResult holds the result of fetching items from a single library
type targetFetchResult struct {
	target  *database.Target
	fixer   TargetFixer
	library Library
	items   []MediaServerItem
	err     error
}

// RunComparison runs a comparison between all Arr instances and all targets
// If autoFix is true, mismatches will be automatically fixed
func (m *Manager) RunComparison(ctx context.Context, autoFix bool, triggeredBy string) (*RunResult, error) {
	m.mu.Lock()
	if m.isComparing {
		m.mu.Unlock()
		return nil, fmt.Errorf("comparison already in progress")
	}
	m.isComparing = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.isComparing = false
		m.currentRunID = nil
		m.mu.Unlock()
	}()

	startTime := time.Now()
	runLog := NewRunLogger()

	// Create run record
	run := &database.MatcharrRun{
		StartedAt:   startTime,
		Status:      database.MatcharrRunStatusRunning,
		TriggeredBy: triggeredBy,
	}
	if err := m.db.CreateMatcharrRun(run); err != nil {
		return nil, fmt.Errorf("failed to create run record: %w", err)
	}

	m.mu.Lock()
	m.currentRunID = &run.ID
	m.mu.Unlock()

	result := &RunResult{
		RunID: run.ID,
	}

	runLog.Info("Starting matcharr comparison (run_id=%d, auto_fix=%t, triggered_by=%s)", run.ID, autoFix, triggeredBy)
	log.Info().
		Int64("run_id", run.ID).
		Bool("auto_fix", autoFix).
		Str("triggered_by", triggeredBy).
		Msg("Starting matcharr comparison")

	// Broadcast run started event
	m.broadcastEvent(sse.EventMatcharrRunStarted, map[string]any{
		"run_id":       run.ID,
		"started_at":   run.StartedAt,
		"triggered_by": run.TriggeredBy,
	})

	// Get all enabled Arr instances
	arrs, err := m.db.ListEnabledMatcharrArrs()
	if err != nil {
		runLog.Error("Failed to list enabled Arrs: %v", err)
		result.Errors = append(result.Errors, err)
		m.finalizeRun(run, result, startTime, runLog)
		return result, nil
	}
	runLog.Info("Found %d enabled Arr instance(s)", len(arrs))

	// Get all targets that have matcharr enabled (opt-in)
	targets, err := m.db.ListMatcharrEnabledTargets()
	if err != nil {
		runLog.Error("Failed to list matcharr-enabled targets: %v", err)
		result.Errors = append(result.Errors, err)
		m.finalizeRun(run, result, startTime, runLog)
		return result, nil
	}
	runLog.Info("Found %d matcharr-enabled target(s)", len(targets))

	if len(arrs) == 0 {
		runLog.Warn("No enabled Arr instances configured")
	}
	if len(targets) == 0 {
		runLog.Warn("No matcharr-enabled targets configured")
	}

	// Phase 1: Fetch metadata (root folders from Arrs, library lists from Targets)
	runLog.Info("Phase 1: Fetching metadata from all sources")
	var arrMetadata []arrMetadataResult
	var targetMetadata []targetMetadataResult
	var metadataWg sync.WaitGroup

	metadataWg.Add(2)
	go func() {
		defer metadataWg.Done()
		arrMetadata = m.fetchAllArrMetadata(ctx, arrs, runLog)
	}()
	go func() {
		defer metadataWg.Done()
		targetMetadata = m.fetchAllTargetMetadata(ctx, targets, runLog)
	}()
	metadataWg.Wait()

	if ctx.Err() != nil {
		runLog.Error("Context cancelled: %v", ctx.Err())
		result.Errors = append(result.Errors, ctx.Err())
		m.finalizeRun(run, result, startTime, runLog)
		return result, nil
	}

	// Phase 2: Determine which libraries need to be fetched based on path overlap
	runLog.Info("Phase 2: Determining required libraries")
	requiredLibraries, arrLibraryMap := m.determineRequiredLibraries(arrMetadata, targetMetadata, runLog)

	if len(requiredLibraries) == 0 {
		runLog.Warn("No libraries match any Arr root folders - nothing to compare")
		m.finalizeRun(run, result, startTime, runLog)
		return result, nil
	}
	runLog.Info("Found %d libraries to fetch", len(requiredLibraries))

	// Phase 3: Fetch actual data (Arr media + library items)
	runLog.Info("Phase 3: Fetching data from required sources")
	var arrResults []arrFetchResult
	var targetResults []targetFetchResult
	var dataWg sync.WaitGroup

	dataWg.Add(2)
	go func() {
		defer dataWg.Done()
		arrResults = m.fetchAllArrMedia(ctx, arrs, runLog)
	}()
	go func() {
		defer dataWg.Done()
		targetResults = m.fetchLibraryItems(ctx, requiredLibraries, runLog)
	}()
	dataWg.Wait()

	if ctx.Err() != nil {
		runLog.Error("Context cancelled: %v", ctx.Err())
		result.Errors = append(result.Errors, ctx.Err())
		m.finalizeRun(run, result, startTime, runLog)
		return result, nil
	}

	// Phase 4: Run comparisons
	runLog.Info("Phase 4: Running comparisons")

	// Build a map of target results by target ID and library ID for quick lookup
	targetItemsByLib := make(map[string]targetFetchResult) // key: "targetID:libraryID"
	for _, tr := range targetResults {
		if tr.err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("target %s library %s: %w", tr.target.Name, tr.library.Name, tr.err))
			continue
		}
		key := fmt.Sprintf("%d:%s", tr.target.ID, tr.library.ID)
		targetItemsByLib[key] = tr
	}

	for _, arrResult := range arrResults {
		if arrResult.err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("arr %s: %w", arrResult.arr.Name, arrResult.err))
			continue
		}

		arr := arrResult.arr
		arrMedia := arrResult.media

		// Get the libraries this Arr should compare against
		libSpecs, ok := arrLibraryMap[arr.ID]
		if !ok || len(libSpecs) == 0 {
			runLog.Info("No matching libraries for %s - skipping", arr.Name)
			continue
		}

		// Group items by target for comparison
		targetItems := make(map[int64][]MediaServerItem)
		targetFixers := make(map[int64]TargetFixer)
		targetNames := make(map[int64]string)

		for _, spec := range libSpecs {
			key := fmt.Sprintf("%d:%s", spec.target.ID, spec.library.ID)
			if tr, exists := targetItemsByLib[key]; exists {
				targetItems[spec.target.ID] = append(targetItems[spec.target.ID], tr.items...)
				targetFixers[spec.target.ID] = tr.fixer
				targetNames[spec.target.ID] = tr.target.Name
			}
		}

		// Compare against each target
		for targetID, items := range targetItems {
			target := &database.Target{ID: targetID, Name: targetNames[targetID]}
			fixer := targetFixers[targetID]

			runLog.Info("Comparing %s (%d items) -> %s (%d items)",
				arr.Name, len(arrMedia), target.Name, len(items))

			compareResult := CompareArrToTarget(ctx, arr, arrMedia, target, items)
			result.TotalCompared += compareResult.Compared
			result.Errors = append(result.Errors, compareResult.Errors...)

			runLog.Info("Compared %d items, found %d mismatches", compareResult.Compared, len(compareResult.Mismatches))

			// Store mismatches and optionally fix them
			for _, mismatch := range compareResult.Mismatches {
				runLog.Info("MISMATCH: %s - expected %s=%s, got %s",
					mismatch.ArrMedia.Title, mismatch.ExpectedIDType, mismatch.ExpectedID, mismatch.ActualID)

				dbMismatch := &database.MatcharrMismatch{
					RunID:            run.ID,
					ArrID:            arr.ID,
					TargetID:         target.ID,
					ArrType:          arr.Type,
					ArrName:          arr.Name,
					TargetName:       target.Name,
					MediaTitle:       mismatch.ArrMedia.Title,
					MediaPath:        mismatch.ArrMedia.Path,
					ArrIDType:        mismatch.ExpectedIDType,
					ArrIDValue:       mismatch.ExpectedID,
					TargetIDType:     mismatch.ExpectedIDType,
					TargetIDValue:    mismatch.ActualID,
					TargetMetadataID: mismatch.ServerItem.ItemID,
					Status:           database.MatcharrMismatchStatusPending,
				}

				if err := m.db.CreateMatcharrMismatch(dbMismatch); err != nil {
					runLog.Error("Failed to save mismatch for %s: %v", mismatch.ArrMedia.Title, err)
					log.Error().Err(err).Msg("Failed to save mismatch")
					continue
				}

				result.Mismatches = append(result.Mismatches, mismatch)
				result.MismatchesFound++

				// Auto-fix if enabled
				if autoFix {
					runLog.Info("Auto-fixing: %s", mismatch.ArrMedia.Title)
					if err := FixMismatch(ctx, fixer, &mismatch); err != nil {
						runLog.Error("Failed to fix %s: %v", mismatch.ArrMedia.Title, err)
						log.Error().
							Err(err).
							Str("title", mismatch.ArrMedia.Title).
							Msg("Failed to fix mismatch")
						_ = m.db.UpdateMatcharrMismatchStatus(dbMismatch.ID, database.MatcharrMismatchStatusFailed, err.Error())
					} else {
						runLog.Info("Fixed: %s", mismatch.ArrMedia.Title)
						_ = m.db.UpdateMatcharrMismatchStatus(dbMismatch.ID, database.MatcharrMismatchStatusFixed, "")
						_ = m.db.IncrementMatcharrRunFixed(run.ID)
						result.MismatchesFixed++

						// Add delay between fixes to avoid overwhelming the server
						if m.config.DelayBetweenFixes > 0 {
							select {
							case <-ctx.Done():
								m.finalizeRun(run, result, startTime, runLog)
								return result, nil
							case <-time.After(m.config.DelayBetweenFixes):
							}
						}
					}
				}
			}
		}
	}

	m.finalizeRun(run, result, startTime, runLog)
	return result, nil
}

// Phase 1: Metadata fetching functions

// fetchAllArrMetadata fetches root folders from all Arr instances concurrently
func (m *Manager) fetchAllArrMetadata(ctx context.Context, arrs []*database.MatcharrArr, runLog *RunLogger) []arrMetadataResult {
	results := make([]arrMetadataResult, len(arrs))
	var wg sync.WaitGroup

	for i, arr := range arrs {
		wg.Add(1)
		go func(idx int, a *database.MatcharrArr) {
			defer wg.Done()

			runLog.Info("Fetching root folders from Arr: %s (%s)", a.Name, a.Type)

			arrClient := NewArrClient(a.URL, a.APIKey, a.Type)
			rootFolders, err := arrClient.GetRootFolders(ctx)
			if err != nil {
				runLog.Error("Failed to fetch root folders from %s: %v", a.Name, err)
				results[idx] = arrMetadataResult{arr: a, err: err}
				return
			}

			runLog.Info("Fetched %d root folders from %s", len(rootFolders), a.Name)
			results[idx] = arrMetadataResult{arr: a, rootFolders: rootFolders}
		}(i, arr)
	}

	wg.Wait()
	return results
}

// fetchAllTargetMetadata fetches library lists from all targets concurrently
func (m *Manager) fetchAllTargetMetadata(ctx context.Context, targets []*database.Target, runLog *RunLogger) []targetMetadataResult {
	results := make([]targetMetadataResult, len(targets))
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, t *database.Target) {
			defer wg.Done()

			runLog.Info("Fetching library list from target: %s", t.Name)

			// Get target fixer
			targetObj, err := m.targetGetter.GetTargetAny(t.ID)
			if err != nil {
				runLog.Warn("Failed to get target %s: %v", t.Name, err)
				results[idx] = targetMetadataResult{target: t, err: err}
				return
			}

			fixer, ok := targetObj.(TargetFixer)
			if !ok {
				runLog.Debug("Target %s does not support matcharr operations", t.Name)
				results[idx] = targetMetadataResult{target: t, err: fmt.Errorf("target does not support matcharr")}
				return
			}

			// Get library list from cached data
			libraries, err := m.getTargetLibraries(ctx, t)
			if err != nil {
				runLog.Warn("Failed to get libraries for target %s: %v", t.Name, err)
				results[idx] = targetMetadataResult{target: t, fixer: fixer, err: err}
				return
			}

			runLog.Info("Found %d libraries in %s", len(libraries), t.Name)
			results[idx] = targetMetadataResult{target: t, fixer: fixer, libraries: libraries}
		}(i, target)
	}

	wg.Wait()
	return results
}

// Phase 2: Determine required libraries

// determineRequiredLibraries finds which libraries overlap with Arr root folders
// Returns: list of libraries to fetch, and a map of arrID -> []libraryFetchSpec
func (m *Manager) determineRequiredLibraries(
	arrMetadata []arrMetadataResult,
	targetMetadata []targetMetadataResult,
	runLog *RunLogger,
) ([]libraryFetchSpec, map[int64][]libraryFetchSpec) {
	// Track unique libraries to fetch (by target:library key)
	uniqueLibraries := make(map[string]libraryFetchSpec)
	// Map arr ID to the libraries it should compare against
	arrLibraryMap := make(map[int64][]libraryFetchSpec)

	for _, arrMeta := range arrMetadata {
		if arrMeta.err != nil {
			runLog.Warn("Skipping Arr %s due to error: %v", arrMeta.arr.Name, arrMeta.err)
			continue
		}

		arr := arrMeta.arr

		// Apply path mappings to root folders
		var mappedPaths []string
		for _, rf := range arrMeta.rootFolders {
			mappedPath := mapPath(rf.Path, arr.PathMappings)
			mappedPath = normalizePath(mappedPath)
			mappedPaths = append(mappedPaths, mappedPath)
			if mappedPath != rf.Path {
				runLog.Debug("Arr %s: mapped path %s -> %s", arr.Name, rf.Path, mappedPath)
			}
		}

		if len(mappedPaths) == 0 {
			runLog.Warn("Arr %s has no root folders", arr.Name)
			continue
		}

		runLog.Debug("Arr %s mapped root folders: %v", arr.Name, mappedPaths)

		// Check each target's libraries for overlap
		for _, targetMeta := range targetMetadata {
			if targetMeta.err != nil {
				continue
			}

			for _, lib := range targetMeta.libraries {
				libPath := normalizePath(lib.Path)

				// Check if library path overlaps with any mapped root folder
				for _, mappedPath := range mappedPaths {
					if pathsOverlap(libPath, mappedPath) {
						key := fmt.Sprintf("%d:%s", targetMeta.target.ID, lib.ID)
						spec := libraryFetchSpec{
							target:  targetMeta.target,
							fixer:   targetMeta.fixer,
							library: lib,
						}

						// Add to unique set
						if _, exists := uniqueLibraries[key]; !exists {
							uniqueLibraries[key] = spec
							runLog.Info("Library %s/%s (path: %s) matches Arr %s",
								targetMeta.target.Name, lib.Name, lib.Path, arr.Name)
						}

						// Add to arr's library list
						arrLibraryMap[arr.ID] = append(arrLibraryMap[arr.ID], spec)
						break // Don't add same library multiple times for same arr
					}
				}
			}
		}

		if len(arrLibraryMap[arr.ID]) == 0 {
			runLog.Warn("No matching libraries found for Arr %s", arr.Name)
		}
	}

	// Convert unique map to slice
	var result []libraryFetchSpec
	for _, spec := range uniqueLibraries {
		result = append(result, spec)
	}

	return result, arrLibraryMap
}

// pathsOverlap checks if two paths overlap (one contains the other or they're equal)
func pathsOverlap(path1, path2 string) bool {
	if path1 == path2 {
		return true
	}
	if strings.HasPrefix(path1, path2+"/") {
		return true
	}
	if strings.HasPrefix(path2, path1+"/") {
		return true
	}
	return false
}

// Phase 3: Data fetching functions

// fetchAllArrMedia fetches media from all Arr instances concurrently
func (m *Manager) fetchAllArrMedia(ctx context.Context, arrs []*database.MatcharrArr, runLog *RunLogger) []arrFetchResult {
	results := make([]arrFetchResult, len(arrs))
	var wg sync.WaitGroup

	for i, arr := range arrs {
		wg.Add(1)
		go func(idx int, a *database.MatcharrArr) {
			defer wg.Done()

			runLog.Info("Fetching media from Arr: %s (%s)", a.Name, a.Type)

			arrClient := NewArrClient(a.URL, a.APIKey, a.Type)
			media, err := arrClient.GetMedia(ctx)
			if err != nil {
				runLog.Error("Failed to fetch media from Arr %s: %v", a.Name, err)
				log.Error().Err(err).Str("arr", a.Name).Msg("Failed to fetch media from Arr")
				results[idx] = arrFetchResult{arr: a, err: err}
				return
			}

			runLog.Info("Fetched %d media items from %s", len(media), a.Name)
			log.Debug().Str("arr", a.Name).Int("media_count", len(media)).Msg("Fetched media from Arr")

			results[idx] = arrFetchResult{arr: a, media: media}
		}(i, arr)
	}

	wg.Wait()
	return results
}

// fetchLibraryItems fetches items from specified libraries concurrently
func (m *Manager) fetchLibraryItems(ctx context.Context, specs []libraryFetchSpec, runLog *RunLogger) []targetFetchResult {
	results := make([]targetFetchResult, len(specs))
	var wg sync.WaitGroup

	for i, spec := range specs {
		wg.Add(1)
		go func(idx int, s libraryFetchSpec) {
			defer wg.Done()

			runLog.Info("Fetching items from %s/%s (path: %s)", s.target.Name, s.library.Name, s.library.Path)

			items, err := s.fixer.GetLibraryItemsWithProviderIDs(ctx, s.library.ID)
			if err != nil {
				runLog.Warn("Failed to fetch library items from %s/%s: %v", s.target.Name, s.library.Name, err)
				log.Warn().
					Err(err).
					Str("target", s.target.Name).
					Str("library", s.library.Name).
					Msg("Failed to fetch library items")
				results[idx] = targetFetchResult{target: s.target, fixer: s.fixer, library: s.library, err: err}
				return
			}

			runLog.Info("Fetched %d items from %s/%s", len(items), s.target.Name, s.library.Name)
			results[idx] = targetFetchResult{target: s.target, fixer: s.fixer, library: s.library, items: items}
		}(i, spec)
	}

	wg.Wait()
	return results
}

// FixMismatchByID fixes a single mismatch by its database ID
// Allows retrying failed mismatches
func (m *Manager) FixMismatchByID(ctx context.Context, mismatchID int64) error {
	mismatch, err := m.db.GetMatcharrMismatch(mismatchID)
	if err != nil {
		return fmt.Errorf("failed to get mismatch: %w", err)
	}
	if mismatch == nil {
		return fmt.Errorf("mismatch not found")
	}
	// Allow pending or failed (for retry)
	if mismatch.Status != database.MatcharrMismatchStatusPending && mismatch.Status != database.MatcharrMismatchStatusFailed {
		return fmt.Errorf("mismatch is not actionable (status: %s)", mismatch.Status)
	}

	// Get target fixer
	targetObj, err := m.targetGetter.GetTargetAny(mismatch.TargetID)
	if err != nil {
		return fmt.Errorf("failed to get target: %w", err)
	}

	fixer, ok := targetObj.(TargetFixer)
	if !ok {
		return fmt.Errorf("target does not support matcharr")
	}

	// Fix the mismatch
	fixMismatch := &Mismatch{
		ServerItem: MediaServerItem{
			ItemID: mismatch.TargetMetadataID,
		},
		ExpectedIDType: mismatch.ArrIDType,
		ExpectedID:     mismatch.ArrIDValue,
		ArrMedia: ArrMedia{
			Title: mismatch.MediaTitle,
		},
	}

	if err := FixMismatch(ctx, fixer, fixMismatch); err != nil {
		_ = m.db.UpdateMatcharrMismatchStatus(mismatchID, database.MatcharrMismatchStatusFailed, err.Error())
		return err
	}

	_ = m.db.UpdateMatcharrMismatchStatus(mismatchID, database.MatcharrMismatchStatusFixed, "")

	// Increment fixed count in the run
	_ = m.db.IncrementMatcharrRunFixed(mismatch.RunID)

	return nil
}

// SkipMismatch marks a mismatch as skipped
func (m *Manager) SkipMismatch(mismatchID int64) error {
	return m.db.UpdateMatcharrMismatchStatus(mismatchID, database.MatcharrMismatchStatusSkipped, "")
}

// FixAllPending fixes all pending mismatches from the latest run
func (m *Manager) FixAllPending(ctx context.Context) (int, error) {
	mismatches, err := m.db.GetLatestPendingMatcharrMismatches()
	if err != nil {
		return 0, fmt.Errorf("failed to get pending mismatches: %w", err)
	}

	fixed := 0
	for _, mismatch := range mismatches {
		select {
		case <-ctx.Done():
			return fixed, ctx.Err()
		default:
		}

		if err := m.FixMismatchByID(ctx, mismatch.ID); err != nil {
			log.Warn().
				Err(err).
				Int64("mismatch_id", mismatch.ID).
				Str("title", mismatch.MediaTitle).
				Msg("Failed to fix mismatch")
			continue
		}
		fixed++

		// Add delay between fixes
		if m.config.DelayBetweenFixes > 0 {
			select {
			case <-ctx.Done():
				return fixed, ctx.Err()
			case <-time.After(m.config.DelayBetweenFixes):
			}
		}
	}

	return fixed, nil
}

// Library represents a media server library
type Library struct {
	ID   string
	Name string
	Type string
	Path string
}

// getTargetLibraries returns the libraries from a target
func (m *Manager) getTargetLibraries(_ context.Context, target *database.Target) ([]Library, error) {
	// Use cached libraries from database (24 hour max age)
	cached, err := m.db.GetCachedLibraries(target.ID, 24*time.Hour)
	if err != nil {
		return nil, err
	}

	// Deduplicate by library ID
	seen := make(map[string]bool)
	var libraries []Library
	for _, lib := range cached {
		if seen[lib.LibraryID] {
			continue
		}
		seen[lib.LibraryID] = true
		libraries = append(libraries, Library{
			ID:   lib.LibraryID,
			Name: lib.Name,
			Type: lib.Type,
			Path: lib.Path,
		})
	}

	return libraries, nil
}

// finalizeRun updates the run record with final status
func (m *Manager) finalizeRun(run *database.MatcharrRun, result *RunResult, startTime time.Time, runLog *RunLogger) {
	now := time.Now()
	run.CompletedAt = &now
	run.TotalCompared = result.TotalCompared
	run.MismatchesFound = result.MismatchesFound
	run.MismatchesFixed = result.MismatchesFixed
	result.Duration = now.Sub(startTime)

	if len(result.Errors) > 0 {
		run.Status = database.MatcharrRunStatusFailed
		run.Error = result.Errors[0].Error()
		runLog.Error("Run failed: %v", result.Errors[0])
	} else {
		run.Status = database.MatcharrRunStatusCompleted
	}

	runLog.Info("Comparison completed: compared=%d, mismatches=%d, fixed=%d, duration=%v",
		result.TotalCompared, result.MismatchesFound, result.MismatchesFixed, result.Duration)

	// Store logs in the run record
	run.Logs = runLog.String()

	if err := m.db.UpdateMatcharrRun(run); err != nil {
		log.Error().Err(err).Int64("run_id", run.ID).Msg("Failed to update run record")
	}

	log.Info().
		Int64("run_id", run.ID).
		Int("compared", result.TotalCompared).
		Int("mismatches", result.MismatchesFound).
		Int("fixed", result.MismatchesFixed).
		Str("duration", result.Duration.Round(time.Millisecond).String()).
		Msg("Matcharr comparison completed")

	// Clear running state BEFORE broadcasting the event
	// This ensures that when clients fetch status after receiving the SSE event,
	// they see IsComparing=false immediately
	m.mu.Lock()
	m.isComparing = false
	m.currentRunID = nil
	m.mu.Unlock()

	// Broadcast completion event
	eventType := sse.EventMatcharrRunCompleted
	if run.Status == database.MatcharrRunStatusFailed {
		eventType = sse.EventMatcharrRunFailed
	}
	m.broadcastEvent(eventType, map[string]any{
		"run_id":           run.ID,
		"status":           run.Status,
		"total_compared":   run.TotalCompared,
		"mismatches_found": run.MismatchesFound,
		"mismatches_fixed": run.MismatchesFixed,
		"completed_at":     run.CompletedAt,
		"error":            run.Error,
	})
}

// updateSchedule updates the cron schedule
func (m *Manager) updateSchedule(schedule string) error {
	// Remove existing schedule
	m.removeSchedule()

	// Add new schedule
	id, err := m.cron.AddFunc(schedule, m.scheduledRun)
	if err != nil {
		return err
	}

	m.cronEntryID = id
	log.Info().Str("schedule", schedule).Msg("Matcharr schedule updated")
	return nil
}

// removeSchedule removes the current cron schedule
func (m *Manager) removeSchedule() {
	if m.cronEntryID != 0 {
		m.cron.Remove(m.cronEntryID)
		m.cronEntryID = 0
	}
}

// scheduledRun is called by cron to run a scheduled comparison
func (m *Manager) scheduledRun() {
	m.mu.RLock()
	autoFix := m.config.AutoFix
	m.mu.RUnlock()

	log.Info().Bool("auto_fix", autoFix).Msg("Running scheduled matcharr comparison")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if _, err := m.RunComparison(ctx, autoFix, "schedule"); err != nil {
		log.Error().Err(err).Msg("Scheduled matcharr comparison failed")
	}
}

// loadConfigFromDB loads configuration from the database
func (m *Manager) loadConfigFromDB() {
	if v, err := m.db.GetSetting("matcharr.enabled"); err == nil && v != "" {
		m.config.Enabled = v == "true"
	}
	if v, err := m.db.GetSetting("matcharr.schedule"); err == nil {
		m.config.Schedule = v
	}
	if v, err := m.db.GetSetting("matcharr.auto_fix"); err == nil && v != "" {
		m.config.AutoFix = v == "true"
	}
	if v, err := m.db.GetSetting("matcharr.delay_between_fixes"); err == nil && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			m.config.DelayBetweenFixes = time.Duration(i) * time.Second
		}
	}
}

// saveConfigToDB saves configuration to the database
func (m *Manager) saveConfigToDB() {
	_ = m.db.SetSetting("matcharr.enabled", fmt.Sprintf("%t", m.config.Enabled))
	_ = m.db.SetSetting("matcharr.schedule", m.config.Schedule)
	_ = m.db.SetSetting("matcharr.auto_fix", fmt.Sprintf("%t", m.config.AutoFix))
	_ = m.db.SetSetting("matcharr.delay_between_fixes", fmt.Sprintf("%d", int(m.config.DelayBetweenFixes.Seconds())))
}

package matcharr

import (
	"context"
	"fmt"
	"slices"
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
	db           *database.Manager
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
func NewManager(db *database.Manager, targetGetter TargetGetter) *Manager {
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

func (m *Manager) managerContext() context.Context {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ctx
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
	target       *database.Target
	fixer        TargetFixer
	library      Library
	ignoredPaths []string
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

type compareTask struct {
	arr      *database.MatcharrArr
	arrMedia []ArrMedia
	target   *database.Target
	items    []MediaServerItem
	fixer    TargetFixer
}

// RunComparison runs a comparison between all Arr instances and all targets
// If autoFix is true, mismatches will be automatically fixed
func (m *Manager) RunComparison(ctx context.Context, autoFix bool, triggeredBy string) (*RunResult, error) {
	if mgrCtx := m.managerContext(); mgrCtx != nil {
		combinedCtx, cancel := context.WithCancel(ctx)
		ctx = combinedCtx
		defer cancel()
		go func() {
			select {
			case <-mgrCtx.Done():
				cancel()
			case <-combinedCtx.Done():
			}
		}()
	}

	m.mu.RLock()
	delayBetweenFixes := m.config.DelayBetweenFixes
	m.mu.RUnlock()

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
	ignoreSet := make(map[string]struct{})

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

	if ignores, err := m.db.ListMatcharrFileIgnores(); err != nil {
		runLog.Warn("Failed to load file mismatch ignores: %v", err)
	} else {
		for _, ignore := range ignores {
			key := fileIgnoreKey(ignore.ArrType, ignore.ArrMediaID, ignore.TargetID, ignore.SeasonNumber, ignore.EpisodeNumber, ignore.ArrFileName)
			ignoreSet[key] = struct{}{}
		}
	}

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

	metadataWg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Fetching Arr metadata panicked")
			}
		}()
		arrMetadata = m.fetchAllArrMetadata(ctx, arrs, runLog)
	})
	metadataWg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Fetching target metadata panicked")
			}
		}()
		targetMetadata = m.fetchAllTargetMetadata(ctx, targets, runLog)
	})
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

	dataWg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Fetching Arr media panicked")
			}
		}()
		arrResults = m.fetchAllArrMedia(ctx, arrs, runLog)
	})
	dataWg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Fetching library items panicked")
			}
		}()
		targetResults = m.fetchLibraryItems(ctx, requiredLibraries, runLog)
	})
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

	fileLimiters := make(map[int64]chan struct{}, len(targets))
	for _, target := range targets {
		limit := normalizeFileConcurrency(target.Config.MatcharrFileConcurrency)
		fileLimiters[target.ID] = make(chan struct{}, limit)
	}

	var tasks []compareTask
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
		targetConfigs := make(map[int64]*database.Target)

		for _, spec := range libSpecs {
			key := fmt.Sprintf("%d:%s", spec.target.ID, spec.library.ID)
			if tr, exists := targetItemsByLib[key]; exists {
				targetItems[spec.target.ID] = append(targetItems[spec.target.ID], tr.items...)
				targetFixers[spec.target.ID] = tr.fixer
				targetNames[spec.target.ID] = tr.target.Name
				targetConfigs[spec.target.ID] = tr.target
			}
		}

		// Compare against each target
		for targetID, items := range targetItems {
			target := targetConfigs[targetID]
			if target == nil {
				target = &database.Target{ID: targetID, Name: targetNames[targetID]}
			}
			fixer := targetFixers[targetID]

			tasks = append(tasks, compareTask{
				arr:      arr,
				arrMedia: arrMedia,
				target:   target,
				items:    items,
				fixer:    fixer,
			})
		}
	}

	compareConcurrency := 3
	if autoFix {
		compareConcurrency = 1
	}
	if compareConcurrency > len(tasks) {
		compareConcurrency = len(tasks)
	}

	taskCh := make(chan compareTask)
	var wg sync.WaitGroup
	var resultMu sync.Mutex

	worker := func() {
		defer wg.Done()
		for task := range taskCh {
			select {
			case <-ctx.Done():
				return
			default:
			}

			runLog.Info("Comparing %s (%d items) -> %s (%d items)",
				task.arr.Name, len(task.arrMedia), task.target.Name, len(task.items))

			compareResult := CompareArrToTarget(ctx, task.arr, task.arrMedia, task.target, task.items)
			resultMu.Lock()
			result.TotalCompared += compareResult.Compared
			result.Errors = append(result.Errors, compareResult.Errors...)
			resultMu.Unlock()

			runLog.Info("Compared %d items, found %d mismatches", compareResult.Compared, len(compareResult.Mismatches))

			// Store mismatches and optionally fix them
			for _, mismatch := range compareResult.Mismatches {
				runLog.Info("MISMATCH: %s - expected %s=%s, got %s",
					mismatch.ArrMedia.Title, mismatch.ExpectedIDType, mismatch.ExpectedID, mismatch.ActualID)
				runLog.Debug("Paths: arr=%s mapped=%s server=%s",
					mismatch.ArrMedia.Path, mapPath(mismatch.ArrMedia.Path, mismatch.ArrInstance.PathMappings), mismatch.ServerItem.Path)

				dbMismatch := &database.MatcharrMismatch{
					RunID:            run.ID,
					ArrID:            task.arr.ID,
					TargetID:         task.target.ID,
					ArrType:          task.arr.Type,
					ArrName:          task.arr.Name,
					TargetName:       task.target.Name,
					TargetTitle:      mismatch.ServerItem.Title,
					MediaTitle:       mismatch.ArrMedia.Title,
					MediaPath:        mismatch.ArrMedia.Path,
					TargetPath:       mismatch.ServerItem.Path,
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

				resultMu.Lock()
				result.Mismatches = append(result.Mismatches, mismatch)
				result.MismatchesFound++
				resultMu.Unlock()

				// Auto-fix if enabled
				if autoFix {
					runLog.Info("Auto-fixing: %s", mismatch.ArrMedia.Title)
					if err := FixMismatch(ctx, task.fixer, &mismatch); err != nil {
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
						resultMu.Lock()
						result.MismatchesFixed++
						resultMu.Unlock()

						// Add delay between fixes to avoid overwhelming the server
						if delayBetweenFixes > 0 {
							select {
							case <-ctx.Done():
								return
							case <-time.After(delayBetweenFixes):
							}
						}
					}
				}
			}

			// Store missing paths (Arr present, server missing)
			for _, gap := range compareResult.MissingArr {
				dbGap := &database.MatcharrGap{
					RunID:      run.ID,
					ArrID:      task.arr.ID,
					TargetID:   task.target.ID,
					Source:     database.MatcharrGapSourceArr,
					Title:      gap.ArrMedia.Title,
					ArrName:    task.arr.Name,
					TargetName: task.target.Name,
					ArrPath:    gap.ArrMedia.Path,
					TargetPath: mapPath(gap.ArrMedia.Path, task.arr.PathMappings),
				}
				if err := m.db.CreateMatcharrGap(dbGap); err != nil {
					runLog.Warn("Failed to save missing Arr path %s: %v", gap.ArrMedia.Path, err)
					log.Warn().Err(err).Str("arr_path", gap.ArrMedia.Path).Msg("Failed to save missing Arr path")
				}
			}

			// Store missing paths (on server but not in Arr)
			for _, gap := range compareResult.MissingSrv {
				dbGap := &database.MatcharrGap{
					RunID:      run.ID,
					ArrID:      task.arr.ID,
					TargetID:   task.target.ID,
					Source:     database.MatcharrGapSourceTarget,
					Title:      gap.ServerItem.Title,
					ArrName:    task.arr.Name,
					TargetName: task.target.Name,
					ArrPath:    "",
					TargetPath: gap.ServerItem.Path,
				}
				if err := m.db.CreateMatcharrGap(dbGap); err != nil {
					runLog.Warn("Failed to save missing server path %s: %v", gap.ServerItem.Path, err)
					log.Warn().Err(err).Str("server_path", gap.ServerItem.Path).Msg("Failed to save missing server path")
				}
			}

			if fileFetcher, ok := task.fixer.(TargetFileFetcher); ok {
				arrClient := NewArrClient(task.arr.URL, task.arr.APIKey, task.arr.Type)
				m.compareFileMismatches(ctx, run.ID, task.arr, arrClient, task.target, fileFetcher, compareResult.Matches, fileLimiters[task.target.ID], ignoreSet, runLog)
			}
		}
	}

	if compareConcurrency > 0 {
		wg.Add(compareConcurrency)
		for i := 0; i < compareConcurrency; i++ {
			go worker()
		}

		for _, task := range tasks {
			select {
			case <-ctx.Done():
				close(taskCh)
				wg.Wait()
				m.finalizeRun(run, result, startTime, runLog)
				return result, nil
			case taskCh <- task:
			}
		}
		close(taskCh)
		wg.Wait()
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
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Str("arr", arr.Name).Msg("Fetching Arr metadata panicked")
				}
			}()

			runLog.Info("Fetching root folders from Arr: %s (%s)", arr.Name, arr.Type)

			arrClient := NewArrClient(arr.URL, arr.APIKey, arr.Type)
			rootFolders, err := arrClient.GetRootFolders(ctx)
			if err != nil {
				runLog.Error("Failed to fetch root folders from %s: %v", arr.Name, err)
				results[i] = arrMetadataResult{arr: arr, err: err}
				return
			}

			runLog.Info("Fetched %d root folders from %s", len(rootFolders), arr.Name)
			results[i] = arrMetadataResult{arr: arr, rootFolders: rootFolders}
		})
	}

	wg.Wait()
	return results
}

// fetchAllTargetMetadata fetches library lists from all targets concurrently
func (m *Manager) fetchAllTargetMetadata(ctx context.Context, targets []*database.Target, runLog *RunLogger) []targetMetadataResult {
	results := make([]targetMetadataResult, len(targets))
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Str("target", target.Name).Msg("Fetching target metadata panicked")
				}
			}()

			runLog.Info("Fetching library list from target: %s", target.Name)

			// Get target fixer
			targetObj, err := m.targetGetter.GetTargetAny(target.ID)
			if err != nil {
				runLog.Warn("Failed to get target %s: %v", target.Name, err)
				results[i] = targetMetadataResult{target: target, err: err}
				return
			}

			fixer, ok := targetObj.(TargetFixer)
			if !ok {
				runLog.Debug("Target %s does not support matcharr operations", target.Name)
				results[i] = targetMetadataResult{target: target, err: fmt.Errorf("target does not support matcharr")}
				return
			}

			// Get library list from cached data
			libraries, err := m.getTargetLibraries(ctx, target)
			if err != nil {
				runLog.Warn("Failed to get libraries for target %s: %v", target.Name, err)
				results[i] = targetMetadataResult{target: target, fixer: fixer, err: err}
				return
			}

			runLog.Info("Found %d libraries in %s", len(libraries), target.Name)
			results[i] = targetMetadataResult{target: target, fixer: fixer, libraries: libraries}
		})
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
				ignorePaths := normalizeIgnorePaths(targetMeta.target.Config.MatcharrExcludePaths)
				libPaths := filteredLibraryPaths(lib, ignorePaths)
				if len(libPaths) == 0 {
					continue
				}

				libCopy := lib
				libCopy.Paths = libPaths
				if libCopy.Path == "" && len(libPaths) > 0 {
					libCopy.Path = libPaths[0]
				}

				// Check if any library path overlaps with any mapped root folder
				if libraryOverlaps(libPaths, mappedPaths) {
					key := fmt.Sprintf("%d:%s", targetMeta.target.ID, lib.ID)
					spec := libraryFetchSpec{
						target:       targetMeta.target,
						fixer:        targetMeta.fixer,
						library:      libCopy,
						ignoredPaths: ignorePaths,
					}

					// Add to unique set
					if _, exists := uniqueLibraries[key]; !exists {
						uniqueLibraries[key] = spec
						runLog.Info("Library %s/%s (paths: %s) matches Arr %s",
							targetMeta.target.Name, lib.Name, strings.Join(libPaths, ", "), arr.Name)
					}

					// Add to arr's library list
					arrLibraryMap[arr.ID] = append(arrLibraryMap[arr.ID], spec)
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

// libraryOverlaps returns true if any library path overlaps with any mapped Arr root
func libraryOverlaps(libraryPaths, mappedPaths []string) bool {
	for _, libPath := range libraryPaths {
		for _, mappedPath := range mappedPaths {
			if pathsOverlap(libPath, mappedPath) {
				return true
			}
		}
	}
	return false
}

// normalizedLibraryPaths returns all normalized paths for a library, including legacy Path.
func normalizedLibraryPaths(lib Library) []string {
	paths := make([]string, 0, len(lib.Paths)+1)
	for _, p := range lib.Paths {
		if np := normalizePath(p); np != "" && !containsString(paths, np) {
			paths = append(paths, np)
		}
	}
	if lib.Path != "" {
		if np := normalizePath(lib.Path); np != "" && !containsString(paths, np) {
			paths = append(paths, np)
		}
	}
	return paths
}

// filteredLibraryPaths returns normalized library paths with ignored prefixes removed
func filteredLibraryPaths(lib Library, ignored []string) []string {
	if len(ignored) == 0 {
		return normalizedLibraryPaths(lib)
	}

	libPaths := normalizedLibraryPaths(lib)
	var filtered []string
	for _, p := range libPaths {
		if pathIgnored(p, ignored) {
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered
}

// normalizeIgnorePaths normalizes user-provided ignore prefixes
func normalizeIgnorePaths(paths []string) []string {
	var normalized []string
	for _, p := range paths {
		if np := normalizePath(p); np != "" && !containsString(normalized, np) {
			normalized = append(normalized, np)
		}
	}
	return normalized
}

// filterItemsByIgnoredPaths removes media items that live under ignored prefixes
func filterItemsByIgnoredPaths(items []MediaServerItem, ignored []string) []MediaServerItem {
	if len(ignored) == 0 {
		return items
	}

	filtered := items[:0]
	for _, item := range items {
		path := normalizePath(item.Path)
		matchPath := item.MatchPath()
		if pathIgnored(path, ignored) || pathIgnored(matchPath, ignored) {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func targetItemsContainPath(items []MediaServerItem, path string) bool {
	normalized := normalizePath(path)
	if normalized == "" {
		return false
	}

	for _, item := range items {
		if item.MatchPath() == normalized {
			return true
		}
	}

	return false
}

// pathIgnored returns true if path is within an ignored prefix (boundary aware)
func pathIgnored(path string, ignored []string) bool {
	for _, prefix := range ignored {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// pathListForLog returns a human-friendly list of library paths for logging
func pathListForLog(lib Library) string {
	paths := normalizedLibraryPaths(lib)
	if len(paths) == 0 && lib.Path != "" {
		return normalizePath(lib.Path)
	}
	return strings.Join(paths, ", ")
}

// containsString checks if slice contains string s
func containsString(list []string, s string) bool {
	return slices.Contains(list, s)
}

// Phase 3: Data fetching functions

// fetchAllArrMedia fetches media from all Arr instances concurrently
func (m *Manager) fetchAllArrMedia(ctx context.Context, arrs []*database.MatcharrArr, runLog *RunLogger) []arrFetchResult {
	results := make([]arrFetchResult, len(arrs))
	var wg sync.WaitGroup

	for i, arr := range arrs {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Str("arr", arr.Name).Msg("Fetching Arr media panicked")
				}
			}()

			runLog.Info("Fetching media from Arr: %s (%s)", arr.Name, arr.Type)

			arrClient := NewArrClient(arr.URL, arr.APIKey, arr.Type)
			media, err := arrClient.GetMedia(ctx)
			if err != nil {
				runLog.Error("Failed to fetch media from Arr %s: %v", arr.Name, err)
				log.Error().Err(err).Str("arr", arr.Name).Msg("Failed to fetch media from Arr")
				results[i] = arrFetchResult{arr: arr, err: err}
				return
			}

			runLog.Info("Fetched %d media items from %s", len(media), arr.Name)
			log.Debug().Str("arr", arr.Name).Int("media_count", len(media)).Msg("Fetched media from Arr")

			results[i] = arrFetchResult{arr: arr, media: media}
		})
	}

	wg.Wait()
	return results
}

// fetchLibraryItems fetches items from specified libraries concurrently
func (m *Manager) fetchLibraryItems(ctx context.Context, specs []libraryFetchSpec, runLog *RunLogger) []targetFetchResult {
	results := make([]targetFetchResult, len(specs))
	var wg sync.WaitGroup

	for i, spec := range specs {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Str("target", spec.target.Name).Str("library", spec.library.Name).Msg("Fetching library items panicked")
				}
			}()

			runLog.Info("Fetching items from %s/%s (paths: %s)", spec.target.Name, spec.library.Name, pathListForLog(spec.library))

			items, err := spec.fixer.GetLibraryItemsWithProviderIDs(ctx, spec.library.ID)
			if err != nil {
				runLog.Warn("Failed to fetch library items from %s/%s: %v", spec.target.Name, spec.library.Name, err)
				log.Warn().
					Err(err).
					Str("target", spec.target.Name).
					Str("library", spec.library.Name).
					Msg("Failed to fetch library items")
				results[i] = targetFetchResult{target: spec.target, fixer: spec.fixer, library: spec.library, err: err}
				return
			}

			if len(spec.ignoredPaths) > 0 {
				before := len(items)
				items = filterItemsByIgnoredPaths(items, spec.ignoredPaths)
				if removed := before - len(items); removed > 0 {
					runLog.Info("Skipped %d item(s) in %s/%s due to matcharr ignore paths", removed, spec.target.Name, spec.library.Name)
				}
			}

			runLog.Info("Fetched %d items from %s/%s", len(items), spec.target.Name, spec.library.Name)
			results[i] = targetFetchResult{target: spec.target, fixer: spec.fixer, library: spec.library, items: items}
		})
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

	// Notify UI to refresh mismatches
	m.broadcastEvent(sse.EventMatcharrMismatchUpdated, map[string]any{
		"mismatch_id": mismatchID,
		"run_id":      mismatch.RunID,
		"status":      database.MatcharrMismatchStatusFixed,
	})

	return nil
}

// SkipMismatch marks a mismatch as skipped
func (m *Manager) SkipMismatch(mismatchID int64) error {
	if err := m.db.UpdateMatcharrMismatchStatus(mismatchID, database.MatcharrMismatchStatusSkipped, ""); err != nil {
		return err
	}

	if mismatch, _ := m.db.GetMatcharrMismatch(mismatchID); mismatch != nil {
		m.broadcastEvent(sse.EventMatcharrMismatchUpdated, map[string]any{
			"mismatch_id": mismatchID,
			"run_id":      mismatch.RunID,
			"status":      database.MatcharrMismatchStatusSkipped,
		})
	}

	return nil
}

// FixAllPending fixes all pending mismatches from the latest run
func (m *Manager) FixAllPending(ctx context.Context) (int, error) {
	m.mu.RLock()
	delayBetweenFixes := m.config.DelayBetweenFixes
	m.mu.RUnlock()

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
		if delayBetweenFixes > 0 {
			select {
			case <-ctx.Done():
				return fixed, ctx.Err()
			case <-time.After(delayBetweenFixes):
			}
		}
	}

	return fixed, nil
}

// CheckTargetPath verifies whether a target contains an item matching the given path.
// Uses the same library/item parsing logic as matcharr comparisons.
func (m *Manager) CheckTargetPath(ctx context.Context, targetID int64, path string) (bool, error) {
	if m == nil {
		return false, fmt.Errorf("manager not initialized")
	}

	target, err := m.db.GetTarget(targetID)
	if err != nil {
		return false, fmt.Errorf("failed to load target: %w", err)
	}
	if target == nil {
		return false, fmt.Errorf("target not found: %d", targetID)
	}

	targetObj, err := m.targetGetter.GetTargetAny(target.ID)
	if err != nil {
		return false, fmt.Errorf("failed to get target: %w", err)
	}

	fixer, ok := targetObj.(TargetFixer)
	if !ok {
		return false, fmt.Errorf("target does not support matcharr operations")
	}

	normalizedPath := normalizePath(path)
	if normalizedPath == "" {
		return false, fmt.Errorf("empty path")
	}

	libraries, err := m.getTargetLibraries(ctx, target)
	if err != nil {
		return false, err
	}
	if len(libraries) == 0 {
		return false, fmt.Errorf("no cached libraries for target")
	}

	ignored := normalizeIgnorePaths(target.Config.MatcharrExcludePaths)
	for _, lib := range libraries {
		libPaths := filteredLibraryPaths(lib, ignored)
		for _, libPath := range libPaths {
			if !pathsOverlap(normalizedPath, libPath) {
				continue
			}

			items, err := fixer.GetLibraryItemsWithProviderIDs(ctx, lib.ID)
			if err != nil {
				return false, err
			}
			items = filterItemsByIgnoredPaths(items, ignored)

			if targetItemsContainPath(items, normalizedPath) {
				return true, nil
			}
		}
	}

	return false, nil
}

// Library represents a media server library (supports multiple root paths)
type Library struct {
	ID    string
	Name  string
	Type  string
	Path  string   // First path for compatibility/logging
	Paths []string // All root paths for this library
}

// getTargetLibraries returns the libraries from a target
func (m *Manager) getTargetLibraries(_ context.Context, target *database.Target) ([]Library, error) {
	// Use cached libraries from database (24 hour max age)
	cached, err := m.db.GetCachedLibraries(target.ID, 24*time.Hour)
	if err != nil {
		return nil, err
	}

	// Aggregate paths by library ID
	libraryMap := make(map[string]*Library)
	for _, lib := range cached {
		entry, exists := libraryMap[lib.LibraryID]
		if !exists {
			entry = &Library{
				ID:   lib.LibraryID,
				Name: lib.Name,
				Type: lib.Type,
			}
			libraryMap[lib.LibraryID] = entry
		}

		path := normalizePath(lib.Path)
		if path == "" || containsString(entry.Paths, path) {
			continue
		}
		entry.Paths = append(entry.Paths, path)
	}

	var libraries []Library
	for _, lib := range libraryMap {
		if lib.Path == "" && len(lib.Paths) > 0 {
			lib.Path = lib.Paths[0]
		}
		libraries = append(libraries, *lib)
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

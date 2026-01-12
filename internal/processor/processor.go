package processor

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/scantracker"
	"github.com/saltyorg/autoplow/internal/targets"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

// Config holds the processor configuration
type Config struct {
	// MaxRetries is the maximum number of retry attempts for failed scans
	// After this many retries, the scan is marked as permanently failed
	MaxRetries int `json:"max_retries"`

	// CleanupDays is how many days to keep completed/failed scan history
	// Set to 0 to disable cleanup and keep scan history forever
	CleanupDays int `json:"cleanup_days"`

	// PathNotFoundRetries is the number of times to retry when a path doesn't exist
	// Set to 0 to fail immediately (default behavior)
	PathNotFoundRetries int `json:"path_not_found_retries"`

	// PathNotFoundDelaySeconds is the fixed delay between retry attempts when path not found
	PathNotFoundDelaySeconds int `json:"path_not_found_delay_seconds"`

	// Anchor configuration
	Anchor AnchorConfig `json:"anchor"`
}

// DefaultConfig returns the default processor configuration
func DefaultConfig() Config {
	return Config{
		MaxRetries:               5,
		CleanupDays:              7,
		PathNotFoundRetries:      0,
		PathNotFoundDelaySeconds: 5,
		Anchor:                   DefaultAnchorConfig(),
	}
}

const (
	scanDedupeBuffer = 5 * time.Second
	batchInterval    = 1 * time.Second
	// maxConcurrentScansInternal is a hard cap to prevent runaway scan processing.
	maxConcurrentScansInternal = 50
)

// ScanRequest represents a request to scan a path
type ScanRequest struct {
	Path      string
	TriggerID *int64
	Priority  int
	EventType string
	FilePaths []string // Original file paths that triggered this scan (for tracking/context)
}

// fileStabilityCheck tracks file size for stability checking
type fileStabilityCheck struct {
	size      int64
	checkedAt time.Time
}

// Processor handles scan processing
type Processor struct {
	db            *database.Manager
	config        Config
	anchorChecker *AnchorChecker
	targetsMgr    *targets.Manager
	sseBroker     *sse.Broker
	plexTracker   *scantracker.PlexTracker

	// Channel for receiving new scan requests
	requests chan ScanRequest

	// Active scans tracking
	activeScans   map[int64]bool
	activeScansMu sync.RWMutex

	// pendingHint avoids polling the database when no scans are pending.
	pendingHint atomic.Bool

	// File stability tracking for files (not directories)
	fileStability   map[string]fileStabilityCheck
	fileStabilityMu sync.Mutex

	// Shutdown handling
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Signals when a scan finishes to trigger a new batch pass.
	scanSlotFreed chan struct{}
}

// New creates a new scan processor
func New(db *database.Manager, config Config, targetsMgr *targets.Manager) *Processor {
	ctx, cancel := context.WithCancel(context.Background())
	if targetsMgr == nil {
		targetsMgr = targets.NewManager(db)
	}

	return &Processor{
		db:            db,
		config:        config,
		anchorChecker: NewAnchorChecker(config.Anchor),
		targetsMgr:    targetsMgr,
		requests:      make(chan ScanRequest, 100),
		activeScans:   make(map[int64]bool),
		fileStability: make(map[string]fileStabilityCheck),
		ctx:           ctx,
		cancel:        cancel,
		scanSlotFreed: make(chan struct{}, 1),
	}
}

// SetSSEBroker sets the SSE broker for broadcasting events
func (p *Processor) SetSSEBroker(broker *sse.Broker) {
	p.sseBroker = broker
}

// SetPlexScanTracker sets the Plex scan tracker used for upload gating.
func (p *Processor) SetPlexScanTracker(tracker *scantracker.PlexTracker) {
	p.plexTracker = tracker
}

// Start starts the scan processor
func (p *Processor) Start() {
	// Reset any scans that were in 'scanning' status from a previous interrupted run
	if count, err := p.db.ResetScanningScans(); err != nil {
		log.Error().Err(err).Msg("Failed to reset interrupted scans")
	} else if count > 0 {
		log.Info().Int64("count", count).Msg("Reset interrupted scans back to pending")
	}
	p.refreshPendingHint()

	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Scan request processor panicked")
			}
		}()
		p.requestProcessor()
	})
	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Scan batch processor panicked")
			}
		}()
		p.batchProcessor()
	})
	log.Info().Msg("Scan processor started")
}

// Stop stops the scan processor
func (p *Processor) Stop() {
	p.cancel()
	close(p.requests)
	p.wg.Wait()
	log.Info().Msg("Scan processor stopped")
}

// QueueScan adds a new scan request to the queue
func (p *Processor) QueueScan(req ScanRequest) {
	select {
	case p.requests <- req:
		log.Debug().Str("path", req.Path).Msg("Scan request queued")
	default:
		log.Warn().Str("path", req.Path).Msg("Scan request queue full, dropping request")
	}
}

// QueueScans adds multiple scan requests to the queue
func (p *Processor) QueueScans(reqs []ScanRequest) {
	for _, req := range reqs {
		p.QueueScan(req)
	}
}

func (p *Processor) markPending() {
	p.pendingHint.Store(true)
}

func (p *Processor) refreshPendingHint() {
	count, err := p.db.CountPendingScans()
	if err != nil {
		log.Error().Err(err).Msg("Failed to count pending scans")
		p.pendingHint.Store(true)
		return
	}
	p.pendingHint.Store(count > 0)
}

// requestProcessor handles incoming scan requests
func (p *Processor) requestProcessor() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case req, ok := <-p.requests:
			if !ok {
				return
			}
			p.handleRequest(req)
		}
	}
}

// handleRequest processes a single scan request
func (p *Processor) handleRequest(req ScanRequest) {
	// Normalize the path (path rewriting is now done per-trigger in the API handler)
	path := filepath.Clean(req.Path)

	// Check for duplicate pending scans
	existing, err := p.db.FindDuplicatePendingScan(path, req.TriggerID)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("Failed to check for duplicate scan")
		return
	}

	if existing != nil {
		p.markPending()
		// Merge file paths if new request has any
		if len(req.FilePaths) > 0 {
			if err := p.db.AppendScanFilePaths(existing.ID, req.FilePaths); err != nil {
				log.Error().Err(err).Str("path", path).Msg("Failed to append file paths to existing scan")
			} else {
				log.Debug().Str("path", path).Int("new_files", len(req.FilePaths)).Msg("Appended file paths to existing scan")
			}
		}

		// Update priority if new request has higher priority
		if req.Priority > existing.Priority {
			if err := p.db.UpdateScanPriority(existing.ID, req.Priority); err != nil {
				log.Error().Err(err).Str("path", path).Msg("Failed to update scan priority")
				return
			}
			log.Debug().Str("path", path).Int("old_priority", existing.Priority).Int("new_priority", req.Priority).Msg("Updated scan priority")
		} else {
			log.Debug().Str("path", path).Msg("Duplicate scan request, ignoring")
		}
		return
	}

	// Create new scan record
	scan := &database.Scan{
		Path:      path,
		TriggerID: req.TriggerID,
		Priority:  req.Priority,
		Status:    database.ScanStatusPending,
		EventType: req.EventType,
		FilePaths: req.FilePaths,
	}

	if err := p.db.CreateScan(scan); err != nil {
		log.Error().Err(err).Str("path", path).Msg("Failed to create scan")
		return
	}
	p.markPending()

	log.Info().Str("path", path).Int64("scan_id", scan.ID).Msg("Scan created")

	if p.sseBroker != nil {
		p.sseBroker.Broadcast(sse.Event{
			Type: sse.EventScanQueued,
			Data: map[string]any{"scan_id": scan.ID, "path": scan.Path},
		})
	}
}

// batchProcessor periodically processes ready scans
func (p *Processor) batchProcessor() {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	// Cleanup old file stability entries and scan history periodically
	cleanupTicker := time.NewTicker(5 * time.Minute)
	defer cleanupTicker.Stop()

	// Run scan history cleanup on startup
	p.cleanupOldScans()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.processBatch()
		case <-p.scanSlotFreed:
			p.processBatch()
		case <-cleanupTicker.C:
			p.cleanupFileStability(1 * time.Hour)
			p.cleanupOldScans()
		}
	}
}

// cleanupOldScans removes completed/failed scans older than the configured retention period
func (p *Processor) cleanupOldScans() {
	if p.config.CleanupDays <= 0 {
		// Cleanup disabled, keep history forever
		return
	}

	age := time.Duration(p.config.CleanupDays) * 24 * time.Hour
	deleted, err := p.db.DeleteOldScans(age)
	if err != nil {
		log.Error().Err(err).Msg("Failed to cleanup old scans")
		return
	}

	if deleted > 0 {
		log.Info().
			Int64("deleted", deleted).
			Int("days", p.config.CleanupDays).
			Msg("Cleaned up old scan history")
	}
}

// processBatch processes a batch of ready scans
func (p *Processor) processBatch() {
	if !p.pendingHint.Load() {
		return
	}

	// Get all pending scans (we'll filter by per-trigger age)
	scans, err := p.db.ListPendingScans()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list pending scans")
		return
	}

	if len(scans) == 0 {
		p.refreshPendingHint()
		return
	}

	type scanWork struct {
		scan          *database.Scan
		trigger       *database.Trigger
		triggerName   string
		uploadCapable bool
		ready         bool
		triggerConfig *database.TriggerConfig
	}
	type targetKey struct {
		targetID   int64
		mappedPath string
	}
	type targetCandidate struct {
		work *scanWork
	}

	uploadsEnabled := true
	if val, _ := p.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}

	enabledTargets, err := p.db.ListEnabledTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list enabled targets for scan processing")
		return
	}

	triggerCache := make(map[int64]*database.Trigger)
	getTrigger := func(triggerID int64) *database.Trigger {
		if trigger, ok := triggerCache[triggerID]; ok {
			return trigger
		}
		trigger, err := p.db.GetTrigger(triggerID)
		if err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to load trigger for scan")
			triggerCache[triggerID] = nil
			return nil
		}
		triggerCache[triggerID] = trigger
		return trigger
	}

	scanWorks := make(map[int64]*scanWork, len(scans))
	candidateCounts := make(map[int64]int, len(scans))
	candidatesByKey := make(map[targetKey][]targetCandidate)

	for _, scan := range scans {
		minAge := time.Duration(0)
		var trigger *database.Trigger
		triggerName := ""
		if scan.TriggerID != nil {
			trigger = getTrigger(*scan.TriggerID)
			if trigger != nil && !trigger.Config.ScanEnabledValue() {
				if err := p.db.UpdateScanStatus(scan.ID, database.ScanStatusCompleted); err != nil {
					log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to mark scan as completed")
				} else {
					log.Debug().Int64("scan_id", scan.ID).Msg("Skipped scan (trigger scanning disabled)")
				}
				continue
			}
			if trigger != nil {
				triggerName = trigger.Name
				if trigger.Config.MinimumAgeSeconds > 0 {
					minAge = time.Duration(trigger.Config.MinimumAgeSeconds) * time.Second
				}
			}
		}

		if minAge < scanDedupeBuffer {
			minAge = scanDedupeBuffer
		}

		ready := time.Since(scan.CreatedAt) >= minAge
		uploadCapable := false
		if trigger != nil &&
			uploadsEnabled &&
			trigger.Config.UploadEnabledValue() &&
			(trigger.Type == database.TriggerTypeInotify || trigger.Type == database.TriggerTypePolling) {
			uploadCapable = true
		}

		work := &scanWork{
			scan:          scan,
			trigger:       trigger,
			triggerName:   triggerName,
			uploadCapable: uploadCapable,
			ready:         ready,
		}
		if trigger != nil {
			work.triggerConfig = &trigger.Config
		}
		scanWorks[scan.ID] = work

		for _, target := range enabledTargets {
			if triggerName != "" && target.Config.ShouldExcludeTrigger(triggerName) {
				continue
			}
			if target.Config.ShouldExcludePath(scan.Path) {
				continue
			}

			mappedPath := targets.ApplyPathMappings(scan.Path, target.Config.PathMappings)
			mappedPath = filepath.Clean(mappedPath)
			if mappedPath == "" || mappedPath == "." {
				continue
			}

			key := targetKey{
				targetID:   target.ID,
				mappedPath: mappedPath,
			}
			candidatesByKey[key] = append(candidatesByKey[key], targetCandidate{work: work})
			candidateCounts[scan.ID]++
		}
	}

	assignments := make(map[int64]map[int64]struct{}, len(scanWorks))
	assignTarget := func(scanID int64, targetID int64) {
		assigned := assignments[scanID]
		if assigned == nil {
			assigned = make(map[int64]struct{})
			assignments[scanID] = assigned
		}
		assigned[targetID] = struct{}{}
	}

	for key, candidates := range candidatesByKey {
		sort.Slice(candidates, func(i, j int) bool {
			left := candidates[i].work.scan
			right := candidates[j].work.scan
			if left.CreatedAt.Equal(right.CreatedAt) {
				return left.ID < right.ID
			}
			return left.CreatedAt.Before(right.CreatedAt)
		})

		for i := 0; i < len(candidates); {
			windowStart := candidates[i].work.scan.CreatedAt
			j := i + 1
			for j < len(candidates) && candidates[j].work.scan.CreatedAt.Sub(windowStart) <= scanDedupeBuffer {
				j++
			}

			winner := candidates[i]
			for k := i; k < j; k++ {
				if candidates[k].work.uploadCapable {
					winner = candidates[k]
					break
				}
			}

			assignTarget(winner.work.scan.ID, key.targetID)
			i = j
		}
	}

	scanTargets := make(map[int64][]int64, len(assignments))
	for scanID, targets := range assignments {
		targetIDs := make([]int64, 0, len(targets))
		for targetID := range targets {
			targetIDs = append(targetIDs, targetID)
		}
		slices.Sort(targetIDs)
		scanTargets[scanID] = targetIDs
	}

	var readyScans []*database.Scan
	readyWork := make(map[int64]*scanWork, len(scanWorks))
	for scanID, work := range scanWorks {
		targetIDs := scanTargets[scanID]
		if len(targetIDs) == 0 {
			if candidateCounts[scanID] > 0 {
				log.Debug().
					Int64("scan_id", scanID).
					Str("path", work.scan.Path).
					Msg("Skipping scan (deduped within buffer)")
				if err := p.db.DeleteScan(scanID); err != nil {
					log.Error().Err(err).Int64("scan_id", scanID).Msg("Failed to delete deduped scan")
				} else if p.sseBroker != nil {
					p.sseBroker.Broadcast(sse.Event{
						Type: sse.EventScanCompleted,
						Data: map[string]any{
							"scan_id": scanID,
							"path":    work.scan.Path,
							"reason":  "deduped_within_buffer",
						},
					})
				}
				continue
			}

			log.Debug().
				Int64("scan_id", scanID).
				Str("path", work.scan.Path).
				Msg("Skipping scan (no eligible targets)")
			if err := p.db.UpdateScanStatus(scanID, database.ScanStatusCompleted); err != nil {
				log.Error().Err(err).Int64("scan_id", scanID).Msg("Failed to mark scan as completed")
			} else if p.sseBroker != nil {
				p.sseBroker.Broadcast(sse.Event{
					Type: sse.EventScanCompleted,
					Data: map[string]any{
						"scan_id": scanID,
						"path":    work.scan.Path,
						"reason":  "no_eligible_targets",
					},
				})
			}
			continue
		}
		if !work.ready {
			continue
		}
		readyScans = append(readyScans, work.scan)
		readyWork[scanID] = work
	}

	if len(readyScans) == 0 {
		return
	}

	log.Debug().Int("count", len(readyScans)).Msg("Processing batch of scans")

	activeCount := p.ActiveScanCount()
	availableSlots := maxConcurrentScansInternal - activeCount
	if availableSlots <= 0 {
		log.Debug().
			Int("active", activeCount).
			Int("max", maxConcurrentScansInternal).
			Msg("Scan concurrency limit reached, skipping batch")
		return
	}

	// Group scans by parent directory for batching
	batches := p.groupByParent(readyScans)

	for parentPath, batchScans := range batches {
		if availableSlots <= 0 {
			return
		}
		// Check anchor/readiness for the parent path
		ready, reason := p.anchorChecker.IsReady(parentPath)
		if !ready {
			log.Debug().Str("path", parentPath).Str("reason", reason).Msg("Path not ready for scanning")
			continue
		}

		// Process each scan in the batch
		for _, scan := range batchScans {
			if availableSlots <= 0 {
				return
			}
			work := readyWork[scan.ID]
			if work == nil {
				continue
			}

			targetIDs := scanTargets[scan.ID]
			if len(targetIDs) == 0 {
				continue
			}

			// Skip path stability check for delete events since the file won't exist
			if isDeleteEvent(scan.EventType) {
				log.Debug().Int64("scan_id", scan.ID).Str("path", scan.Path).Str("event_type", scan.EventType).
					Msg("Skipping path check for delete event")
				p.processScan(scan, work.triggerName, targetIDs)
				availableSlots--
				continue
			}

			// Get trigger config for filesystem type and stability settings
			triggerConfig := work.triggerConfig

			// Check file stability for files (directories use simple age check)
			// Skip stability check for remote filesystems
			stable, stabilityReason := p.checkPathStability(scan.Path, triggerConfig)
			if !stable {
				// If path doesn't exist, check if we should retry
				if stabilityReason == "path does not exist" {
					if p.config.PathNotFoundRetries > 0 && scan.RetryCount < p.config.PathNotFoundRetries {
						// Schedule retry by updating retry_count and next_retry_at, keep status pending
						nextRetryAt := time.Now().Add(time.Duration(p.config.PathNotFoundDelaySeconds) * time.Second)
						if err := p.db.SetScanPathNotFoundRetry(scan.ID, scan.RetryCount+1, nextRetryAt); err != nil {
							log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to schedule path retry")
						} else {
							log.Debug().Int64("scan_id", scan.ID).Str("path", scan.Path).
								Int("retry", scan.RetryCount+1).Int("max", p.config.PathNotFoundRetries).
								Msg("Path not found, will retry")
						}
						continue
					}
					// Retries exhausted or disabled - fail immediately
					errMsg := "path does not exist"
					if err := p.db.UpdateScanError(scan.ID, errMsg); err != nil {
						log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to mark scan as failed")
					} else {
						if p.sseBroker != nil {
							p.sseBroker.Broadcast(sse.Event{
								Type: sse.EventScanFailed,
								Data: map[string]any{"scan_id": scan.ID, "path": scan.Path, "error": errMsg},
							})
						}
						log.Warn().Int64("scan_id", scan.ID).Str("path", scan.Path).Msg("Scan failed: path does not exist")
					}
					continue
				}
				log.Debug().Str("path", scan.Path).Str("reason", stabilityReason).Msg("Path not stable for scanning")
				continue
			}

			p.processScan(scan, work.triggerName, targetIDs)
			availableSlots--
		}
	}
}

// isDeleteEvent checks if the event type is a file/media delete event
// These events should skip path existence checks since the file has been deleted
func isDeleteEvent(eventType string) bool {
	switch eventType {
	case "MovieFileDelete", "EpisodeFileDelete", "TrackFileDelete",
		"MovieDelete", "SeriesDelete", "ArtistDelete", "AlbumDelete":
		return true
	}
	return false
}

// checkPathStability checks if a path is stable enough to process
// For files on local filesystem: checks if size has stopped changing
// For files on remote filesystem or directories: returns true (relies on age gating in batch processing)
func (p *Processor) checkPathStability(path string, triggerConfig *database.TriggerConfig) (bool, string) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, "path does not exist"
		}
		return false, "failed to stat path"
	}

	// Directories rely on the age gating done in batch processing.
	if info.IsDir() {
		return true, ""
	}

	// Check filesystem type from trigger config
	// Remote filesystems use dumb delay (no stability checking), just age check
	if triggerConfig == nil || triggerConfig.FilesystemType == "" || triggerConfig.FilesystemType == database.FilesystemTypeRemote {
		return true, ""
	}

	// For local filesystem, check file size stability
	currentSize := info.Size()

	// Get stability check interval from trigger config (default 10s)
	stabilityInterval := 10 * time.Second
	if triggerConfig.StabilityCheckSeconds > 0 {
		stabilityInterval = time.Duration(triggerConfig.StabilityCheckSeconds) * time.Second
	}

	p.fileStabilityMu.Lock()
	defer p.fileStabilityMu.Unlock()

	lastCheck, exists := p.fileStability[path]
	if !exists {
		// First check - record size and wait
		p.fileStability[path] = fileStabilityCheck{
			size:      currentSize,
			checkedAt: time.Now(),
		}
		return false, "waiting for file stability check"
	}

	// Size changed - reset timer and wait
	if lastCheck.size != currentSize {
		p.fileStability[path] = fileStabilityCheck{
			size:      currentSize,
			checkedAt: time.Now(),
		}
		return false, "file size changed, resetting stability timer"
	}

	// Size unchanged - check if stability interval has passed since last check
	if time.Since(lastCheck.checkedAt) < stabilityInterval {
		return false, "waiting for file stability (size unchanged, checking again)"
	}

	// File is stable - clean up and allow processing
	delete(p.fileStability, path)
	return true, ""
}

// cleanupFileStability removes old file stability entries to prevent memory leaks
func (p *Processor) cleanupFileStability(maxAge time.Duration) {
	p.fileStabilityMu.Lock()
	defer p.fileStabilityMu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for path, check := range p.fileStability {
		if check.checkedAt.Before(cutoff) {
			delete(p.fileStability, path)
		}
	}
}

// groupByParent groups scans by their parent directory
func (p *Processor) groupByParent(scans []*database.Scan) map[string][]*database.Scan {
	batches := make(map[string][]*database.Scan)

	for _, scan := range scans {
		parent := filepath.Dir(scan.Path)
		batches[parent] = append(batches[parent], scan)
	}

	return batches
}

// processScan processes a single scan for the assigned targets.
func (p *Processor) processScan(scan *database.Scan, triggerName string, targetIDs []int64) {
	p.activeScansMu.Lock()
	p.activeScans[scan.ID] = true
	p.activeScansMu.Unlock()

	// Update status to scanning
	if err := p.db.UpdateScanStatus(scan.ID, database.ScanStatusScanning); err != nil {
		log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to update scan status")
		p.activeScansMu.Lock()
		delete(p.activeScans, scan.ID)
		p.activeScansMu.Unlock()
		return
	}

	// Broadcast scan started event
	if p.sseBroker != nil {
		p.sseBroker.Broadcast(sse.Event{
			Type: sse.EventScanStarted,
			Data: map[string]any{"scan_id": scan.ID, "path": scan.Path},
		})
	}

	log.Info().Int64("scan_id", scan.ID).Str("path", scan.Path).Msg("Starting scan")

	go func() {
		defer func() {
			p.activeScansMu.Lock()
			delete(p.activeScans, scan.ID)
			p.activeScansMu.Unlock()
			select {
			case p.scanSlotFreed <- struct{}{}:
			default:
			}
		}()

		// Check for cancellation before starting
		select {
		case <-p.ctx.Done():
			log.Debug().Int64("scan_id", scan.ID).Msg("Scan cancelled before processing")
			return
		default:
		}

		// Check if scanning is enabled (default to true)
		scanningEnabled := true
		if val, _ := p.db.GetSetting("scanning.enabled"); val == "false" {
			scanningEnabled = false
		}

		// Check for cancellation before target scanning
		select {
		case <-p.ctx.Done():
			log.Debug().Int64("scan_id", scan.ID).Msg("Scan cancelled before target scanning")
			return
		default:
		}

		if scanningEnabled {
			ctx, cancel := context.WithTimeout(p.ctx, config.GetTimeouts().ScanOperation)
			defer cancel()

			results := make([]targets.ScanResult, 0, len(targetIDs))
			completionInfos := make([]targets.ScanCompletionInfo, 0)
			for _, targetID := range targetIDs {
				result, completionInfo := p.targetsMgr.ScanTarget(ctx, targetID, scan.Path, triggerName)
				results = append(results, result)
				if completionInfo != nil {
					completionInfos = append(completionInfos, *completionInfo)
				}
			}

			// Check for cancellation after target scanning
			select {
			case <-p.ctx.Done():
				log.Debug().Int64("scan_id", scan.ID).Msg("Scan cancelled after target scanning")
				return
			default:
			}

			// Log results and collect errors
			successCount := 0
			var lastError string
			for _, result := range results {
				if result.Success {
					successCount++
					log.Debug().
						Int64("scan_id", scan.ID).
						Str("target", result.Message).
						Msg("Target scan triggered")
				} else {
					lastError = result.Error
					log.Warn().
						Int64("scan_id", scan.ID).
						Str("target", result.Message).
						Str("error", result.Error).
						Msg("Target scan failed")
				}
			}

			log.Info().
				Int64("scan_id", scan.ID).
				Str("path", scan.Path).
				Int("success_count", successCount).
				Int("total_targets", len(results)).
				Msg("Scan triggered on targets")

			// Check if all targets failed - schedule retry with exponential backoff
			if successCount == 0 && len(results) > 0 {
				scheduled, err := p.db.ScheduleScanRetry(scan.ID, lastError, p.config.MaxRetries)
				if err != nil {
					log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to schedule scan retry")
					return
				}
				if scheduled {
					log.Info().
						Int64("scan_id", scan.ID).
						Str("path", scan.Path).
						Int("retry_count", scan.RetryCount+1).
						Msg("Scan scheduled for retry")
				} else {
					// Broadcast scan failed event
					if p.sseBroker != nil {
						p.sseBroker.Broadcast(sse.Event{
							Type: sse.EventScanFailed,
							Data: map[string]any{"scan_id": scan.ID, "path": scan.Path, "error": lastError},
						})
					}
					log.Error().
						Int64("scan_id", scan.ID).
						Str("path", scan.Path).
						Int("max_retries", p.config.MaxRetries).
						Msg("Scan failed permanently after max retries")
				}
				return
			}

			// Track Plex scan completion for upload-side gating.
			if p.plexTracker != nil && len(completionInfos) > 0 {
				p.plexTracker.TrackScan(scan, completionInfos)
			}

		} else {
			log.Debug().
				Int64("scan_id", scan.ID).
				Str("path", scan.Path).
				Msg("Scanning disabled, skipping target notifications")
		}

		// Check for cancellation before marking complete
		select {
		case <-p.ctx.Done():
			log.Debug().Int64("scan_id", scan.ID).Msg("Scan cancelled before completion")
			return
		default:
		}

		// Mark scan as completed
		if err := p.db.UpdateScanStatus(scan.ID, database.ScanStatusCompleted); err != nil {
			log.Error().Err(err).Int64("scan_id", scan.ID).Msg("Failed to mark scan as completed")
			return
		}

		// Check for cancellation before broadcasting
		select {
		case <-p.ctx.Done():
			log.Debug().Int64("scan_id", scan.ID).Msg("Scan cancelled after completion, skipping broadcast")
			return
		default:
		}

		// Broadcast scan completed event
		if p.sseBroker != nil {
			p.sseBroker.Broadcast(sse.Event{
				Type: sse.EventScanCompleted,
				Data: map[string]any{"scan_id": scan.ID, "path": scan.Path},
			})
		}

		log.Info().Int64("scan_id", scan.ID).Str("path", scan.Path).Msg("Scan completed")

	}()
}

// Stats returns current processor statistics
func (p *Processor) Stats() ProcessorStats {
	p.activeScansMu.RLock()
	activeCount := len(p.activeScans)
	p.activeScansMu.RUnlock()

	pendingCount, _ := p.db.CountPendingScans()

	return ProcessorStats{
		ActiveScans:  activeCount,
		PendingScans: pendingCount,
		QueueSize:    len(p.requests),
	}
}

// ProcessorStats holds processor statistics
type ProcessorStats struct {
	ActiveScans  int `json:"active_scans"`
	PendingScans int `json:"pending_scans"`
	QueueSize    int `json:"queue_size"`
}

// IsRunning returns whether the processor is running
func (p *Processor) IsRunning() bool {
	select {
	case <-p.ctx.Done():
		return false
	default:
		return true
	}
}

// ActiveScanCount returns the number of currently active scans
func (p *Processor) ActiveScanCount() int {
	p.activeScansMu.RLock()
	defer p.activeScansMu.RUnlock()
	return len(p.activeScans)
}

// UpdateConfig updates the processor configuration
func (p *Processor) UpdateConfig(config Config) {
	p.config = config
	p.anchorChecker = NewAnchorChecker(config.Anchor)
	log.Info().Msg("Processor configuration updated")
}

// Config returns the current processor configuration
func (p *Processor) Config() Config {
	return p.config
}

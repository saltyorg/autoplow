package polling

import (
	"context"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
	"github.com/saltyorg/autoplow/internal/uploader"
)

// Poller watches filesystem paths for changes using interval-based polling
type Poller struct {
	db            *database.DB
	processor     *processor.Processor
	uploadManager *uploader.Manager
	triggers      map[int64]*triggerPoll
	mu            sync.RWMutex

	// Running state
	running bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// triggerPoll tracks polling state for a trigger
type triggerPoll struct {
	trigger    *database.Trigger
	seenFiles  map[string]struct{}
	seenMu     sync.RWMutex
	scanning   atomic.Bool
	firstScan  bool
	cancelFunc context.CancelFunc
}

// New creates a new filesystem poller
func New(db *database.DB, proc *processor.Processor) *Poller {
	ctx, cancel := context.WithCancel(context.Background())

	return &Poller{
		db:        db,
		processor: proc,
		triggers:  make(map[int64]*triggerPoll),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start starts the poller and loads all polling triggers.
// Returns true if the poller was started (triggers exist), false otherwise.
func (p *Poller) Start() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return true, nil
	}

	// Load polling triggers from database
	if err := p.loadTriggersLocked(); err != nil {
		return false, err
	}

	// Only start if we have triggers
	if len(p.triggers) == 0 {
		return false, nil
	}

	p.running = true
	log.Info().Msg("Polling watcher started")
	return true, nil
}

// Stop stops the poller
func (p *Poller) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false

	// Cancel all trigger poll loops
	for _, tp := range p.triggers {
		if tp.cancelFunc != nil {
			tp.cancelFunc()
		}
	}
	p.mu.Unlock()

	p.cancel()
	p.wg.Wait()

	log.Info().Msg("Polling watcher stopped")
}

// IsRunning returns whether the poller is currently running
func (p *Poller) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

// SetUploadManager sets the upload manager for queuing uploads from polling events
func (p *Poller) SetUploadManager(mgr *uploader.Manager) {
	p.uploadManager = mgr
}

// loadTriggersLocked loads all enabled polling triggers from the database.
// Caller must hold p.mu.
func (p *Poller) loadTriggersLocked() error {
	triggers, err := p.db.ListTriggersByType(database.TriggerTypePolling)
	if err != nil {
		return err
	}

	for _, trigger := range triggers {
		if trigger.Enabled {
			if err := p.addTriggerLocked(trigger); err != nil {
				log.Error().Err(err).Str("trigger", trigger.Name).Msg("Failed to add polling trigger")
			}
		}
	}

	return nil
}

// addTrigger adds a trigger and starts its polling loop
func (p *Poller) addTrigger(trigger *database.Trigger) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addTriggerLocked(trigger)
}

// addTriggerLocked adds a trigger and starts its polling loop.
// Caller must hold p.mu.
func (p *Poller) addTriggerLocked(trigger *database.Trigger) error {
	// Remove existing poll loop for this trigger if any
	if existing, ok := p.triggers[trigger.ID]; ok {
		if existing.cancelFunc != nil {
			existing.cancelFunc()
		}
	}

	ctx, cancel := context.WithCancel(p.ctx)
	tp := &triggerPoll{
		trigger:    trigger,
		seenFiles:  make(map[string]struct{}),
		firstScan:  true,
		cancelFunc: cancel,
	}

	p.triggers[trigger.ID] = tp

	// Start the polling loop
	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Str("trigger", trigger.Name).Msg("Polling loop panicked")
			}
		}()
		p.pollLoop(ctx, tp)
	})

	log.Info().Str("trigger", trigger.Name).Int("paths", len(trigger.Config.WatchPaths)).Msg("Polling trigger loaded")

	return nil
}

// RemoveTrigger removes a trigger and stops its polling loop
func (p *Poller) RemoveTrigger(triggerID int64) {
	p.mu.Lock()
	removed := false
	shouldStop := false

	if tp, ok := p.triggers[triggerID]; ok {
		if tp.cancelFunc != nil {
			tp.cancelFunc()
		}
		delete(p.triggers, triggerID)
		removed = true
		// Check if we should stop the poller (no more triggers)
		if p.running && len(p.triggers) == 0 {
			shouldStop = true
		}
	}
	p.mu.Unlock()

	if removed {
		log.Info().Int64("trigger_id", triggerID).Msg("Polling trigger removed")
	}

	// Stop the poller if no triggers remain (must be done outside the lock)
	if shouldStop {
		p.Stop()
	}
}

// ReloadTrigger reloads a trigger's configuration.
// If the poller is not running and an enabled trigger is added, it will start the poller.
func (p *Poller) ReloadTrigger(triggerID int64) error {
	trigger, err := p.db.GetTrigger(triggerID)
	if err != nil {
		return err
	}

	if trigger == nil || trigger.Type != database.TriggerTypePolling {
		p.RemoveTrigger(triggerID)
		return nil
	}

	if !trigger.Enabled {
		p.RemoveTrigger(triggerID)
		return nil
	}

	// If poller is not running, start it (which will load all triggers including this one)
	if !p.IsRunning() {
		_, err := p.Start()
		return err
	}

	return p.addTrigger(trigger)
}

// pollLoop is the main polling loop for a trigger
func (p *Poller) pollLoop(ctx context.Context, tp *triggerPoll) {
	interval := time.Duration(tp.trigger.Config.PollIntervalSeconds) * time.Second
	if interval < 10*time.Second {
		interval = 60 * time.Second // Default to 60 seconds
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Do an initial scan immediately
	p.doScan(tp)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Skip this cycle if previous scan is still running
			if !tp.scanning.CompareAndSwap(false, true) {
				log.Debug().
					Int64("trigger_id", tp.trigger.ID).
					Str("trigger", tp.trigger.Name).
					Msg("Skipping poll cycle - previous scan still running")
				continue
			}
			p.doScan(tp)
			tp.scanning.Store(false)
		}
	}
}

// doScan performs a single scan of all watch paths for a trigger
func (p *Poller) doScan(tp *triggerPoll) {
	// Check settings
	scanningEnabled := true
	if val, _ := p.db.GetSetting("scanning.enabled"); val == "false" {
		scanningEnabled = false
	}
	uploadsEnabled := true
	if val, _ := p.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}

	if !uploadsEnabled {
		// Still track files even if uploads disabled, so we don't queue them all when re-enabled
		p.trackFilesOnly(tp)
		return
	}

	isFirstScan := tp.firstScan
	queueExisting := tp.trigger.Config.QueueExistingOnStart

	var newFiles []string

	for _, watchPath := range tp.trigger.Config.WatchPaths {
		absPath, err := filepath.Abs(watchPath)
		if err != nil {
			log.Error().Err(err).Str("path", watchPath).Msg("Failed to get absolute path")
			continue
		}

		err = filepath.WalkDir(absPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // Skip errors, continue walking
			}
			if d.IsDir() {
				return nil
			}

			tp.seenMu.RLock()
			_, seen := tp.seenFiles[path]
			tp.seenMu.RUnlock()

			if !seen {
				tp.seenMu.Lock()
				tp.seenFiles[path] = struct{}{}
				tp.seenMu.Unlock()

				// Queue for upload if not first scan, or if queueExisting is enabled
				if !isFirstScan || queueExisting {
					newFiles = append(newFiles, path)
				}
			}

			return nil
		})

		if err != nil {
			log.Error().Err(err).Str("path", absPath).Msg("Error walking directory")
		}
	}

	tp.firstScan = false

	// Queue new files
	if len(newFiles) > 0 {
		triggerID := tp.trigger.ID

		if scanningEnabled {
			// Group files by parent directory for efficient scanning
			filesByDir := make(map[string][]string)
			for _, path := range newFiles {
				dir := filepath.Dir(path)
				filesByDir[dir] = append(filesByDir[dir], path)
			}

			// Queue one scan per directory with all file paths
			for dir, files := range filesByDir {
				log.Info().
					Str("path", dir).
					Int("files", len(files)).
					Int64("trigger_id", triggerID).
					Msg("Polling trigger queuing scan")

				p.processor.QueueScan(processor.ScanRequest{
					Path:      dir,
					TriggerID: &triggerID,
					Priority:  tp.trigger.Priority,
					FilePaths: files,
				})
			}

			log.Info().
				Int("files", len(newFiles)).
				Int64("trigger_id", triggerID).
				Str("trigger", tp.trigger.Name).
				Msg("Polling scan completed - scans queued")
		} else if p.uploadManager != nil {
			// Scanning disabled - queue uploads directly
			for _, path := range newFiles {
				log.Info().
					Str("path", path).
					Int64("trigger_id", triggerID).
					Msg("Polling trigger queuing upload (scanning disabled)")

				p.uploadManager.QueueUpload(uploader.UploadRequest{
					LocalPath: path,
					ScanID:    nil,
					Priority:  tp.trigger.Priority,
					TriggerID: &triggerID,
				})
			}

			log.Info().
				Int("files", len(newFiles)).
				Int64("trigger_id", triggerID).
				Str("trigger", tp.trigger.Name).
				Msg("Polling scan completed - uploads queued")
		}
	}
}

// trackFilesOnly updates the seen files map without queueing uploads
func (p *Poller) trackFilesOnly(tp *triggerPoll) {
	for _, watchPath := range tp.trigger.Config.WatchPaths {
		absPath, err := filepath.Abs(watchPath)
		if err != nil {
			continue
		}

		if err := filepath.WalkDir(absPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}

			tp.seenMu.Lock()
			tp.seenFiles[path] = struct{}{}
			tp.seenMu.Unlock()

			return nil
		}); err != nil {
			log.Error().Err(err).Str("path", absPath).Msg("Failed to walk directory for initial scan")
		}
	}
	tp.firstScan = false
}

// Stats returns poller statistics
func (p *Poller) Stats() PollerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pathCount := 0
	seenCount := 0
	for _, tp := range p.triggers {
		pathCount += len(tp.trigger.Config.WatchPaths)
		tp.seenMu.RLock()
		seenCount += len(tp.seenFiles)
		tp.seenMu.RUnlock()
	}

	return PollerStats{
		TriggerCount: len(p.triggers),
		PathCount:    pathCount,
		SeenFiles:    seenCount,
	}
}

// PollerStats holds poller statistics
type PollerStats struct {
	TriggerCount int `json:"trigger_count"`
	PathCount    int `json:"path_count"`
	SeenFiles    int `json:"seen_files"`
}

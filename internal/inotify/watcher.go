package inotify

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
	"github.com/saltyorg/autoplow/internal/uploader"
)

// Watcher watches filesystem paths for changes and triggers scans
type Watcher struct {
	db            *database.DB
	processor     *processor.Processor
	uploadManager *uploader.Manager
	watcher       *fsnotify.Watcher
	triggers      map[int64]*triggerWatch
	mu            sync.RWMutex

	// Debounce tracking
	pending   map[string]*pendingEvent
	pendingMu sync.Mutex

	// Running state
	running bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// triggerWatch tracks watch state for a trigger
type triggerWatch struct {
	trigger *database.Trigger
	paths   []string
}

// pendingEvent tracks a debounced event
type pendingEvent struct {
	path      string
	triggerID int64
	priority  int
	timer     *time.Timer
}

// New creates a new filesystem watcher
func New(db *database.DB, proc *processor.Processor) (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Watcher{
		db:        db,
		processor: proc,
		watcher:   fsWatcher,
		triggers:  make(map[int64]*triggerWatch),
		pending:   make(map[string]*pendingEvent),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Start starts the watcher and loads all inotify triggers.
// Returns true if the watcher was started (triggers exist), false otherwise.
func (w *Watcher) Start() (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return true, nil
	}

	// Load inotify triggers from database
	if err := w.loadTriggersLocked(); err != nil {
		return false, err
	}

	// Only start if we have triggers
	if len(w.triggers) == 0 {
		return false, nil
	}

	// Start event processing
	w.running = true
	w.wg.Add(1)
	go w.eventLoop()

	log.Info().Msg("Inotify watcher started")
	return true, nil
}

// Stop stops the watcher
func (w *Watcher) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	w.mu.Unlock()

	w.cancel()
	w.watcher.Close()
	w.wg.Wait()

	// Cancel any pending events
	w.pendingMu.Lock()
	for _, p := range w.pending {
		p.timer.Stop()
	}
	w.pending = make(map[string]*pendingEvent)
	w.pendingMu.Unlock()

	log.Info().Msg("Inotify watcher stopped")
}

// IsRunning returns whether the watcher is currently running
func (w *Watcher) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.running
}

// SetUploadManager sets the upload manager for queuing uploads from inotify events
func (w *Watcher) SetUploadManager(mgr *uploader.Manager) {
	w.uploadManager = mgr
}

// loadTriggersLocked loads all enabled inotify triggers from the database.
// Caller must hold w.mu.
func (w *Watcher) loadTriggersLocked() error {
	triggers, err := w.db.ListTriggersByType(database.TriggerTypeInotify)
	if err != nil {
		return err
	}

	for _, trigger := range triggers {
		if trigger.Enabled {
			if err := w.addTriggerLocked(trigger); err != nil {
				log.Error().Err(err).Str("trigger", trigger.Name).Msg("Failed to add inotify trigger")
			}
		}
	}

	return nil
}

// addTrigger adds watches for a trigger's paths
func (w *Watcher) addTrigger(trigger *database.Trigger) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.addTriggerLocked(trigger)
}

// addTriggerLocked adds watches for a trigger's paths.
// Caller must hold w.mu.
func (w *Watcher) addTriggerLocked(trigger *database.Trigger) error {
	// Remove existing watches for this trigger if any
	if existing, ok := w.triggers[trigger.ID]; ok {
		for _, path := range existing.paths {
			if err := w.watcher.Remove(path); err != nil {
				log.Debug().Err(err).Str("path", path).Msg("Failed to remove existing watch (may not exist)")
			}
		}
	}

	tw := &triggerWatch{
		trigger: trigger,
		paths:   make([]string, 0, len(trigger.Config.WatchPaths)),
	}

	for _, path := range trigger.Config.WatchPaths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("Failed to get absolute path")
			continue
		}

		// Recursively add watches for this path and all subdirectories
		watched := w.addWatchRecursive(absPath)
		tw.paths = append(tw.paths, watched...)
		log.Debug().Str("path", absPath).Int("directories", len(watched)).Str("trigger", trigger.Name).Msg("Added recursive watch")
	}

	w.triggers[trigger.ID] = tw
	log.Info().Str("trigger", trigger.Name).Int("paths", len(tw.paths)).Msg("Inotify trigger loaded")

	return nil
}

// RemoveTrigger removes watches for a trigger
func (w *Watcher) RemoveTrigger(triggerID int64) {
	w.mu.Lock()
	removed := false
	shouldStop := false

	if tw, ok := w.triggers[triggerID]; ok {
		for _, path := range tw.paths {
			if err := w.watcher.Remove(path); err != nil {
				log.Debug().Err(err).Str("path", path).Msg("Failed to remove watch during trigger removal")
			}
		}
		delete(w.triggers, triggerID)
		removed = true
		// Check if we should stop the watcher (no more triggers)
		if w.running && len(w.triggers) == 0 {
			shouldStop = true
		}
	}
	w.mu.Unlock()

	if removed {
		log.Info().Int64("trigger_id", triggerID).Msg("Inotify trigger removed")
	}

	// Stop the watcher if no triggers remain (must be done outside the lock)
	if shouldStop {
		w.Stop()
	}
}

// ReloadTrigger reloads a trigger's configuration.
// If the watcher is not running and an enabled trigger is added, it will start the watcher.
func (w *Watcher) ReloadTrigger(triggerID int64) error {
	trigger, err := w.db.GetTrigger(triggerID)
	if err != nil {
		return err
	}

	if trigger == nil || trigger.Type != database.TriggerTypeInotify {
		w.RemoveTrigger(triggerID)
		return nil
	}

	if !trigger.Enabled {
		w.RemoveTrigger(triggerID)
		return nil
	}

	// If watcher is not running, start it (which will load all triggers including this one)
	if !w.IsRunning() {
		_, err := w.Start()
		return err
	}

	return w.addTrigger(trigger)
}

// eventLoop processes filesystem events
func (w *Watcher) eventLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return

		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(event)

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			log.Error().Err(err).Msg("Inotify watcher error")
		}
	}
}

// handleEvent processes a single filesystem event
func (w *Watcher) handleEvent(event fsnotify.Event) {
	// Only process create and write events
	if !event.Has(fsnotify.Create) && !event.Has(fsnotify.Write) {
		return
	}

	// If a new directory was created, add watches for it and its subdirectories
	if event.Has(fsnotify.Create) {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			w.mu.Lock()
			// Find which trigger this directory belongs to and add watches
			for _, tw := range w.triggers {
				for _, watchPath := range tw.paths {
					if isUnderPath(event.Name, watchPath) {
						watched := w.addWatchRecursive(event.Name)
						tw.paths = append(tw.paths, watched...)
						log.Debug().Str("path", event.Name).Int("directories", len(watched)).Str("trigger", tw.trigger.Name).Msg("Added watch for new directory")
						break
					}
				}
			}
			w.mu.Unlock()
			return // Don't trigger scans for directory creation
		}
	}

	// Find which trigger this path belongs to
	w.mu.RLock()
	var matchedTrigger *database.Trigger
	for _, tw := range w.triggers {
		for _, watchPath := range tw.paths {
			// Check if event path is under this watch path
			if isUnderPath(event.Name, watchPath) {
				matchedTrigger = tw.trigger
				break
			}
		}
		if matchedTrigger != nil {
			break
		}
	}
	w.mu.RUnlock()

	if matchedTrigger == nil {
		return
	}

	// Get debounce delay
	debounce := time.Duration(matchedTrigger.Config.DebounceSeconds) * time.Second
	if debounce < time.Second {
		debounce = 5 * time.Second // Default 5 seconds
	}

	// Schedule or reset debounced event
	w.scheduleEvent(event.Name, matchedTrigger.ID, matchedTrigger.Priority, debounce)
}

// scheduleEvent schedules a scan with debouncing
func (w *Watcher) scheduleEvent(path string, triggerID int64, priority int, debounce time.Duration) {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	// Check if there's already a pending event for this path
	if existing, ok := w.pending[path]; ok {
		existing.timer.Reset(debounce)
		return
	}

	// Create new pending event
	pending := &pendingEvent{
		path:      path,
		triggerID: triggerID,
		priority:  priority,
	}

	pending.timer = time.AfterFunc(debounce, func() {
		w.fireScan(path, triggerID, priority)
	})

	w.pending[path] = pending
	log.Debug().Str("path", path).Str("debounce", debounce.String()).Msg("Scheduled debounced scan")
}

// fireScan queues a scan and/or upload after debounce period
func (w *Watcher) fireScan(path string, triggerID int64, priority int) {
	// Remove from pending
	w.pendingMu.Lock()
	delete(w.pending, path)
	w.pendingMu.Unlock()

	// Check settings
	scanningEnabled := true
	if val, _ := w.db.GetSetting("scanning.enabled"); val == "false" {
		scanningEnabled = false
	}
	uploadsEnabled := true
	if val, _ := w.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}

	// Get parent directory for the scan (more efficient for media servers)
	scanPath := filepath.Dir(path)

	// If scanning enabled, queue the scan
	if scanningEnabled {
		log.Info().
			Str("path", scanPath).
			Int64("trigger_id", triggerID).
			Msg("Inotify triggered scan")

		w.processor.QueueScan(processor.ScanRequest{
			Path:      scanPath,
			TriggerID: &triggerID,
			Priority:  priority,
		})
	}

	// If uploads enabled and upload manager set, queue upload for the file
	if uploadsEnabled && w.uploadManager != nil {
		log.Info().
			Str("path", path).
			Int64("trigger_id", triggerID).
			Msg("Inotify triggered upload")

		w.uploadManager.QueueUpload(uploader.UploadRequest{
			LocalPath: path,
			ScanID:    nil,
			Priority:  priority,
			TriggerID: &triggerID,
		})
	}
}

// isUnderPath checks if childPath is under or equal to parentPath
func isUnderPath(childPath, parentPath string) bool {
	rel, err := filepath.Rel(parentPath, childPath)
	if err != nil {
		return false
	}
	// If the relative path starts with "..", it's not under parentPath
	return len(rel) > 0 && rel[0] != '.'
}

// addWatchRecursive adds watches to a directory and all its subdirectories.
// Returns a slice of all paths that were successfully watched.
func (w *Watcher) addWatchRecursive(rootPath string) []string {
	var watched []string

	err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Debug().Err(err).Str("path", path).Msg("Error walking directory")
			return nil // Continue walking despite errors
		}

		if !d.IsDir() {
			return nil
		}

		if err := w.watcher.Add(path); err != nil {
			log.Debug().Err(err).Str("path", path).Msg("Failed to add watch for directory")
			return nil
		}

		watched = append(watched, path)
		return nil
	})

	if err != nil {
		log.Error().Err(err).Str("path", rootPath).Msg("Failed to walk directory tree")
	}

	return watched
}

// ScanExistingFiles walks all watched paths and queues existing files for upload.
// Called on startup to catch files created while the app was stopped.
// Only queues uploads - does not trigger media server scans.
func (w *Watcher) ScanExistingFiles() {
	uploadsEnabled := true
	if val, _ := w.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}
	if !uploadsEnabled || w.uploadManager == nil {
		return
	}

	w.mu.RLock()
	triggers := make([]*triggerWatch, 0, len(w.triggers))
	for _, tw := range w.triggers {
		triggers = append(triggers, tw)
	}
	w.mu.RUnlock()

	var fileCount int
	for _, tw := range triggers {
		triggerID := tw.trigger.ID
		for _, watchPath := range tw.paths {
			if err := filepath.WalkDir(watchPath, func(path string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				w.uploadManager.QueueUpload(uploader.UploadRequest{
					LocalPath: path,
					ScanID:    nil,
					Priority:  1000, // Lower priority than real-time events
					TriggerID: &triggerID,
				})
				fileCount++
				return nil
			}); err != nil {
				log.Warn().Err(err).Str("path", watchPath).Msg("Failed to walk directory for existing files")
			}
		}
	}

	if fileCount > 0 {
		log.Info().Int("files", fileCount).Msg("Queued existing files for upload from startup scan")
	}
}

// Stats returns watcher statistics
func (w *Watcher) Stats() WatcherStats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	pathCount := 0
	for _, tw := range w.triggers {
		pathCount += len(tw.paths)
	}

	w.pendingMu.Lock()
	pendingCount := len(w.pending)
	w.pendingMu.Unlock()

	return WatcherStats{
		TriggerCount: len(w.triggers),
		PathCount:    pathCount,
		PendingCount: pendingCount,
	}
}

// WatcherStats holds watcher statistics
type WatcherStats struct {
	TriggerCount int `json:"trigger_count"`
	PathCount    int `json:"path_count"`
	PendingCount int `json:"pending_count"`
}

package uploader

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/rclone"
	"github.com/saltyorg/autoplow/internal/scantracker"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

// Config holds upload manager configuration
type Config struct {
	BatchIntervalSeconds int           `json:"batch_interval_seconds"`
	RetryDelaySeconds    int           `json:"retry_delay_seconds"`
	MaxRetries           int           `json:"max_retries"`
	MaxBatchSize         int           `json:"max_batch_size"` // Maximum files per batch (default: 100)
	ProgressPollInterval time.Duration `json:"progress_poll_interval"`
	DeleteAfterUpload    bool          `json:"delete_after_upload"`
}

// UploadSkipChecker is an interface for checking if new uploads should be skipped
type UploadSkipChecker interface {
	ShouldSkipNewUploads() bool
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		BatchIntervalSeconds: 30,
		RetryDelaySeconds:    60,
		MaxRetries:           3,
		MaxBatchSize:         100, // Matches rclone's core/transferred ring buffer size
		ProgressPollInterval: 1 * time.Second,
		DeleteAfterUpload:    false,
	}
}

// UploadRequest represents a request to upload a path
type UploadRequest struct {
	LocalPath string
	ScanID    *int64
	Priority  int
	TriggerID *int64 // Which trigger initiated this upload (for exclusion checking)
}

// Manager handles upload processing
type Manager struct {
	db        *database.DB
	rcloneMgr *rclone.Manager
	config    Config
	sseBroker *sse.Broker

	// Signal that new upload requests were queued
	requestReady chan struct{}

	// Active batch tracking
	activeBatch   *activeBatch
	activeBatchMu sync.RWMutex
	batchActive   chan struct{} // signals when a batch becomes active
	batchReady    chan struct{} // signals batch processor to check for work immediately

	// Skip checker for bandwidth-based upload skipping
	skipChecker   UploadSkipChecker
	skipCheckerMu sync.RWMutex

	// Paused state - user can manually pause uploads
	paused   bool
	pausedMu sync.RWMutex

	// Database error state - halts all operations due to DB failure
	dbError   error
	dbErrorMu sync.RWMutex

	// Destination size check coordination
	sizeCheckMu         sync.Mutex
	sizeCheckInProgress map[int64]bool

	// Plex scan tracking for upload gating
	plexTracker *scantracker.PlexTracker

	// Shutdown handling
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	runMu   sync.Mutex
}

// activeBatch tracks an in-progress batch of uploads
type activeBatch struct {
	batchJobID   int64              // rclone job/batch job ID
	uploads      []*database.Upload // uploads in this batch
	startedAt    time.Time
	completedIDs map[int64]bool // track uploads already marked complete during progress monitoring
}

// Stats holds manager statistics
type Stats struct {
	ActiveUploads  int     `json:"active_uploads"`
	PendingUploads int     `json:"pending_uploads"`
	QueuedUploads  int     `json:"queued_uploads"`
	QueueSize      int     `json:"queue_size"`
	TotalBytes     int64   `json:"total_bytes"`
	CurrentSpeed   float64 `json:"current_speed"`
}

// New creates a new upload manager
func New(db *database.DB, rcloneMgr *rclone.Manager, config Config) *Manager {
	return &Manager{
		db:                  db,
		rcloneMgr:           rcloneMgr,
		config:              config,
		requestReady:        make(chan struct{}, 1),
		batchActive:         make(chan struct{}, 1),
		batchReady:          make(chan struct{}, 1),
		sizeCheckInProgress: make(map[int64]bool),
	}
}

// Start starts the upload manager
func (m *Manager) Start() {
	m.runMu.Lock()
	if m.running {
		m.runMu.Unlock()
		return
	}

	// Create fresh context for this run
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.running = true
	m.runMu.Unlock()

	log.Info().
		Int("batch_interval", m.config.BatchIntervalSeconds).
		Msg("Starting upload manager")

	// Reset paused state and database error on start
	m.pausedMu.Lock()
	m.paused = false
	m.pausedMu.Unlock()

	m.dbErrorMu.Lock()
	m.dbError = nil
	m.dbErrorMu.Unlock()

	// Recover orphaned uploads that were in "uploading" status when app was closed
	m.recoverOrphanedUploads()

	// Start request processor
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Upload request processor panicked")
			}
		}()
		m.requestProcessor()
	})

	// Start batch processor
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Upload batch processor panicked")
			}
		}()
		m.batchProcessor()
	})

	// Start progress monitor
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Upload progress monitor panicked")
			}
		}()
		m.progressMonitor()
	})
}

// IsRunning returns whether the upload manager is running
func (m *Manager) IsRunning() bool {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	return m.running
}

// recoverOrphanedUploads handles uploads stuck in "uploading" status from a previous session
// These uploads were interrupted mid-transfer and need to be resumed immediately
func (m *Manager) recoverOrphanedUploads() {
	uploads, err := m.db.ListActiveUploads()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list active uploads for recovery")
		return
	}

	if len(uploads) == 0 {
		return
	}

	log.Info().Int("count", len(uploads)).Msg("Found orphaned uploads from previous session")

	// Reset progress since we're starting over, but keep status as uploading
	for _, upload := range uploads {
		if err := m.db.UpdateUploadProgress(upload.ID, 0, nil); err != nil {
			log.Debug().Err(err).Int64("id", upload.ID).Msg("Failed to reset orphaned upload progress")
		}
		log.Info().Int64("id", upload.ID).Str("path", upload.LocalPath).Msg("Will resume orphaned upload")
	}

	// Start a goroutine to resume these uploads once rclone is ready
	go m.resumeOrphanedUploads(uploads)
}

// resumeOrphanedUploads waits for rclone to be ready and starts a batch with orphaned uploads
func (m *Manager) resumeOrphanedUploads(uploads []*database.Upload) {
	// Wait for rclone to be ready (check every second for up to 60 seconds)
	for range 60 {
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(time.Second):
		}

		if m.rcloneMgr.IsRunning() {
			log.Info().Int("count", len(uploads)).Msg("Rclone ready, resuming orphaned uploads")
			m.startBatch(uploads)
			return
		}
	}

	// Rclone didn't start in time, reset uploads to queued so they get picked up later
	log.Warn().Msg("Rclone not ready after 60s, resetting orphaned uploads to queued")
	for _, upload := range uploads {
		if err := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusQueued); err != nil {
			log.Error().Err(err).Int64("id", upload.ID).Msg("Failed to reset orphaned upload status")
		}
	}
}

// Stop stops the upload manager gracefully
func (m *Manager) Stop() {
	m.runMu.Lock()
	if !m.running {
		m.runMu.Unlock()
		return
	}
	m.runMu.Unlock()

	log.Info().Msg("Stopping upload manager")

	// Cancel any active batch job and reset uploads to queued
	m.activeBatchMu.Lock()
	if m.activeBatch != nil {
		batch := m.activeBatch
		log.Info().Int64("batch_job_id", batch.batchJobID).Msg("Cancelling active batch job")

		// Stop the rclone batch job
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := m.rcloneMgr.Client().StopJob(ctx, batch.batchJobID); err != nil {
			log.Warn().Err(err).Int64("job_id", batch.batchJobID).Msg("Failed to stop batch job")
		}
		cancel()

		// Reset in-flight uploads to queued so they'll be retried on restart
		for _, upload := range batch.uploads {
			if !batch.completedIDs[upload.ID] {
				if err := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusQueued); err != nil {
					log.Warn().Err(err).Int64("id", upload.ID).Msg("Failed to reset upload status")
				} else {
					log.Debug().Int64("id", upload.ID).Msg("Reset in-flight upload to queued")
				}
			}
		}
		m.activeBatch = nil
	}
	m.activeBatchMu.Unlock()

	// Signal goroutines to stop
	m.cancel()
	m.wg.Wait()

	m.runMu.Lock()
	m.running = false
	m.runMu.Unlock()

	log.Info().Msg("Upload manager stopped")
}

// SetSkipChecker sets the upload skip checker (typically the throttle manager)
func (m *Manager) SetSkipChecker(checker UploadSkipChecker) {
	m.skipCheckerMu.Lock()
	defer m.skipCheckerMu.Unlock()
	m.skipChecker = checker
}

// SetPlexScanTracker sets the Plex scan tracker for upload gating.
func (m *Manager) SetPlexScanTracker(tracker *scantracker.PlexTracker) {
	m.plexTracker = tracker
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

// IsPaused returns whether the upload manager is paused by the user
func (m *Manager) IsPaused() bool {
	m.pausedMu.RLock()
	defer m.pausedMu.RUnlock()
	return m.paused
}

// Pause pauses the upload manager (user-initiated)
// This only pauses new uploads - active uploads will continue
func (m *Manager) Pause() {
	m.pausedMu.Lock()
	defer m.pausedMu.Unlock()
	if !m.paused {
		m.paused = true
		log.Info().Msg("Upload manager paused by user")
		m.broadcastEvent(sse.EventUploadManagerPaused, map[string]any{
			"reason": "user",
		})
	}
}

// Resume resumes the upload manager (user-initiated)
func (m *Manager) Resume() {
	m.pausedMu.Lock()
	defer m.pausedMu.Unlock()
	if m.paused {
		m.paused = false
		log.Info().Msg("Upload manager resumed by user")
		m.broadcastEvent(sse.EventUploadManagerResumed, nil)
	}
}

// HasDatabaseError returns whether the upload manager has a database error
func (m *Manager) HasDatabaseError() bool {
	m.dbErrorMu.RLock()
	defer m.dbErrorMu.RUnlock()
	return m.dbError != nil
}

// GetDatabaseError returns the current database error, if any
func (m *Manager) GetDatabaseError() error {
	m.dbErrorMu.RLock()
	defer m.dbErrorMu.RUnlock()
	return m.dbError
}

// handleDatabaseError handles a database error by halting all upload operations.
// This prevents silent data loss when the database becomes inaccessible.
// A full application restart is required to recover from this state.
func (m *Manager) handleDatabaseError(err error, context string) {
	m.dbErrorMu.Lock()
	hadError := m.dbError != nil
	m.dbError = err
	m.dbErrorMu.Unlock()

	if !hadError {
		log.Error().
			Err(err).
			Str("context", context).
			Msg("DATABASE ERROR: Upload manager halted due to database failure. All operations suspended. A full application restart is required after fixing the database issue.")
		m.broadcastEvent(sse.EventUploadManagerPaused, map[string]any{
			"reason":  "database_error",
			"error":   err.Error(),
			"context": context,
		})
	}
}

// canOperate returns true if the manager can perform operations (not paused and no DB error)
func (m *Manager) canOperate() bool {
	if m.HasDatabaseError() {
		return false
	}
	return !m.IsPaused()
}

// shouldSkipNewUploads checks if new uploads should be skipped
func (m *Manager) shouldSkipNewUploads() bool {
	m.skipCheckerMu.RLock()
	defer m.skipCheckerMu.RUnlock()
	if m.skipChecker == nil {
		return false
	}
	return m.skipChecker.ShouldSkipNewUploads()
}

// QueueUpload queues a path for upload
func (m *Manager) QueueUpload(req UploadRequest) {
	entry := &database.UploadRequestEntry{
		ScanID:    req.ScanID,
		LocalPath: req.LocalPath,
		Priority:  req.Priority,
		TriggerID: req.TriggerID,
	}
	if err := m.db.CreateUploadRequest(entry); err != nil {
		m.handleDatabaseError(err, "CreateUploadRequest")
		return
	}

	log.Debug().
		Str("path", req.LocalPath).
		Msg("Upload request queued")

	select {
	case m.requestReady <- struct{}{}:
	default:
	}
}

// Stats returns current statistics
func (m *Manager) Stats() Stats {
	m.activeBatchMu.RLock()
	var activeCount int
	if m.activeBatch != nil {
		activeCount = len(m.activeBatch.uploads)
	}
	m.activeBatchMu.RUnlock()

	pendingCount, _ := m.db.CountUploads(database.UploadStatusPending)
	queuedCount, _ := m.db.CountUploads(database.UploadStatusQueued)

	var currentSpeed float64
	if m.rcloneMgr.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if stats, err := m.rcloneMgr.Client().GetStats(ctx); err == nil {
			currentSpeed = stats.Speed
		}
	}

	queueCount, _ := m.db.CountUploadRequests()

	return Stats{
		ActiveUploads:  activeCount,
		PendingUploads: pendingCount,
		QueuedUploads:  queuedCount,
		QueueSize:      queueCount,
		CurrentSpeed:   currentSpeed,
	}
}

// ActiveTransfers represents active transfer information for the UI
type ActiveTransfers struct {
	Stats        *rclone.TransferStats
	Transfers    []rclone.TransferringFile
	BatchJobID   int64
	TotalFiles   int
	Completed    int
	Transferring int
}

// GetActiveTransfers returns information about currently active transfers
func (m *Manager) GetActiveTransfers() *ActiveTransfers {
	m.activeBatchMu.RLock()
	hasBatch := m.activeBatch != nil
	var batchJobID int64
	var totalFiles int
	if hasBatch {
		batchJobID = m.activeBatch.batchJobID
		totalFiles = len(m.activeBatch.uploads)
	}
	m.activeBatchMu.RUnlock()

	if !hasBatch || !m.rcloneMgr.IsRunning() {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use the autoplow stats group for batch progress tracking
	stats, err := m.rcloneMgr.Client().GetGroupStats(ctx, "autoplow")
	if err != nil {
		return nil
	}

	// Get completed count from core/transferred which tracks actual completions
	transferred, _ := m.rcloneMgr.Client().GetGroupTransferred(ctx, "autoplow")
	completedCount := len(transferred)

	return &ActiveTransfers{
		Stats:        stats,
		Transfers:    stats.Transferring,
		BatchJobID:   batchJobID,
		TotalFiles:   totalFiles,
		Completed:    completedCount,
		Transferring: len(stats.Transferring),
	}
}

// requestProcessor handles incoming upload requests
func (m *Manager) requestProcessor() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	m.drainUploadRequests()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.requestReady:
			m.drainUploadRequests()
		case <-ticker.C:
			m.drainUploadRequests()
		}
	}
}

func (m *Manager) drainUploadRequests() {
	const batchSize = 200

	if m.HasDatabaseError() {
		return
	}

	for {
		requests, err := m.db.ListUploadRequests(batchSize)
		if err != nil {
			m.handleDatabaseError(err, "ListUploadRequests")
			return
		}
		if len(requests) == 0 {
			return
		}

		ids := make([]int64, 0, len(requests))
		for _, req := range requests {
			m.handleRequest(UploadRequest{
				LocalPath: req.LocalPath,
				ScanID:    req.ScanID,
				Priority:  req.Priority,
				TriggerID: req.TriggerID,
			})
			ids = append(ids, req.ID)
		}

		if err := m.db.DeleteUploadRequests(ids); err != nil {
			m.handleDatabaseError(err, "DeleteUploadRequests")
			return
		}

		if len(requests) < batchSize {
			return
		}
	}
}

// handleRequest processes a single upload request
func (m *Manager) handleRequest(req UploadRequest) {
	log.Debug().
		Str("path", req.LocalPath).
		Msg("Processing upload request")

	// Check if new uploads should be skipped due to low bandwidth
	if m.shouldSkipNewUploads() {
		log.Debug().
			Str("path", req.LocalPath).
			Msg("Skipping upload request due to low bandwidth")
		return
	}

	// Find matching destination configuration
	dest, err := m.db.GetDestinationByPath(req.LocalPath)
	if err != nil {
		log.Error().Err(err).Str("path", req.LocalPath).Msg("Failed to find destination")
		return
	}
	if dest == nil {
		log.Debug().Str("path", req.LocalPath).Msg("No destination configured for this path")
		return
	}
	if len(dest.Remotes) == 0 {
		log.Warn().Str("path", req.LocalPath).Msg("Destination has no remotes configured")
		return
	}

	// Check if this trigger is allowed for this destination
	if req.TriggerID != nil && !dest.ShouldAllowTrigger(*req.TriggerID) {
		log.Debug().
			Str("path", req.LocalPath).
			Int64("trigger_id", *req.TriggerID).
			Msg("Trigger not in included list for this destination, skipping")
		return
	}

	// Get file info for size
	var sizeBytes *int64
	if info, err := os.Stat(req.LocalPath); err == nil && !info.IsDir() {
		size := info.Size()
		sizeBytes = &size
	}

	// Create upload record for the first (highest priority) remote
	firstRemote := dest.Remotes[0]

	// Calculate relative path from destination base
	relativePath := strings.TrimPrefix(req.LocalPath, dest.LocalPath)
	relativePath = strings.TrimPrefix(relativePath, "/")
	remotePath := filepath.Join(firstRemote.RemotePath, relativePath)

	// Check for duplicate
	existing, err := m.db.FindDuplicateUpload(req.LocalPath, firstRemote.RemoteName)
	if err != nil {
		log.Error().Err(err).Msg("Failed to check for duplicate upload")
		return
	}
	if existing != nil {
		log.Debug().
			Str("path", req.LocalPath).
			Str("status", string(existing.Status)).
			Msg("Upload already exists for this path")
		return
	}

	// Determine initial status based on readiness checks
	ready, waitState, waitChecks := m.evaluateUploadReadiness(dest, &database.Upload{
		LocalPath: req.LocalPath,
	})
	status := database.UploadStatusPending
	if ready {
		status = database.UploadStatusQueued
	}

	upload := &database.Upload{
		ScanID:         req.ScanID,
		LocalPath:      req.LocalPath,
		RemoteName:     firstRemote.RemoteName,
		RemotePath:     remotePath,
		Status:         status,
		SizeBytes:      sizeBytes,
		RemotePriority: firstRemote.Priority,
		WaitState:      waitState,
		WaitChecks:     waitChecks,
	}

	if err := m.db.CreateUpload(upload); err != nil {
		log.Error().Err(err).Str("path", req.LocalPath).Msg("Failed to create upload record")
		return
	}

	log.Info().
		Int64("id", upload.ID).
		Str("path", req.LocalPath).
		Str("remote", firstRemote.RemoteName).
		Str("status", string(status)).
		Msg("Upload queued")

	// Signal batch processor if file is ready for immediate upload
	if status == database.UploadStatusQueued {
		select {
		case m.batchReady <- struct{}{}:
		default:
		}
	}
}

// batchProcessor periodically processes ready uploads
func (m *Manager) batchProcessor() {
	ticker := time.NewTicker(time.Duration(m.config.BatchIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.processBatch()
		case <-m.batchReady:
			m.processBatch()
		}
	}
}

// processBatch processes a batch of ready uploads
func (m *Manager) processBatch() {
	// Check if we can operate (not paused and no DB error)
	if !m.canOperate() {
		log.Debug().Msg("Upload manager cannot operate, skipping batch processing")
		return
	}

	// Check if rclone is available
	if !m.rcloneMgr.IsRunning() {
		log.Debug().Msg("Rclone not running, skipping batch processing")
		return
	}

	// Check pending uploads for readiness (age/size modes)
	m.checkPendingUploads()

	// If a batch is already running, check its status
	m.activeBatchMu.Lock()
	if m.activeBatch != nil {
		batch := m.activeBatch
		m.activeBatchMu.Unlock()

		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
		status, err := m.rcloneMgr.Client().GetJobStatus(ctx, batch.batchJobID)
		cancel()

		if err != nil {
			log.Warn().Err(err).Int64("job_id", batch.batchJobID).Msg("Failed to get batch job status")
			return
		}

		if !status.Finished {
			// Batch still running, skip this cycle
			log.Debug().Int64("job_id", batch.batchJobID).Msg("Batch still running")
			return
		}

		// Batch finished - process results
		m.handleBatchComplete(batch, status)
		return
	}
	m.activeBatchMu.Unlock()

	// No batch running, get queued uploads and start a new batch
	uploads, err := m.db.ListQueuedUploads()
	if err != nil {
		m.handleDatabaseError(err, "ListQueuedUploads")
		return
	}

	if len(uploads) == 0 {
		return
	}

	// Start a new batch with all queued uploads
	m.startBatch(uploads)
}

// checkPendingUploads checks pending uploads for readiness
func (m *Manager) checkPendingUploads() {
	uploads, err := m.db.ListPendingUploads()
	if err != nil {
		m.handleDatabaseError(err, "ListPendingUploads")
		return
	}

	for _, upload := range uploads {
		ready, waitState, waitChecks := m.checkUploadReadiness(upload)
		if err := m.db.UpdateUploadWaitState(upload.ID, waitState, waitChecks); err != nil {
			m.handleDatabaseError(err, "UpdateUploadWaitState")
			return
		}
		if ready {
			if err := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusQueued); err != nil {
				m.handleDatabaseError(err, "UpdateUploadStatus (pending to queued)")
				return
			}
			m.broadcastEvent(sse.EventUploadQueued, map[string]any{"upload_id": upload.ID, "path": upload.LocalPath})
			log.Info().Int64("id", upload.ID).Str("path", upload.LocalPath).Msg("Upload ready for processing")
		}
	}
}

// checkUploadReadiness checks if upload meets mode conditions
func (m *Manager) checkUploadReadiness(upload *database.Upload) (bool, string, string) {
	// Find the destination configuration
	dest, err := m.db.GetDestinationByPath(upload.LocalPath)
	if err != nil || dest == nil {
		waitChecks := m.marshalWaitChecks(map[string]any{
			"destination_found": false,
		})
		return false, "waiting_config", waitChecks
	}

	return m.evaluateUploadReadiness(dest, upload)
}

func (m *Manager) evaluateUploadReadiness(dest *database.Destination, upload *database.Upload) (bool, string, string) {
	checks := map[string]any{
		"destination_id": dest.ID,
	}

	if dest.UsePlexTracking {
		checks["plex_tracking_enabled"] = true
		if len(dest.PlexTargets) == 0 || m.plexTracker == nil {
			checks["plex_configured"] = len(dest.PlexTargets) > 0
			checks["plex_tracker_available"] = m.plexTracker != nil
			return false, "waiting_config", m.marshalWaitChecks(checks)
		}

		targetChecks := make([]map[string]any, 0, len(dest.PlexTargets))
		allReady := true
		for _, target := range dest.PlexTargets {
			status := m.plexTracker.CheckPath(dest.ID, target.TargetID, upload.LocalPath)
			entry := map[string]any{
				"target_id":              target.TargetID,
				"idle_threshold_seconds": target.IdleThresholdSeconds,
				"ready":                  status.Ready,
				"matched_scan":           status.Matched,
			}
			if status.Pending {
				entry["pending"] = true
				allReady = false
			}
			if !status.Matched {
				entry["waiting_on_scan"] = true
				allReady = false
			}
			targetChecks = append(targetChecks, entry)
		}
		checks["plex_targets"] = targetChecks

		if allReady {
			return true, "ready", m.marshalWaitChecks(checks)
		}
		return false, "waiting_plex", m.marshalWaitChecks(checks)
	}

	ageReady := true
	sizeReady := true

	if dest.MinFileAgeMinutes > 0 {
		checks["age_required_minutes"] = dest.MinFileAgeMinutes
		info, err := os.Stat(upload.LocalPath)
		if err != nil {
			ageReady = false
			checks["age_error"] = err.Error()
		} else {
			ageMinutes := time.Since(info.ModTime()).Minutes()
			checks["age_minutes"] = ageMinutes
			ageReady = ageMinutes >= float64(dest.MinFileAgeMinutes)
		}
	}
	checks["age_ready"] = ageReady

	if dest.MinFolderSizeGB > 0 {
		checks["size_required_gb"] = dest.MinFolderSizeGB
		if !m.startSizeCheck(dest.ID) {
			sizeReady = false
			checks["size_check_in_progress"] = true
		} else {
			sizeBytes, err := m.calculateFolderSize(dest.LocalPath)
			m.finishSizeCheck(dest.ID)
			if err != nil {
				sizeReady = false
				checks["size_error"] = err.Error()
			} else {
				checks["size_bytes"] = sizeBytes
				requiredBytes := int64(dest.MinFolderSizeGB) * 1024 * 1024 * 1024
				checks["size_required_bytes"] = requiredBytes
				sizeReady = sizeBytes >= requiredBytes
			}
		}
	}
	checks["size_ready"] = sizeReady

	ready := ageReady && sizeReady
	waitState := "ready"
	if dest.MinFolderSizeGB > 0 || dest.MinFileAgeMinutes > 0 {
		if !ready {
			if dest.MinFolderSizeGB > 0 && !sizeReady {
				waitState = "waiting_size"
			} else if dest.MinFileAgeMinutes > 0 && !ageReady {
				waitState = "waiting_age"
			} else {
				waitState = "pending"
			}
		}
	}

	return ready, waitState, m.marshalWaitChecks(checks)
}

func (m *Manager) marshalWaitChecks(checks map[string]any) string {
	if len(checks) == 0 {
		return ""
	}
	data, err := json.Marshal(checks)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to marshal upload wait checks")
		return ""
	}
	return string(data)
}

func (m *Manager) startSizeCheck(destinationID int64) bool {
	m.sizeCheckMu.Lock()
	defer m.sizeCheckMu.Unlock()
	if m.sizeCheckInProgress[destinationID] {
		return false
	}
	m.sizeCheckInProgress[destinationID] = true
	return true
}

func (m *Manager) finishSizeCheck(destinationID int64) {
	m.sizeCheckMu.Lock()
	defer m.sizeCheckMu.Unlock()
	delete(m.sizeCheckInProgress, destinationID)
}

func (m *Manager) calculateFolderSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// startBatch starts a new batch of uploads
func (m *Manager) startBatch(uploads []*database.Upload) {
	// Clear any stale stats from previous batch (important when rclone persists between app restarts)
	cleanupCtx, cleanupCancel := context.WithTimeout(m.ctx, 5*time.Second)
	if err := m.rcloneMgr.Client().StatsDelete(cleanupCtx, "autoplow"); err != nil {
		log.Debug().Err(err).Msg("Failed to clear stale rclone stats (may not exist)")
	}
	cleanupCancel()

	// Limit batch size to MaxBatchSize (matches rclone's core/transferred ring buffer)
	maxSize := m.config.MaxBatchSize
	if maxSize <= 0 {
		maxSize = 100 // fallback default
	}
	if len(uploads) > maxSize {
		log.Info().
			Int("queued", len(uploads)).
			Int("batch_size", maxSize).
			Msg("Limiting batch size, remaining uploads will be processed in next batch")
		uploads = uploads[:maxSize]
	}

	// Get all remote configurations upfront
	remotes, err := m.db.ListRemotes()
	if err != nil {
		m.handleDatabaseError(err, "ListRemotes")
		return
	}

	remoteMap := make(map[string]*database.Remote)
	for _, r := range remotes {
		remoteMap[r.Name] = r
	}

	// Build batch inputs for all uploads
	var batchInputs []rclone.BatchInput
	var batchUploads []*database.Upload

	// Cache for destination lookups
	destCache := make(map[string]*database.Destination)

	for _, upload := range uploads {
		remote, ok := remoteMap[upload.RemoteName]
		if !ok {
			log.Warn().
				Int64("id", upload.ID).
				Str("remote", upload.RemoteName).
				Msg("Remote not found, skipping upload")
			if err := m.db.UpdateUploadError(upload.ID, fmt.Sprintf("remote not found: %s", upload.RemoteName)); err != nil {
				m.handleDatabaseError(err, "UpdateUploadError")
			}
			continue
		}

		// Determine operation based on destination's transfer type
		var opPath string
		dest, ok := destCache[upload.LocalPath]
		if !ok {
			var err error
			dest, err = m.db.GetDestinationByPath(upload.LocalPath)
			if err != nil {
				log.Warn().Err(err).Str("path", upload.LocalPath).Msg("Failed to get destination config")
			}
			destCache[upload.LocalPath] = dest
		}

		// Default to move, but respect destination setting
		if dest != nil && dest.TransferType == database.TransferTypeCopy {
			opPath = "operations/copyfile"
		} else {
			opPath = "operations/movefile"
		}

		// Build rclone parameters
		srcFs := filepath.Dir(upload.LocalPath)
		srcRemote := filepath.Base(upload.LocalPath)
		dstFs := remote.RcloneRemote + filepath.Dir(upload.RemotePath)
		dstRemote := filepath.Base(upload.RemotePath)

		params := map[string]any{
			"srcFs":     srcFs,
			"srcRemote": srcRemote,
			"dstFs":     dstFs,
			"dstRemote": dstRemote,
			"_group":    "autoplow", // Stats group for progress tracking
		}

		// Merge transfer options from remote
		maps.Copy(params, remote.TransferOptions)

		batchInputs = append(batchInputs, rclone.BatchInput{
			Path:   opPath,
			Params: params,
		})
		batchUploads = append(batchUploads, upload)

		// Update status to uploading
		if err := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusUploading); err != nil {
			m.handleDatabaseError(err, "UpdateUploadStatus (to uploading)")
			continue
		}
		m.broadcastEvent(sse.EventUploadStarted, map[string]any{
			"upload_id": upload.ID,
			"path":      upload.LocalPath,
			"remote":    upload.RemoteName,
		})

		log.Info().
			Int64("id", upload.ID).
			Str("path", upload.LocalPath).
			Str("remote", upload.RemoteName).
			Msg("Upload added to batch")
	}

	if len(batchInputs) == 0 {
		log.Debug().Msg("No valid uploads for batch")
		return
	}

	// Pre-create unique remote directories to avoid race conditions
	// when multiple concurrent uploads try to create the same parent directory
	uniqueDirs := make(map[string]struct{})
	for _, input := range batchInputs {
		if dstFs, ok := input.Params["dstFs"].(string); ok {
			uniqueDirs[dstFs] = struct{}{}
		}
	}

	for dir := range uniqueDirs {
		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
		err := m.rcloneMgr.Client().Mkdir(ctx, dir)
		cancel()
		if err != nil {
			// Log but don't fail - the directory might already exist
			// or the backend might not support explicit mkdir
			log.Debug().Err(err).Str("dir", dir).Msg("Pre-creating directory (may already exist)")
		} else {
			log.Debug().Str("dir", dir).Msg("Pre-created remote directory")
		}
	}

	// Submit batch to rclone
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	batchJobID, err := m.rcloneMgr.Client().Batch(ctx, batchInputs)
	cancel()

	if err != nil {
		log.Error().Err(err).Msg("Failed to start batch job")
		// Mark all uploads as failed
		for _, upload := range batchUploads {
			m.handleUploadFailed(upload, fmt.Errorf("batch start failed: %w", err))
		}
		return
	}

	// Track the active batch
	m.activeBatchMu.Lock()
	m.activeBatch = &activeBatch{
		batchJobID:   batchJobID,
		uploads:      batchUploads,
		startedAt:    time.Now(),
		completedIDs: make(map[int64]bool),
	}
	m.activeBatchMu.Unlock()

	// Signal progress monitor that a batch is active
	select {
	case m.batchActive <- struct{}{}:
	default:
		// Already signaled
	}

	log.Info().
		Int64("batch_job_id", batchJobID).
		Int("upload_count", len(batchUploads)).
		Msg("Batch job started")
}

// handleBatchComplete processes the results of a completed batch
func (m *Manager) handleBatchComplete(batch *activeBatch, status *rclone.JobStatus) {
	log.Info().
		Int64("batch_job_id", batch.batchJobID).
		Bool("success", status.Success).
		Float64("duration", status.Duration).
		Msg("Batch job completed")

	// Parse batch output to get per-operation results
	var results []rclone.BatchResultItem
	if status.Output != nil {
		if outputList, ok := status.Output.([]any); ok {
			for _, item := range outputList {
				if itemMap, ok := item.(map[string]any); ok {
					result := rclone.BatchResultItem{}
					if success, ok := itemMap["success"].(bool); ok {
						result.Success = success
					}
					if errStr, ok := itemMap["error"].(string); ok {
						result.Error = errStr
					}
					if statusCode, ok := itemMap["status"].(float64); ok {
						result.Status = int(statusCode)
					}
					results = append(results, result)
				}
			}
		}
	}

	// Track source directories for cleanup (only for uploads using move mode)
	var sourceDirsToClean []string

	// Cache for destination lookups
	destCache := make(map[string]*database.Destination)

	// Helper to check if upload uses move mode
	isMoveModeUpload := func(localPath string) bool {
		dest, ok := destCache[localPath]
		if !ok {
			var err error
			dest, err = m.db.GetDestinationByPath(localPath)
			if err != nil {
				log.Warn().Err(err).Str("path", localPath).Msg("Failed to get destination config for cleanup check")
			}
			destCache[localPath] = dest
		}
		// Default to move mode
		return dest == nil || dest.TransferType != database.TransferTypeCopy
	}

	// Process each upload result (skip uploads already handled via core/transferred)
	for i, upload := range batch.uploads {
		// Skip if already processed during progress monitoring
		if batch.completedIDs[upload.ID] {
			// Still track source directory for cleanup in move mode
			if isMoveModeUpload(upload.LocalPath) {
				srcDir := filepath.Dir(upload.LocalPath)
				sourceDirsToClean = append(sourceDirsToClean, srcDir)
			}
			continue
		}

		var success bool
		var errMsg string

		if i < len(results) {
			success = results[i].Success
			errMsg = results[i].Error
		} else {
			// If no result for this index, check overall batch status
			success = status.Success
			errMsg = status.Error
		}

		if success {
			m.handleUploadComplete(upload)
			// Track source directory for cleanup in move mode
			if isMoveModeUpload(upload.LocalPath) {
				srcDir := filepath.Dir(upload.LocalPath)
				sourceDirsToClean = append(sourceDirsToClean, srcDir)
			}
		} else {
			if errMsg == "" {
				errMsg = "batch operation failed"
			}
			m.handleUploadFailed(upload, fmt.Errorf("%s", errMsg))
		}
	}

	// Clear the active batch
	m.activeBatchMu.Lock()
	m.activeBatch = nil
	m.activeBatchMu.Unlock()

	// Signal batch processor to check for more work immediately
	select {
	case m.batchReady <- struct{}{}:
	default:
		// Already signaled
	}

	// Cleanup empty source directories after move operations
	if len(sourceDirsToClean) > 0 {
		m.cleanupEmptyDirectories(sourceDirsToClean)
	}
}

// cleanupEmptyDirectories removes empty directories after move operations
func (m *Manager) cleanupEmptyDirectories(dirs []string) {
	// Get all destinations to ensure we only clean within them
	dests, err := m.db.ListDestinations()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list destinations for cleanup")
		return
	}

	// Build set of unique directories and deduplicate
	dirSet := make(map[string]bool)
	for _, dir := range dirs {
		dirSet[dir] = true
	}

	// Sort directories by depth (deepest first) to clean bottom-up
	var sortedDirs []string
	for dir := range dirSet {
		sortedDirs = append(sortedDirs, dir)
	}
	// Sort by path length descending (longer paths = deeper directories)
	for i := 0; i < len(sortedDirs)-1; i++ {
		for j := i + 1; j < len(sortedDirs); j++ {
			if len(sortedDirs[i]) < len(sortedDirs[j]) {
				sortedDirs[i], sortedDirs[j] = sortedDirs[j], sortedDirs[i]
			}
		}
	}

	for _, dir := range sortedDirs {
		// Check if this directory is under a configured destination
		isUnderDest := false
		for _, dest := range dests {
			if strings.HasPrefix(dir, dest.LocalPath) && dir != dest.LocalPath {
				isUnderDest = true
				break
			}
		}
		if !isUnderDest {
			continue
		}

		// Try to remove the directory (will fail if not empty)
		if err := os.Remove(dir); err == nil {
			log.Debug().Str("dir", dir).Msg("Removed empty directory")
		}
	}
}

// progressMonitor monitors active batch for progress
func (m *Manager) progressMonitor() {
	ticker := time.NewTicker(m.config.ProgressPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.batchActive:
			// Batch started, begin polling until it completes
			m.pollUntilBatchComplete(ticker)
		}
	}
}

// pollUntilBatchComplete polls for progress until the batch completes
func (m *Manager) pollUntilBatchComplete(ticker *time.Ticker) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.activeBatchMu.RLock()
			batch := m.activeBatch
			m.activeBatchMu.RUnlock()

			if batch == nil {
				// Batch complete, stop polling and wait for next batch
				return
			}
			m.checkBatchProgress()
		}
	}
}

// checkBatchProgress checks progress of the active batch
func (m *Manager) checkBatchProgress() {
	m.activeBatchMu.RLock()
	batch := m.activeBatch
	m.activeBatchMu.RUnlock()

	if batch == nil {
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	// Check job status to catch fast completions (e.g., server-side copies)
	status, err := m.rcloneMgr.Client().GetJobStatus(ctx, batch.batchJobID)
	if err != nil {
		log.Debug().Err(err).Int64("job_id", batch.batchJobID).Msg("Failed to get job status")
	} else if status.Finished {
		// Job completed, handle it immediately
		m.handleBatchComplete(batch, status)
		return
	}

	// Build a map of filename -> upload for quick lookup
	uploadByName := make(map[string]*database.Upload)
	for _, upload := range batch.uploads {
		filename := filepath.Base(upload.LocalPath)
		uploadByName[filename] = upload
	}

	// Get overall stats for the batch job using the autoplow stats group
	stats, err := m.rcloneMgr.Client().GetGroupStats(ctx, "autoplow")
	if err != nil {
		log.Debug().Err(err).Int64("job_id", batch.batchJobID).Msg("Failed to get batch job stats")
		return
	}

	// Update per-file progress from Transferring list
	if len(stats.Transferring) > 0 {
		// Update progress for each actively transferring file
		for _, tf := range stats.Transferring {
			if upload, ok := uploadByName[tf.Name]; ok {
				if err := m.db.UpdateUploadProgress(upload.ID, tf.Bytes, nil); err != nil {
					log.Debug().Err(err).Int64("id", upload.ID).Msg("Failed to update upload progress")
				}

				// Broadcast per-file progress
				m.broadcastEvent(sse.EventUploadProgress, map[string]any{
					"upload_id":  upload.ID,
					"bytes":      tf.Bytes,
					"total":      tf.Size,
					"speed":      tf.Speed,
					"percentage": tf.Percentage,
				})
			}
		}
	}

	// Check for completed transfers via core/transferred (real-time per-file completion)
	transferred, err := m.rcloneMgr.Client().GetGroupTransferred(ctx, "autoplow")
	if err != nil {
		log.Debug().Err(err).Int64("job_id", batch.batchJobID).Msg("Failed to get transferred files")
	} else {
		m.activeBatchMu.Lock()
		for _, tf := range transferred {
			upload, ok := uploadByName[tf.Name]
			if !ok {
				continue
			}
			// Skip if already processed
			if batch.completedIDs[upload.ID] {
				continue
			}
			batch.completedIDs[upload.ID] = true

			if tf.Error == "" {
				m.handleUploadComplete(upload)
				log.Debug().
					Int64("upload_id", upload.ID).
					Str("name", tf.Name).
					Msg("Upload completed (detected via core/transferred)")
			} else {
				m.handleUploadFailed(upload, fmt.Errorf("%s", tf.Error))
				log.Debug().
					Int64("upload_id", upload.ID).
					Str("name", tf.Name).
					Str("error", tf.Error).
					Msg("Upload failed (detected via core/transferred)")
			}
		}
		m.activeBatchMu.Unlock()
	}

	// Broadcast overall batch progress
	m.broadcastEvent(sse.EventUploadProgress, map[string]any{
		"batch_job_id": batch.batchJobID,
		"bytes":        stats.Bytes,
		"total":        stats.TotalBytes,
		"speed":        stats.Speed,
		"transfers":    stats.Transfers,
		"total_files":  stats.TotalTransfers,
	})

	log.Debug().
		Int64("batch_job_id", batch.batchJobID).
		Int64("bytes", stats.Bytes).
		Int64("total", stats.TotalBytes).
		Float64("speed", stats.Speed).
		Int("transferring", len(stats.Transferring)).
		Int("completed", len(batch.completedIDs)).
		Msg("Batch progress")

	// If all uploads are done and nothing is transferring, signal batch processor
	// to check job status immediately rather than waiting for the next tick
	if len(stats.Transferring) == 0 && len(batch.completedIDs) == len(batch.uploads) {
		select {
		case m.batchReady <- struct{}{}:
		default:
		}
	}
}

// handleUploadComplete handles successful upload completion
func (m *Manager) handleUploadComplete(upload *database.Upload) {
	log.Info().
		Int64("id", upload.ID).
		Str("path", upload.LocalPath).
		Str("remote", upload.RemoteName).
		Msg("Upload completed")

	// Update status
	if err := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusCompleted); err != nil {
		m.handleDatabaseError(err, "UpdateUploadStatus (to completed)")
		// Continue - the upload succeeded in rclone, we should still try to record history
	}
	m.broadcastEvent(sse.EventUploadCompleted, map[string]any{"upload_id": upload.ID, "path": upload.LocalPath, "remote": upload.RemoteName})

	// Create history record
	history := &database.UploadHistory{
		UploadID:   &upload.ID,
		LocalPath:  upload.LocalPath,
		RemoteName: upload.RemoteName,
		RemotePath: upload.RemotePath,
		SizeBytes:  upload.SizeBytes,
	}

	if err := m.db.CreateUploadHistory(history); err != nil {
		m.handleDatabaseError(err, "CreateUploadHistory")
	}
}

// handleUploadFailed handles upload failure with retry/failover
func (m *Manager) handleUploadFailed(upload *database.Upload, err error) {
	log.Error().
		Err(err).
		Int64("id", upload.ID).
		Str("path", upload.LocalPath).
		Str("remote", upload.RemoteName).
		Int("retry_count", upload.RetryCount).
		Msg("Upload failed")

	// Update error in database
	if dbErr := m.db.UpdateUploadError(upload.ID, err.Error()); dbErr != nil {
		m.handleDatabaseError(dbErr, "UpdateUploadError")
	}
	m.broadcastEvent(sse.EventUploadFailed, map[string]any{"upload_id": upload.ID, "path": upload.LocalPath, "error": err.Error()})

	// Check retry count
	if upload.RetryCount >= m.config.MaxRetries {
		// Try failover to next remote
		if m.failoverToNextRemote(upload) {
			log.Info().
				Int64("id", upload.ID).
				Msg("Failing over to next remote")
			return
		}

		log.Error().
			Int64("id", upload.ID).
			Msg("Upload failed permanently, no more remotes to try")
		return
	}

	// Schedule retry - status is already set to failed with incremented retry_count
	// The batch processor will pick it up again after retry delay
	log.Info().
		Int64("id", upload.ID).
		Int("retry_count", upload.RetryCount+1).
		Int("max_retries", m.config.MaxRetries).
		Msg("Upload will be retried")

	// Reset to queued for retry
	if dbErr := m.db.UpdateUploadStatus(upload.ID, database.UploadStatusQueued); dbErr != nil {
		m.handleDatabaseError(dbErr, "UpdateUploadStatus (retry)")
	} else {
		m.broadcastEvent(sse.EventUploadQueued, map[string]any{"upload_id": upload.ID, "path": upload.LocalPath})
	}
}

// failoverToNextRemote attempts to use the next priority remote
func (m *Manager) failoverToNextRemote(upload *database.Upload) bool {
	// Find the destination configuration
	dest, err := m.db.GetDestinationByPath(upload.LocalPath)
	if err != nil || dest == nil {
		return false
	}

	// Find next remote with higher priority number (lower priority)
	var nextRemote *database.DestinationRemote
	for _, remote := range dest.Remotes {
		if remote.Priority > upload.RemotePriority {
			nextRemote = remote
			break
		}
	}

	if nextRemote == nil {
		return false // No more remotes to try
	}

	// Calculate remote path
	relativePath := strings.TrimPrefix(upload.LocalPath, dest.LocalPath)
	relativePath = strings.TrimPrefix(relativePath, "/")
	remotePath := filepath.Join(nextRemote.RemotePath, relativePath)

	// Update upload to use next remote
	if err := m.db.UpdateUploadRemote(upload.ID, nextRemote.RemoteName, remotePath, nextRemote.Priority); err != nil {
		m.handleDatabaseError(err, "UpdateUploadRemote (failover)")
		return false
	}

	log.Info().
		Int64("id", upload.ID).
		Str("new_remote", nextRemote.RemoteName).
		Int("new_priority", nextRemote.Priority).
		Msg("Upload failing over to next remote")

	return true
}

// CancelUpload cancels an active or queued upload
func (m *Manager) CancelUpload(uploadID int64) error {
	// Check if there's an active batch containing this upload
	m.activeBatchMu.Lock()
	if m.activeBatch != nil {
		for _, upload := range m.activeBatch.uploads {
			if upload.ID == uploadID {
				// Stop the entire batch job (can't cancel individual items)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := m.rcloneMgr.Client().StopJob(ctx, m.activeBatch.batchJobID); err != nil {
					log.Warn().Err(err).Int64("job_id", m.activeBatch.batchJobID).Msg("Failed to stop batch job")
				}
				cancel()
				m.activeBatch = nil
				break
			}
		}
	}
	m.activeBatchMu.Unlock()

	// Delete from database
	return m.db.DeleteUpload(uploadID)
}

// RetryUpload retries a failed upload
func (m *Manager) RetryUpload(uploadID int64) error {
	upload, err := m.db.GetUpload(uploadID)
	if err != nil {
		return err
	}
	if upload == nil {
		return fmt.Errorf("upload not found")
	}
	if upload.Status != database.UploadStatusFailed {
		return fmt.Errorf("upload is not in failed status")
	}

	if err := m.db.UpdateUploadStatus(uploadID, database.UploadStatusQueued); err != nil {
		return err
	}
	m.broadcastEvent(sse.EventUploadQueued, map[string]any{"upload_id": uploadID, "path": upload.LocalPath})
	return nil
}

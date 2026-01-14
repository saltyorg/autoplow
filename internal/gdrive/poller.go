package gdrive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/processor"
	"github.com/saltyorg/autoplow/internal/targets"
	"github.com/saltyorg/autoplow/internal/triggerpaths"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

const pollInterval = 60 * time.Second
const gdriveFolderMimeType = "application/vnd.google-apps.folder"
const gdriveTraceLimit = 20
const gdriveInitialSyncRetryDelay = 5 * time.Minute

// Poller watches Google Drive for changes and queues scans.
type Poller struct {
	db        *database.Manager
	proc      *processor.Processor
	service   *Service
	sseBroker *sse.Broker

	mu       sync.Mutex
	triggers map[int64]*triggerPoll
	running  bool
	ctx      context.Context
	cancel   context.CancelFunc
}

type triggerPoll struct {
	trigger   *database.Trigger
	ctx       context.Context
	cancel    context.CancelFunc
	driveID   string
	driveName string
	rootID    string
	snapshot  map[string]*database.GDriveSnapshotEntry
	loaded    bool
}

// New creates a new Google Drive poller.
func New(db *database.Manager, proc *processor.Processor) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	return &Poller{
		db:       db,
		proc:     proc,
		service:  NewService(db),
		triggers: make(map[int64]*triggerPoll),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// SetSSEBroker sets the SSE broker for broadcasting events.
func (p *Poller) SetSSEBroker(broker *sse.Broker) {
	p.sseBroker = broker
}

func (p *Poller) broadcastEvent(eventType sse.EventType, data map[string]any) {
	if p.sseBroker != nil {
		p.sseBroker.Broadcast(sse.Event{Type: eventType, Data: data})
	}
}

// Start starts the poller and loads all gdrive triggers.
// Returns true if the poller was started (triggers exist), false otherwise.
func (p *Poller) Start() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return true, nil
	}

	if err := p.loadTriggersLocked(); err != nil {
		return false, err
	}

	if len(p.triggers) == 0 {
		return false, nil
	}

	p.running = true
	log.Info().Msg("Google Drive watcher started")
	return true, nil
}

// Stop stops the poller.
func (p *Poller) Stop() {
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, tp := range p.triggers {
		tp.cancel()
	}
	p.triggers = make(map[int64]*triggerPoll)
	p.running = false
	log.Info().Msg("Google Drive watcher stopped")
}

// IsRunning returns whether the poller is currently running.
func (p *Poller) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// ReloadTrigger reloads a trigger's configuration.
// If the poller is not running and an enabled trigger is added, it will start the poller.
func (p *Poller) ReloadTrigger(triggerID int64) error {
	trigger, err := p.db.GetTrigger(triggerID)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if trigger == nil || trigger.Type != database.TriggerTypeGDrive {
		p.removeTriggerLocked(triggerID)
		return nil
	}
	if !trigger.Enabled {
		p.removeTriggerLocked(triggerID)
		return nil
	}

	if !p.running {
		p.running = true
	}
	return p.addTriggerLocked(trigger)
}

// ResetSnapshot clears snapshot state for a trigger and forces a fresh poll.
func (p *Poller) ResetSnapshot(triggerID int64) error {
	p.mu.Lock()
	p.removeTriggerLocked(triggerID)
	p.mu.Unlock()

	if err := p.db.DeleteGDriveSyncState(triggerID); err != nil {
		return err
	}
	if err := p.db.ReplaceGDriveSnapshotEntries(triggerID, nil); err != nil {
		return err
	}

	return p.ReloadTrigger(triggerID)
}

// RemoveTrigger removes a trigger and stops its polling loop.
func (p *Poller) RemoveTrigger(triggerID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeTriggerLocked(triggerID)
}

func (p *Poller) removeTriggerLocked(triggerID int64) {
	if tp, ok := p.triggers[triggerID]; ok {
		tp.cancel()
		delete(p.triggers, triggerID)
		log.Info().Int64("trigger_id", triggerID).Msg("Google Drive trigger removed")
	}

	if p.running && len(p.triggers) == 0 {
		p.running = false
	}
}

func (p *Poller) loadTriggersLocked() error {
	triggers, err := p.db.ListTriggersByType(database.TriggerTypeGDrive)
	if err != nil {
		return err
	}

	for _, trigger := range triggers {
		if trigger.Enabled {
			if err := p.addTriggerLocked(trigger); err != nil {
				log.Error().Err(err).Str("trigger", trigger.Name).Msg("Failed to add gdrive trigger")
			}
		}
	}
	return nil
}

func (p *Poller) addTriggerLocked(trigger *database.Trigger) error {
	if existing, ok := p.triggers[trigger.ID]; ok {
		existing.cancel()
		delete(p.triggers, trigger.ID)
	}

	ctx, cancel := context.WithCancel(p.ctx)
	tp := &triggerPoll{
		trigger:  trigger,
		ctx:      ctx,
		cancel:   cancel,
		driveID:  trigger.Config.GDriveDriveID,
		snapshot: make(map[string]*database.GDriveSnapshotEntry),
	}

	p.triggers[trigger.ID] = tp
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Str("trigger", trigger.Name).Msg("Google Drive poll loop panicked")
			}
		}()
		p.pollLoop(tp)
	}()

	log.Info().Str("trigger", trigger.Name).Msg("Google Drive trigger loaded")
	return nil
}

func (p *Poller) pollLoop(tp *triggerPoll) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	p.pollOnce(tp)

	for {
		select {
		case <-tp.ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(tp)
		}
	}
}

func (p *Poller) pollOnce(tp *triggerPoll) {
	trigger := tp.trigger
	if trigger == nil || !trigger.Enabled {
		return
	}
	if !trigger.Config.ScanEnabledValue() {
		return
	}
	log.Debug().
		Int64("trigger_id", trigger.ID).
		Str("trigger", trigger.Name).
		Int64("account_id", trigger.Config.GDriveAccountID).
		Str("drive_id", trigger.Config.GDriveDriveID).
		Msg("GDrive poll tick")
	if trigger.Config.GDriveAccountID == 0 {
		log.Warn().Int64("trigger_id", trigger.ID).Str("trigger", trigger.Name).Msg("GDrive trigger missing account")
		return
	}

	account, err := p.db.GetGDriveAccount(trigger.Config.GDriveAccountID)
	if err != nil || account == nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to load gdrive account")
		return
	}

	driveSvc, err := p.service.DriveServiceForAccount(tp.ctx, account)
	if err != nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to create gdrive client")
		return
	}

	driveName, err := p.ensureDriveName(tp.ctx, driveSvc, tp, trigger.Config.GDriveDriveID)
	if err != nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to resolve gdrive name")
		return
	}

	rootID, err := p.ensureDriveRootID(tp.ctx, driveSvc, tp, trigger.Config.GDriveDriveID)
	if err != nil {
		p.scheduleSnapshotRetry(trigger, "", err, "Failed to resolve gdrive root id")
		return
	}

	if log.Trace().Enabled() {
		log.Trace().
			Int64("trigger_id", trigger.ID).
			Str("trigger", trigger.Name).
			Str("drive_name", driveName).
			Str("drive_id", trigger.Config.GDriveDriveID).
			Msg("GDrive monitoring drive")
	}

	if err := p.ensureSnapshotLoaded(tp); err != nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to load gdrive snapshot")
		return
	}

	state, err := p.db.GetGDriveSyncState(trigger.ID)
	if err != nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to load gdrive sync state")
		return
	}

	if state != nil && state.Status == database.GDriveSyncStatusFailed {
		if state.NextRetryAt != nil && time.Now().Before(*state.NextRetryAt) {
			log.Debug().
				Int64("trigger_id", trigger.ID).
				Str("trigger", trigger.Name).
				Time("retry_at", *state.NextRetryAt).
				Msg("GDrive initial sync retry pending")
			return
		}
	}

	if state == nil || state.PageToken == "" || state.Status != database.GDriveSyncStatusReady {
		if err := p.refreshSnapshot(tp.ctx, driveSvc, tp, trigger, false); err != nil {
			p.scheduleSnapshotRetry(trigger, rootID, err, "Failed to initialize gdrive snapshot")
			return
		}
		log.Debug().
			Int64("trigger_id", trigger.ID).
			Str("trigger", trigger.Name).
			Msg("GDrive snapshot initialized; will poll on next tick")
		return
	}

	resolver := NewPathResolver(tp.ctx, driveSvc, trigger.Config.GDriveDriveID, driveName, rootID)

	nextToken, err := p.processChanges(tp.ctx, driveSvc, trigger, tp, resolver, state.PageToken, true)
	if err != nil {
		if isInvalidPageToken(err) {
			log.Warn().Err(err).Int64("trigger_id", trigger.ID).Msg("GDrive page token expired; rebuilding snapshot")
			if err := p.refreshSnapshot(tp.ctx, driveSvc, tp, trigger, true); err != nil {
				p.scheduleSnapshotRetry(trigger, rootID, err, "Failed to rebuild gdrive snapshot")
			}
			return
		}
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to poll gdrive changes")
		return
	}

	if nextToken == "" {
		return
	}

	if err := p.db.UpsertGDriveSyncState(&database.GDriveSyncState{
		TriggerID: trigger.ID,
		PageToken: nextToken,
		Status:    database.GDriveSyncStatusReady,
		LastError: "",
		RootID:    rootID,
	}); err != nil {
		log.Error().Err(err).Int64("trigger_id", trigger.ID).Msg("Failed to update gdrive page token")
	}
}

func (p *Poller) scheduleSnapshotRetry(trigger *database.Trigger, rootID string, err error, reason string) {
	if trigger == nil || err == nil {
		return
	}

	retryAt := time.Now().Add(gdriveInitialSyncRetryDelay)
	errMsg := err.Error()
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}

	if dbErr := p.db.UpsertGDriveSyncState(&database.GDriveSyncState{
		TriggerID:   trigger.ID,
		Status:      database.GDriveSyncStatusFailed,
		LastError:   errMsg,
		RootID:      strings.TrimSpace(rootID),
		NextRetryAt: &retryAt,
	}); dbErr != nil {
		log.Error().Err(dbErr).Int64("trigger_id", trigger.ID).Msg("Failed to record gdrive sync failure")
	}

	log.Error().
		Err(err).
		Int64("trigger_id", trigger.ID).
		Str("trigger", trigger.Name).
		Time("retry_at", retryAt).
		Msg(reason)
}

func (p *Poller) queuePaths(trigger *database.Trigger, paths map[string]struct{}, eventType string, filePathsByPath map[string][]string) {
	if len(paths) == 0 {
		return
	}

	rewrites := trigger.Config.PathRewrites
	if trigger.Type == database.TriggerTypeGDrive {
		rewrites = trigger.Config.GDrivePathRewriteRules()
	}

	type queuedPath struct {
		raw       string
		rewritten string
		filePaths []string
	}

	matchesFilters := func(rawPath, rewrittenPath string, rawFilePaths, rewrittenFilePaths []string) bool {
		if trigger.Type == database.TriggerTypeGDrive {
			matcher := trigger.Config.MatchesPathFiltersWithoutPrefixes
			if trigger.Config.FilterAfterRewrite {
				if len(rewrittenFilePaths) > 0 {
					for _, path := range rewrittenFilePaths {
						if matcher(path) {
							return true
						}
					}
					return false
				}
				return matcher(rewrittenPath)
			}
			if len(rawFilePaths) > 0 {
				for _, path := range rawFilePaths {
					if matcher(path) {
						return true
					}
				}
				return false
			}
			return matcher(rawPath)
		}
		if trigger.Config.FilterAfterRewrite {
			return trigger.Config.MatchesPathFilters(rewrittenPath)
		}
		return trigger.Config.MatchesPathFilters(rawPath)
	}

	appendUnique := func(existing, incoming []string) []string {
		if len(incoming) == 0 {
			return existing
		}
		if len(existing) == 0 {
			return append([]string(nil), incoming...)
		}
		seen := make(map[string]struct{}, len(existing))
		for _, path := range existing {
			seen[path] = struct{}{}
		}
		for _, path := range incoming {
			if _, ok := seen[path]; ok {
				continue
			}
			existing = append(existing, path)
			seen[path] = struct{}{}
		}
		return existing
	}

	unique := make([]string, 0, len(paths))
	for path := range paths {
		if path == "" {
			continue
		}
		unique = append(unique, filepath.Clean(path))
	}
	rawPaths := make([]string, len(unique))
	copy(rawPaths, unique)

	pairs := make([]queuedPath, 0, len(unique))
	for _, raw := range unique {
		if trigger.Type == database.TriggerTypeGDrive {
			if !gdrivePathMapped(raw, trigger.Config.GDrivePathRewrites) {
				continue
			}
		}

		rawFilePaths := []string(nil)
		if filePathsByPath != nil {
			rawFilePaths = filePathsByPath[raw]
		}
		if len(rawFilePaths) > 0 {
			cleanedFilePaths := make([]string, 0, len(rawFilePaths))
			for _, path := range rawFilePaths {
				cleaned := filepath.Clean(path)
				if cleaned == "" || cleaned == "." {
					continue
				}
				cleanedFilePaths = append(cleanedFilePaths, cleaned)
			}
			rawFilePaths = cleanedFilePaths
		}

		rewritten := triggerpaths.ApplyPathRewrites([]string{raw}, rewrites)[0]
		rewritten = filepath.Clean(rewritten)

		var rewrittenFilePaths []string
		if len(rawFilePaths) > 0 {
			rewrittenFilePaths = triggerpaths.ApplyPathRewrites(rawFilePaths, rewrites)
			for i := range rewrittenFilePaths {
				rewrittenFilePaths[i] = filepath.Clean(rewrittenFilePaths[i])
			}
		}

		if !matchesFilters(raw, rewritten, rawFilePaths, rewrittenFilePaths) {
			continue
		}
		pairs = append(pairs, queuedPath{raw: raw, rewritten: rewritten, filePaths: rewrittenFilePaths})
	}

	postFilterCount := len(pairs)
	rewrittenPaths := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		rewrittenPaths = append(rewrittenPaths, pair.rewritten)
	}
	rewrittenPaths = p.filterPathsByLibraries(trigger, rewrittenPaths)
	allowed := make(map[string]struct{}, len(rewrittenPaths))
	for _, path := range rewrittenPaths {
		allowed[path] = struct{}{}
	}
	filteredPairs := make([]queuedPath, 0, len(pairs))
	for _, pair := range pairs {
		if _, ok := allowed[pair.rewritten]; ok {
			filteredPairs = append(filteredPairs, pair)
		}
	}

	if log.Trace().Enabled() {
		rewritesLog := make([]map[string]any, 0, len(rewrites))
		for _, rewrite := range rewrites {
			rewritesLog = append(rewritesLog, map[string]any{
				"from":  rewrite.From,
				"to":    rewrite.To,
				"regex": rewrite.IsRegex,
			})
		}
		log.Trace().
			Int64("trigger_id", trigger.ID).
			Str("trigger", trigger.Name).
			Str("event", eventType).
			Int("raw_paths", len(rawPaths)).
			Int("post_paths", len(filteredPairs)).
			Int("library_filtered", postFilterCount-len(filteredPairs)).
			Bool("filter_after_rewrite", trigger.Config.FilterAfterRewrite).
			Int("rewrite_rules", len(rewrites)).
			Interface("rewrites", rewritesLog).
			Msg("GDrive path processing")

		for i, raw := range rawPaths {
			if i >= gdriveTraceLimit {
				break
			}
			rewritten := raw
			if len(rewrites) > 0 {
				rewritten = triggerpaths.ApplyPathRewrites([]string{raw}, rewrites)[0]
			}
			log.Trace().
				Int64("trigger_id", trigger.ID).
				Str("event", eventType).
				Str("path_raw", raw).
				Str("path_rewritten", rewritten).
				Msg("GDrive path rewrite result")
		}
		if len(rawPaths) > gdriveTraceLimit {
			log.Trace().
				Int64("trigger_id", trigger.ID).
				Int("skipped", len(rawPaths)-gdriveTraceLimit).
				Msg("GDrive path rewrite trace truncated")
		}
	}

	if len(filteredPairs) == 0 {
		return
	}

	uniqueRewritten := make(map[string]queuedPath, len(filteredPairs))
	for _, pair := range filteredPairs {
		if existing, exists := uniqueRewritten[pair.rewritten]; exists {
			existing.filePaths = appendUnique(existing.filePaths, pair.filePaths)
			uniqueRewritten[pair.rewritten] = existing
			continue
		}
		uniqueRewritten[pair.rewritten] = pair
	}

	for _, pair := range uniqueRewritten {
		p.proc.QueueScan(processor.ScanRequest{
			Path:        pair.rewritten,
			TriggerPath: pair.raw,
			TriggerID:   &trigger.ID,
			Priority:    trigger.Priority,
			EventType:   eventType,
			FilePaths:   pair.filePaths,
		})
	}
}

func gdrivePathMapped(raw string, mappings []database.GDrivePathRewrite) bool {
	if raw == "" || len(mappings) == 0 {
		return false
	}

	normalized := filepath.Clean(raw)
	sep := string(filepath.Separator)
	for _, mapping := range mappings {
		from := strings.TrimSpace(mapping.FromPath)
		if from == "" {
			continue
		}
		if !strings.HasPrefix(from, sep) {
			from = sep + from
		}
		from = filepath.Clean(from)
		if from == sep {
			return true
		}
		if normalized == from || strings.HasPrefix(normalized, from+sep) {
			return true
		}
	}

	return false
}

func (p *Poller) ensureSnapshotLoaded(tp *triggerPoll) error {
	if tp.loaded {
		return nil
	}

	entries, err := p.db.ListGDriveSnapshotEntries(tp.trigger.ID)
	if err != nil {
		return err
	}
	if tp.snapshot == nil {
		tp.snapshot = make(map[string]*database.GDriveSnapshotEntry)
	}
	for _, entry := range entries {
		if entry == nil || entry.FileID == "" {
			continue
		}
		tp.snapshot[entry.FileID] = entry
	}
	tp.loaded = true
	return nil
}

func (p *Poller) refreshSnapshot(ctx context.Context, svc *drive.Service, tp *triggerPoll, trigger *database.Trigger, queueDiff bool) error {
	if trigger != nil {
		log.Info().
			Int64("trigger_id", trigger.ID).
			Str("trigger", trigger.Name).
			Str("drive_id", trigger.Config.GDriveDriveID).
			Bool("queue_diff", queueDiff).
			Msg("GDrive full sync started")
	}

	startToken, err := p.getStartPageToken(ctx, svc, trigger.Config.GDriveDriveID)
	if err != nil {
		return err
	}

	prevSnapshot := tp.snapshot
	snapshot, entries, err := p.fetchSnapshotEntries(ctx, svc, trigger)
	if err != nil {
		return err
	}

	if err := p.db.ReplaceGDriveSnapshotEntries(trigger.ID, entries); err != nil {
		return err
	}
	tp.snapshot = snapshot
	tp.loaded = true

	// Apply any changes that happened during the snapshot without queueing.
	if startToken != "" {
		if _, err := p.processChanges(ctx, svc, trigger, tp, nil, startToken, false); err != nil {
			return err
		}
	}

	if queueDiff && len(prevSnapshot) > 0 {
		p.queueSnapshotDiff(trigger, prevSnapshot, tp.snapshot, tp.rootID)
	}

	nextToken, err := p.getStartPageToken(ctx, svc, trigger.Config.GDriveDriveID)
	if err != nil {
		return err
	}
	if nextToken == "" {
		nextToken = startToken
	}
	if nextToken == "" {
		return fmt.Errorf("missing start page token")
	}

	if err := p.db.UpsertGDriveSyncState(&database.GDriveSyncState{
		TriggerID: trigger.ID,
		PageToken: nextToken,
		Status:    database.GDriveSyncStatusReady,
		LastError: "",
		RootID:    tp.rootID,
	}); err != nil {
		return err
	}

	if trigger != nil {
		log.Info().
			Int64("trigger_id", trigger.ID).
			Str("trigger", trigger.Name).
			Str("drive_id", trigger.Config.GDriveDriveID).
			Int("entries", len(entries)).
			Bool("queue_diff", queueDiff).
			Msg("GDrive full sync completed")
		p.broadcastEvent(sse.EventGDriveSnapshotComplete, map[string]any{
			"trigger_id": trigger.ID,
			"trigger":    trigger.Name,
			"drive_id":   trigger.Config.GDriveDriveID,
			"entries":    len(entries),
		})
	}

	return nil
}

func (p *Poller) queueSnapshotDiff(trigger *database.Trigger, prevSnapshot, currentSnapshot map[string]*database.GDriveSnapshotEntry, rootID string) {
	if trigger == nil {
		return
	}

	changePaths := make(map[string]struct{})
	directChangePaths := make(map[string]struct{})
	changeFilePaths := make(map[string][]string)
	deleteFolderPaths := make(map[string]struct{})
	deleteFilePaths := make(map[string]struct{})
	deleteFilePathsByDir := make(map[string][]string)

	for fileID, prev := range prevSnapshot {
		if prev == nil {
			continue
		}
		if _, ok := currentSnapshot[fileID]; ok {
			continue
		}

		pathValue, ok := snapshotPath(fileID, prevSnapshot, trigger.Config.GDriveDriveID, rootID)
		if !ok || pathValue == "" {
			continue
		}

		if prev.MimeType == gdriveFolderMimeType {
			deleteFolderPaths[pathValue] = struct{}{}
		} else {
			dirPath := filepath.Clean(filepath.Dir(pathValue))
			deleteFilePaths[dirPath] = struct{}{}
			deleteFilePathsByDir[dirPath] = append(deleteFilePathsByDir[dirPath], pathValue)
		}
	}

	for fileID, current := range currentSnapshot {
		if current == nil {
			continue
		}
		prev := prevSnapshot[fileID]
		changedParent := prev == nil || prev.ParentID != current.ParentID
		changedName := prev == nil || prev.Name != current.Name
		isFolder := current.MimeType == gdriveFolderMimeType

		if isFolder {
			if prev == nil || (!changedParent && !changedName) {
				continue
			}
		} else if !changedParent && !changedName {
			continue
		}

		pathValue, ok := snapshotPath(fileID, currentSnapshot, trigger.Config.GDriveDriveID, rootID)
		if !ok || pathValue == "" {
			continue
		}

		if isFolder {
			changePaths[pathValue] = struct{}{}
			directChangePaths[pathValue] = struct{}{}
		} else {
			dirPath := filepath.Clean(filepath.Dir(pathValue))
			changePaths[dirPath] = struct{}{}
			directChangePaths[dirPath] = struct{}{}
			changeFilePaths[dirPath] = append(changeFilePaths[dirPath], pathValue)
		}
	}

	if len(changePaths) > 0 {
		changePaths = pruneGDriveChangePaths(changePaths, directChangePaths)
	}
	if len(deleteFolderPaths) > 0 {
		deleteFolderPaths = pruneGDriveDeleteFolderPaths(deleteFolderPaths)
	}
	if len(deleteFilePaths) > 0 {
		deleteFilePaths = pruneGDriveDeleteFilePaths(deleteFilePaths, deleteFolderPaths)
	}

	deletePaths := make(map[string]struct{}, len(deleteFolderPaths)+len(deleteFilePaths))
	for pathValue := range deleteFolderPaths {
		deletePaths[pathValue] = struct{}{}
	}
	for pathValue := range deleteFilePaths {
		deletePaths[pathValue] = struct{}{}
	}

	p.queuePaths(trigger, changePaths, "gdrive_change", changeFilePaths)
	p.queuePaths(trigger, deletePaths, "gdrive_delete", deleteFilePathsByDir)
}

func (p *Poller) fetchSnapshotEntries(ctx context.Context, svc *drive.Service, trigger *database.Trigger) (map[string]*database.GDriveSnapshotEntry, []*database.GDriveSnapshotEntry, error) {
	snapshot := make(map[string]*database.GDriveSnapshotEntry)
	entries := make([]*database.GDriveSnapshotEntry, 0, 1024)

	driveID := ""
	if trigger != nil {
		driveID = strings.TrimSpace(trigger.Config.GDriveDriveID)
	}

	pageToken := ""
	for {
		req := svc.Files.List().
			Q("trashed=false").
			PageSize(1000).
			Fields("nextPageToken,files(id,name,parents,mimeType,trashed)")
		if driveID != "" {
			req = req.DriveId(driveID).
				Corpora("drive").
				SupportsAllDrives(true).
				IncludeItemsFromAllDrives(true)
		} else {
			req = req.SupportsAllDrives(true).
				IncludeItemsFromAllDrives(false)
		}
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}

		resp, err := doGDriveRequest(ctx, "gdrive.files.list", func() (*drive.FileList, error) {
			return req.Context(ctx).Do()
		})
		if err != nil {
			return nil, nil, err
		}

		for _, file := range resp.Files {
			if file == nil || file.Id == "" {
				continue
			}
			name := strings.TrimSpace(file.Name)
			if name == "" {
				continue
			}
			parentID := ""
			if len(file.Parents) > 0 {
				parentID = strings.TrimSpace(file.Parents[0])
			}
			entry := &database.GDriveSnapshotEntry{
				TriggerID: trigger.ID,
				FileID:    file.Id,
				ParentID:  parentID,
				Name:      name,
				MimeType:  strings.TrimSpace(file.MimeType),
			}
			snapshot[file.Id] = entry
			entries = append(entries, entry)
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return snapshot, entries, nil
}

func (p *Poller) processChanges(ctx context.Context, svc *drive.Service, trigger *database.Trigger, tp *triggerPoll, resolver *PathResolver, pageToken string, queue bool) (string, error) {
	changePaths := make(map[string]struct{})
	directChangePaths := make(map[string]struct{})
	changeFilePaths := make(map[string][]string)
	deleteFolderPaths := make(map[string]struct{})
	deleteFilePaths := make(map[string]struct{})
	deleteFilePathsByDir := make(map[string][]string)

	var nextToken string
	for pageToken != "" {
		resp, err := p.listChanges(ctx, svc, trigger.Config.GDriveDriveID, pageToken)
		if err != nil {
			return "", err
		}
		log.Trace().
			Int64("trigger_id", trigger.ID).
			Str("drive_id", trigger.Config.GDriveDriveID).
			Str("page_token", pageToken).
			Int("changes", len(resp.Changes)).
			Bool("has_next_page", resp.NextPageToken != "").
			Bool("has_new_start_token", resp.NewStartPageToken != "").
			Msg("GDrive changes fetched")

		for i, change := range resp.Changes {
			if log.Trace().Enabled() && i < gdriveTraceLimit {
				log.Trace().
					Int64("trigger_id", trigger.ID).
					Str("file_id", change.FileId).
					Bool("removed", change.Removed).
					Msg("GDrive change item")
			}
			p.applySnapshotChange(change, resolver, tp, changePaths, directChangePaths, deleteFolderPaths, deleteFilePaths, changeFilePaths, deleteFilePathsByDir, queue)
		}

		if resp.NextPageToken != "" {
			pageToken = resp.NextPageToken
			continue
		}

		if resp.NewStartPageToken != "" {
			nextToken = resp.NewStartPageToken
		} else {
			nextToken = pageToken
		}
		break
	}

	if !queue {
		return nextToken, nil
	}

	changePaths = pruneGDriveChangePaths(changePaths, directChangePaths)
	deleteFolderPaths = pruneGDriveDeleteFolderPaths(deleteFolderPaths)
	deleteFilePaths = pruneGDriveDeleteFilePaths(deleteFilePaths, deleteFolderPaths)

	deletePaths := make(map[string]struct{}, len(deleteFolderPaths)+len(deleteFilePaths))
	for path := range deleteFolderPaths {
		deletePaths[path] = struct{}{}
	}
	for path := range deleteFilePaths {
		deletePaths[path] = struct{}{}
	}

	p.queuePaths(trigger, changePaths, "gdrive_change", changeFilePaths)
	p.queuePaths(trigger, deletePaths, "gdrive_delete", deleteFilePathsByDir)

	return nextToken, nil
}

func (p *Poller) applySnapshotChange(change *drive.Change, resolver *PathResolver, tp *triggerPoll, changePaths, directChangePaths, deleteFolderPaths, deleteFilePaths map[string]struct{}, changeFilePaths, deleteFilePathsByDir map[string][]string, queue bool) {
	if change == nil || tp == nil || tp.trigger == nil {
		return
	}

	fileID := strings.TrimSpace(change.FileId)
	if fileID == "" && change.File != nil {
		fileID = strings.TrimSpace(change.File.Id)
	}
	if fileID == "" {
		return
	}

	prev := tp.snapshot[fileID]

	if change.File == nil {
		if change.Removed {
			p.handleSnapshotDelete(fileID, prev, tp, deleteFolderPaths, deleteFilePaths, deleteFilePathsByDir, queue)
		}
		return
	}

	file := change.File
	name := strings.TrimSpace(file.Name)
	nameKnown := name != ""
	parentID := ""
	parentKnown := false
	if len(file.Parents) > 0 {
		parentID = strings.TrimSpace(file.Parents[0])
		parentKnown = parentID != ""
	}
	if !parentKnown && prev != nil {
		parentID = prev.ParentID
	}
	if !nameKnown && prev != nil {
		name = prev.Name
	}
	if name == "" {
		return
	}
	mimeType := strings.TrimSpace(file.MimeType)

	if change.Removed || file.Trashed {
		p.handleSnapshotDelete(fileID, prev, tp, deleteFolderPaths, deleteFilePaths, deleteFilePathsByDir, queue)
		return
	}

	changedParent := prev == nil || (parentKnown && prev.ParentID != parentID)
	changedName := prev == nil || (nameKnown && prev.Name != name)
	isFolder := mimeType == gdriveFolderMimeType

	entry := &database.GDriveSnapshotEntry{
		TriggerID: tp.trigger.ID,
		FileID:    fileID,
		ParentID:  parentID,
		Name:      name,
		MimeType:  mimeType,
	}
	tp.snapshot[fileID] = entry
	if err := p.db.UpsertGDriveSnapshotEntry(entry); err != nil {
		log.Error().Err(err).Int64("trigger_id", tp.trigger.ID).Str("file_id", fileID).Msg("Failed to update gdrive snapshot entry")
	}

	if !queue {
		return
	}

	shouldQueue := false
	if isFolder {
		shouldQueue = prev != nil && (changedParent || changedName)
	} else {
		shouldQueue = prev == nil || changedParent || changedName
	}

	if !shouldQueue || resolver == nil {
		return
	}

	if file != nil {
		resolver.SeedFile(file)
	}

	pathValue, ok := resolveChangePath(fileID, resolver, tp.snapshot, tp.trigger.Config.GDriveDriveID, tp.rootID)
	if !ok {
		log.Debug().Str("file_id", fileID).Msg("Failed to resolve gdrive path")
		return
	}

	if isFolder {
		changePaths[pathValue] = struct{}{}
		directChangePaths[pathValue] = struct{}{}
		return
	}

	dirPath := filepath.Clean(filepath.Dir(pathValue))
	changePaths[dirPath] = struct{}{}
	directChangePaths[dirPath] = struct{}{}
	if changeFilePaths != nil {
		changeFilePaths[dirPath] = append(changeFilePaths[dirPath], pathValue)
	}
}

func (p *Poller) handleSnapshotDelete(fileID string, prev *database.GDriveSnapshotEntry, tp *triggerPoll, deleteFolderPaths, deleteFilePaths map[string]struct{}, deleteFilePathsByDir map[string][]string, queue bool) {
	if prev != nil {
		if queue {
			pathValue, ok := snapshotPath(fileID, tp.snapshot, tp.trigger.Config.GDriveDriveID, tp.rootID)
			if ok && pathValue != "" {
				if prev.MimeType == gdriveFolderMimeType {
					deleteFolderPaths[pathValue] = struct{}{}
				} else {
					dirPath := filepath.Clean(filepath.Dir(pathValue))
					deleteFilePaths[dirPath] = struct{}{}
					if deleteFilePathsByDir != nil {
						deleteFilePathsByDir[dirPath] = append(deleteFilePathsByDir[dirPath], pathValue)
					}
				}
			}
		}
		delete(tp.snapshot, fileID)
		if err := p.db.DeleteGDriveSnapshotEntry(tp.trigger.ID, fileID); err != nil {
			log.Error().Err(err).Int64("trigger_id", tp.trigger.ID).Str("file_id", fileID).Msg("Failed to delete gdrive snapshot entry")
		}
	}
}

func resolveChangePath(fileID string, resolver *PathResolver, snapshot map[string]*database.GDriveSnapshotEntry, driveID, rootID string) (string, bool) {
	if snapshot != nil {
		if _, ok := snapshot[fileID]; ok {
			pathValue, ok := snapshotPath(fileID, snapshot, driveID, rootID)
			if ok && pathValue != "" {
				return pathValue, true
			}
		}
	}

	if resolver != nil {
		pathValue, _, err := resolver.ResolvePath(fileID)
		if err == nil && pathValue != "" {
			return pathValue, true
		}
	}

	return snapshotPath(fileID, snapshot, driveID, rootID)
}

func snapshotPath(fileID string, snapshot map[string]*database.GDriveSnapshotEntry, driveID, rootID string) (string, bool) {
	if fileID == "" {
		return "", false
	}

	rootID = strings.TrimSpace(rootID)
	visited := make(map[string]struct{})
	parts := make([]string, 0, 8)
	currentID := fileID
	for {
		if currentID == "" || currentID == "root" || (driveID != "" && currentID == driveID) || (rootID != "" && currentID == rootID) {
			break
		}
		if _, seen := visited[currentID]; seen {
			return "", false
		}
		visited[currentID] = struct{}{}

		entry := snapshot[currentID]
		if entry == nil {
			return "", false
		}
		name := strings.TrimSpace(entry.Name)
		if name != "" {
			parts = append(parts, name)
		}

		parentID := strings.TrimSpace(entry.ParentID)
		if parentID == "" || parentID == "root" || (driveID != "" && parentID == driveID) || (rootID != "" && parentID == rootID) {
			break
		}
		currentID = parentID
	}

	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	if len(parts) == 0 {
		return "/", true
	}
	return path.Join(append([]string{"/"}, parts...)...), true
}

func isInvalidPageToken(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusGone
	}
	return false
}

type gdriveLibraryTargets struct {
	target *database.Target
	paths  []string
}

func (p *Poller) filterPathsByLibraries(trigger *database.Trigger, paths []string) []string {
	if len(paths) == 0 {
		return paths
	}

	targetsList, err := p.db.ListEnabledTargets()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list targets for library lookahead")
		return paths
	}

	libraryTargets := make([]gdriveLibraryTargets, 0, len(targetsList))
	for _, target := range targetsList {
		if target.Type != database.TargetTypePlex &&
			target.Type != database.TargetTypeEmby &&
			target.Type != database.TargetTypeJellyfin {
			continue
		}
		if trigger != nil && trigger.Name != "" && target.Config.ShouldExcludeTrigger(trigger.Name) {
			continue
		}
		libraryPaths, err := p.db.GetCachedLibraryPathsAnyAge(target.ID)
		if err != nil {
			log.Warn().Err(err).Int64("target_id", target.ID).Str("target", target.Name).
				Msg("Failed to load cached library paths")
			continue
		}
		if len(libraryPaths) == 0 {
			continue
		}
		libraryTargets = append(libraryTargets, gdriveLibraryTargets{target: target, paths: libraryPaths})
	}

	if len(libraryTargets) == 0 {
		return paths
	}

	filtered := make([]string, 0, len(paths))
	for _, path := range paths {
		if matchesLibraryRoots(path, libraryTargets) {
			filtered = append(filtered, path)
			continue
		}
		if log.Trace().Enabled() {
			logger := log.Trace().Str("path", path)
			if trigger != nil {
				logger = logger.Int64("trigger_id", trigger.ID).Str("trigger", trigger.Name)
			}
			logger.Msg("GDrive path skipped (not under any library root)")
		}
	}

	return filtered
}

func matchesLibraryRoots(path string, targetsList []gdriveLibraryTargets) bool {
	normalized := filepath.Clean(path)
	for _, entry := range targetsList {
		mapped := targets.ApplyPathMappings(normalized, entry.target.Config.PathMappings)
		mapped = filepath.Clean(mapped)
		for _, libPath := range entry.paths {
			if pathWithinLibrary(mapped, libPath) {
				return true
			}
		}
	}
	return false
}

func pathWithinLibrary(path, libraryPath string) bool {
	normalizedPath := filepath.Clean(path)
	normalizedLibrary := filepath.Clean(libraryPath)
	if normalizedPath == normalizedLibrary {
		return true
	}
	if normalizedLibrary == string(filepath.Separator) {
		return true
	}
	return strings.HasPrefix(normalizedPath, normalizedLibrary+string(filepath.Separator))
}

func (p *Poller) getStartPageToken(ctx context.Context, svc *drive.Service, driveID string) (string, error) {
	req := svc.Changes.GetStartPageToken()
	if driveID != "" {
		req = req.DriveId(driveID).SupportsAllDrives(true)
	}
	resp, err := doGDriveRequest(ctx, "gdrive.changes.start_token", func() (*drive.StartPageToken, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return "", err
	}
	if resp.StartPageToken == "" {
		return "", fmt.Errorf("missing start page token")
	}
	return resp.StartPageToken, nil
}

func (p *Poller) listChanges(ctx context.Context, svc *drive.Service, driveID, pageToken string) (*drive.ChangeList, error) {
	driveID = strings.TrimSpace(driveID)
	req := svc.Changes.List(pageToken).
		SupportsAllDrives(true).
		IncludeRemoved(true).
		Fields("nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,parents,mimeType,trashed))")
	if driveID != "" {
		req = req.DriveId(driveID).
			IncludeItemsFromAllDrives(true)
	} else {
		req = req.IncludeItemsFromAllDrives(false)
	}
	resp, err := doGDriveRequest(ctx, "gdrive.changes.list", func() (*drive.ChangeList, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return nil, err
	}
	if log.Trace().Enabled() {
		log.Trace().
			Str("drive_id", driveID).
			Str("page_token", pageToken).
			Interface("response", resp).
			Msg("GDrive changes list response")
	}
	return resp, nil
}

func (p *Poller) ensureDriveName(ctx context.Context, svc *drive.Service, tp *triggerPoll, driveID string) (string, error) {
	if driveID == "" {
		tp.driveName = "My Drive"
		return tp.driveName, nil
	}

	if tp.driveName != "" && tp.driveID == driveID {
		return tp.driveName, nil
	}

	req := svc.Drives.Get(driveID)
	resp, err := doGDriveRequest(ctx, "gdrive.drives.get", func() (*drive.Drive, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return "", err
	}
	if log.Trace().Enabled() {
		log.Trace().
			Str("drive_id", driveID).
			Interface("response", resp).
			Msg("GDrive drive response")
	}
	tp.driveName = resp.Name
	tp.driveID = driveID
	return tp.driveName, nil
}

func (p *Poller) ensureDriveRootID(ctx context.Context, svc *drive.Service, tp *triggerPoll, driveID string) (string, error) {
	driveID = strings.TrimSpace(driveID)
	if driveID != "" {
		tp.rootID = driveID
		return tp.rootID, nil
	}
	if tp.rootID != "" {
		return tp.rootID, nil
	}

	req := svc.Files.Get("root").SupportsAllDrives(true).Fields("id")
	root, err := doGDriveRequest(ctx, "gdrive.files.get_root", func() (*drive.File, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return "", err
	}
	rootID := strings.TrimSpace(root.Id)
	if rootID == "" {
		return "", fmt.Errorf("missing gdrive root id")
	}
	tp.rootID = rootID
	return rootID, nil
}

func pruneGDriveChangePaths(paths, directPaths map[string]struct{}) map[string]struct{} {
	if len(paths) < 2 {
		return paths
	}

	cleanedDirect := make(map[string]struct{}, len(directPaths))
	for p := range directPaths {
		if p == "" {
			continue
		}
		cleanedDirect[filepath.Clean(p)] = struct{}{}
	}

	cleaned := make([]string, 0, len(paths))
	for p := range paths {
		if p == "" {
			continue
		}
		cleaned = append(cleaned, filepath.Clean(p))
	}

	sort.Slice(cleaned, func(i, j int) bool {
		return len(cleaned[i]) > len(cleaned[j])
	})

	kept := make(map[string]struct{}, len(cleaned))
	sep := string(filepath.Separator)
	for _, path := range cleaned {
		if _, ok := kept[path]; ok {
			continue
		}
		if _, ok := cleanedDirect[path]; ok {
			kept[path] = struct{}{}
			continue
		}

		prefix := path + sep
		skip := false
		for existing := range kept {
			if strings.HasPrefix(existing, prefix) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		kept[path] = struct{}{}
	}

	return kept
}

func pruneGDriveDeleteFolderPaths(paths map[string]struct{}) map[string]struct{} {
	if len(paths) < 2 {
		return paths
	}

	cleaned := make([]string, 0, len(paths))
	for p := range paths {
		if p == "" {
			continue
		}
		cleaned = append(cleaned, filepath.Clean(p))
	}

	sort.Slice(cleaned, func(i, j int) bool {
		return len(cleaned[i]) < len(cleaned[j])
	})

	kept := make(map[string]struct{}, len(cleaned))
	sep := string(filepath.Separator)
	for _, path := range cleaned {
		if _, ok := kept[path]; ok {
			continue
		}
		skip := false
		for existing := range kept {
			if path == existing || strings.HasPrefix(path, existing+sep) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		kept[path] = struct{}{}
	}

	return kept
}

func pruneGDriveDeleteFilePaths(paths, deleteFolderPaths map[string]struct{}) map[string]struct{} {
	if len(paths) == 0 {
		return paths
	}
	if len(deleteFolderPaths) == 0 {
		return paths
	}

	deletedFolders := make([]string, 0, len(deleteFolderPaths))
	for p := range deleteFolderPaths {
		if p == "" {
			continue
		}
		deletedFolders = append(deletedFolders, filepath.Clean(p))
	}
	if len(deletedFolders) == 0 {
		return paths
	}

	cleaned := make(map[string]struct{}, len(paths))
	for p := range paths {
		if p == "" {
			continue
		}
		cleaned[filepath.Clean(p)] = struct{}{}
	}

	filtered := make(map[string]struct{}, len(cleaned))
	sep := string(filepath.Separator)
	for path := range cleaned {
		skip := false
		for _, folder := range deletedFolders {
			if path == folder || strings.HasPrefix(path, folder+sep) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		filtered[path] = struct{}{}
	}

	return filtered
}

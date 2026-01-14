package handlers

import (
	"errors"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

const gdriveSnapshotPageSize = 200

var (
	errGDriveSnapshotTriggers = errors.New("gdrive snapshot triggers")
	errGDriveSnapshotEntries  = errors.New("gdrive snapshot entries")
)

type gdriveSnapshotItem struct {
	FileID    string
	ParentID  string
	Name      string
	MimeType  string
	Path      string
	Resolved  bool
	UpdatedAt time.Time
}

func (h *Handlers) GDriveSnapshotPage(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildGDriveSnapshotData(r)
	if err != nil {
		switch err {
		case errGDriveSnapshotTriggers:
			h.flashErr(w, "Failed to load Google Drive triggers")
			h.redirect(w, r, "/")
		case errGDriveSnapshotEntries:
			h.flashErr(w, "Failed to load Google Drive snapshot entries")
			h.redirect(w, r, "/gdrive-snapshot")
		default:
			h.flashErr(w, "Failed to load Google Drive snapshot")
			h.redirect(w, r, "/gdrive-snapshot")
		}
		return
	}

	h.render(w, r, "gdrive_snapshot.html", data)
}

// GDriveSnapshotContentPartial renders the snapshot section for SSE refresh.
func (h *Handlers) GDriveSnapshotContentPartial(w http.ResponseWriter, r *http.Request) {
	data, err := h.buildGDriveSnapshotData(r)
	if err != nil {
		http.Error(w, "Failed to load Google Drive snapshot", http.StatusInternalServerError)
		return
	}

	h.renderPartial(w, "gdrive_snapshot.html", "gdrive_snapshot_content", data)
}

func (h *Handlers) buildGDriveSnapshotData(r *http.Request) (map[string]any, error) {
	triggers, err := h.db.ListTriggersByType(database.TriggerTypeGDrive)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load gdrive triggers")
		return nil, errGDriveSnapshotTriggers
	}

	selectedTriggerID := int64(0)
	if raw := r.URL.Query().Get("trigger_id"); raw != "" {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil && id > 0 {
			selectedTriggerID = id
		}
	}

	var selectedTrigger *database.Trigger
	for _, trigger := range triggers {
		if trigger != nil && trigger.ID == selectedTriggerID {
			selectedTrigger = trigger
			break
		}
	}

	if selectedTrigger == nil && len(triggers) == 1 {
		selectedTrigger = triggers[0]
		selectedTriggerID = triggers[0].ID
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	page := 1
	if raw := r.URL.Query().Get("page"); raw != "" {
		if p, err := strconv.Atoi(raw); err == nil && p > 0 {
			page = p
		}
	}

	var items []gdriveSnapshotItem
	totalCount := 0
	totalFolderCount := 0
	totalFileCount := 0
	filteredCount := 0
	filteredFolderCount := 0
	filteredFileCount := 0
	totalPages := 0
	hasPrev := false
	hasNext := false
	prevPage := 0
	nextPage := 0
	var syncState *database.GDriveSyncState
	syncStatusLabel := ""
	syncStatusBadge := "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400"
	syncInProgress := false
	gdriveRunning := false
	if h.gdriveMgr != nil {
		gdriveRunning = h.gdriveMgr.IsRunning()
	}

	if selectedTrigger != nil {
		entries, err := h.db.ListGDriveSnapshotEntries(selectedTrigger.ID)
		if err != nil {
			log.Error().Err(err).Int64("trigger_id", selectedTrigger.ID).Msg("Failed to load gdrive snapshot entries")
			return nil, errGDriveSnapshotEntries
		}

		snapshot := make(map[string]*database.GDriveSnapshotEntry, len(entries))
		for _, entry := range entries {
			if entry == nil || entry.FileID == "" {
				continue
			}
			snapshot[entry.FileID] = entry
		}

		pathCache := buildSnapshotPathCache(snapshot, selectedTrigger.Config.GDriveDriveID)

		items = make([]gdriveSnapshotItem, 0, len(entries))
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			pathValue, ok := pathCache[entry.FileID]
			item := gdriveSnapshotItem{
				FileID:    entry.FileID,
				ParentID:  entry.ParentID,
				Name:      entry.Name,
				MimeType:  entry.MimeType,
				Path:      pathValue,
				Resolved:  ok,
				UpdatedAt: entry.UpdatedAt,
			}
			items = append(items, item)
			if item.MimeType == gdriveFolderMimeType {
				totalFolderCount++
			} else {
				totalFileCount++
			}
		}

		sort.Slice(items, func(i, j int) bool {
			left := strings.ToLower(items[i].Path)
			right := strings.ToLower(items[j].Path)
			if left == right {
				return items[i].FileID < items[j].FileID
			}
			return left < right
		})

		totalCount = len(items)
		if query != "" {
			filtered := items[:0]
			queryLower := strings.ToLower(query)
			for _, item := range items {
				if strings.Contains(strings.ToLower(item.Path), queryLower) ||
					strings.Contains(strings.ToLower(item.Name), queryLower) ||
					strings.Contains(strings.ToLower(item.FileID), queryLower) ||
					strings.Contains(strings.ToLower(item.ParentID), queryLower) ||
					strings.Contains(strings.ToLower(item.MimeType), queryLower) {
					filtered = append(filtered, item)
					if item.MimeType == gdriveFolderMimeType {
						filteredFolderCount++
					} else {
						filteredFileCount++
					}
				}
			}
			items = filtered
		} else {
			filteredFolderCount = totalFolderCount
			filteredFileCount = totalFileCount
		}

		filteredCount = len(items)
		if filteredCount > 0 {
			totalPages = (filteredCount + gdriveSnapshotPageSize - 1) / gdriveSnapshotPageSize
			if page > totalPages {
				page = totalPages
			}
			start := (page - 1) * gdriveSnapshotPageSize
			end := start + gdriveSnapshotPageSize
			if end > filteredCount {
				end = filteredCount
			}
			items = items[start:end]
		}

		if totalPages == 0 {
			totalPages = 1
		}
		hasPrev = page > 1
		hasNext = page < totalPages
		prevPage = page - 1
		nextPage = page + 1

		syncState, _ = h.db.GetGDriveSyncState(selectedTrigger.ID)
		if syncState == nil || syncState.PageToken == "" {
			if gdriveRunning {
				syncStatusLabel = "Initial sync in progress"
				syncStatusBadge = "bg-yellow-50 dark:bg-yellow-900/50 text-yellow-700 dark:text-yellow-400"
				syncInProgress = true
			} else {
				syncStatusLabel = "Initial sync pending"
			}
		} else {
			syncStatusLabel = "Synced"
			syncStatusBadge = "bg-green-50 dark:bg-green-900/50 text-green-700 dark:text-green-400"
		}
	}

	return map[string]any{
		"Triggers":            triggers,
		"SelectedTrigger":     selectedTrigger,
		"SelectedTriggerID":   selectedTriggerID,
		"SnapshotEntries":     items,
		"TotalCount":          totalCount,
		"TotalFolderCount":    totalFolderCount,
		"TotalFileCount":      totalFileCount,
		"FilteredCount":       filteredCount,
		"FilteredFolderCount": filteredFolderCount,
		"FilteredFileCount":   filteredFileCount,
		"Page":                page,
		"TotalPages":          totalPages,
		"HasPrev":             hasPrev,
		"HasNext":             hasNext,
		"PrevPage":            prevPage,
		"NextPage":            nextPage,
		"Query":               query,
		"SyncState":           syncState,
		"SyncStatusLabel":     syncStatusLabel,
		"SyncStatusBadge":     syncStatusBadge,
		"SyncInProgress":      syncInProgress,
		"GDriveRunning":       gdriveRunning,
	}, nil
}

func (h *Handlers) GDriveSnapshotReset(w http.ResponseWriter, r *http.Request) {
	rawID := chi.URLParam(r, "id")
	triggerID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || triggerID <= 0 {
		h.flashErr(w, "Invalid trigger ID")
		h.redirect(w, r, "/gdrive-snapshot")
		return
	}

	trigger, err := h.db.GetTrigger(triggerID)
	if err != nil {
		log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to load trigger")
		h.flashErr(w, "Failed to load trigger")
		h.redirect(w, r, "/gdrive-snapshot")
		return
	}
	if trigger == nil || trigger.Type != database.TriggerTypeGDrive {
		h.flashErr(w, "Google Drive trigger not found")
		h.redirect(w, r, "/gdrive-snapshot")
		return
	}

	if h.gdriveMgr != nil {
		if err := h.gdriveMgr.ResetSnapshot(triggerID); err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to reset gdrive snapshot")
			h.flashErr(w, "Failed to reset Google Drive snapshot")
			h.redirect(w, r, "/gdrive-snapshot?trigger_id="+strconv.FormatInt(triggerID, 10))
			return
		}
	} else {
		if err := h.db.DeleteGDriveSyncState(triggerID); err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to clear gdrive sync state")
			h.flashErr(w, "Failed to clear Google Drive sync state")
			h.redirect(w, r, "/gdrive-snapshot?trigger_id="+strconv.FormatInt(triggerID, 10))
			return
		}
		if err := h.db.ReplaceGDriveSnapshotEntries(triggerID, nil); err != nil {
			log.Error().Err(err).Int64("trigger_id", triggerID).Msg("Failed to clear gdrive snapshot entries")
			h.flashErr(w, "Failed to clear Google Drive snapshot entries")
			h.redirect(w, r, "/gdrive-snapshot?trigger_id="+strconv.FormatInt(triggerID, 10))
			return
		}
	}

	h.flash(w, "Google Drive snapshot reset; refresh will rebuild shortly")
	h.redirect(w, r, "/gdrive-snapshot?trigger_id="+strconv.FormatInt(triggerID, 10))
}

func buildSnapshotPathCache(snapshot map[string]*database.GDriveSnapshotEntry, driveID string) map[string]string {
	cache := make(map[string]string, len(snapshot))

	var resolve func(fileID string, visited map[string]struct{}) (string, bool)
	resolve = func(fileID string, visited map[string]struct{}) (string, bool) {
		if fileID == "" {
			return "", false
		}
		if fileID == "root" || (driveID != "" && fileID == driveID) {
			return "/", true
		}
		if cached, ok := cache[fileID]; ok {
			return cached, true
		}
		if _, seen := visited[fileID]; seen {
			return "", false
		}
		visited[fileID] = struct{}{}

		entry := snapshot[fileID]
		if entry == nil {
			return "", false
		}
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			return "", false
		}

		parentID := strings.TrimSpace(entry.ParentID)
		if parentID == "" || parentID == "root" || (driveID != "" && parentID == driveID) {
			pathValue := path.Join("/", name)
			cache[fileID] = pathValue
			return pathValue, true
		}

		parentPath, ok := resolve(parentID, visited)
		if !ok {
			return "", false
		}
		pathValue := path.Join(parentPath, name)
		cache[fileID] = pathValue
		return pathValue, true
	}

	for fileID := range snapshot {
		_, _ = resolve(fileID, map[string]struct{}{})
	}

	return cache
}

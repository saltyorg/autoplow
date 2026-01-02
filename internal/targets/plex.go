package targets

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
)

// plexActivityTracker tracks all activities for an item during scan completion (used by waiting uploaders)
type plexActivityTracker struct {
	title          string            // media title - show name or movie name (required match)
	seasonPatterns []string          // season patterns like "Season 02", "S02" (any must match for TV)
	activeUUIDs    map[string]string // uuid -> activity type
	mu             sync.Mutex
	lastActivity   time.Time
	scanStarted    bool
}

// matchesItem checks if the Plex item name matches our title and season
// The title must match, plus at least one season pattern (if any are set)
func (t *plexActivityTracker) matchesItem(itemName string) bool {
	itemLower := strings.ToLower(itemName)

	// Title must be present
	if !strings.Contains(itemLower, strings.ToLower(t.title)) {
		return false
	}

	// If no season patterns, title match is enough (e.g., movie or show-level scan)
	if len(t.seasonPatterns) == 0 {
		return true
	}

	// At least one season pattern must match
	for _, pattern := range t.seasonPatterns {
		if strings.Contains(itemLower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

// plexItemActivity tracks activity state for an item globally (not tied to waiting uploaders)
type plexItemActivity struct {
	activeUUIDs  map[string]string // uuid -> activity type
	mu           sync.Mutex
	lastActivity time.Time
	scanStarted  bool
}

// plexAnalysisActivityTypes are the Plex activity types we track for scan completion
var plexAnalysisActivityTypes = map[string]bool{
	"library.update.section":        true,
	"library.update.item.metadata":  true,
	"media.generate.chapter.thumbs": true,
	"media.generate.voice.activity": true,
	"media.generate.credits":        true,
}

// PlexNotificationCallback is called when WebSocket notifications are received
type PlexNotificationCallback func(notification PlexWebSocketNotification)

// PlexWebSocketNotification is the parsed WebSocket notification (exported for callbacks)
type PlexWebSocketNotification = plexWebSocketNotification

// PlexTarget implements the Target interface for Plex Media Server
type PlexTarget struct {
	dbTarget    *database.Target
	client      *http.Client
	subscribers sync.Map // map[string]*plexActivityTracker - for waiting uploaders
	activeItems sync.Map // map[string]*plexItemActivity - global tracking for all items

	// Notification callbacks for external observers (e.g., Plex Auto Languages)
	notificationCallbacks []PlexNotificationCallback
	callbacksMu           sync.RWMutex
}

// NewPlexTarget creates a new Plex target
func NewPlexTarget(dbTarget *database.Target) *PlexTarget {
	return &PlexTarget{
		dbTarget: dbTarget,
		client: &http.Client{
			Timeout: config.GetTimeouts().HTTPClient,
		},
	}
}

// Name returns the scanner name
func (s *PlexTarget) Name() string {
	return s.dbTarget.Name
}

// Type returns the scanner type
func (s *PlexTarget) Type() database.TargetType {
	return database.TargetTypePlex
}

// SupportsSessionMonitoring returns true as Plex supports session monitoring
func (s *PlexTarget) SupportsSessionMonitoring() bool {
	return true
}

// RegisterNotificationCallback adds a callback for WebSocket notifications
// Callbacks are invoked for all notification types (playing, timeline, activity, status)
func (s *PlexTarget) RegisterNotificationCallback(cb PlexNotificationCallback) {
	s.callbacksMu.Lock()
	defer s.callbacksMu.Unlock()
	s.notificationCallbacks = append(s.notificationCallbacks, cb)
}

// RegisterNotificationCallbackAny adds a callback that receives notifications as any type.
// This is useful for external packages that can't import the notification type directly
// due to circular import restrictions.
func (s *PlexTarget) RegisterNotificationCallbackAny(cb func(notification any)) {
	s.RegisterNotificationCallback(func(notification PlexWebSocketNotification) {
		cb(notification)
	})
}

// dispatchNotification sends the notification to all registered callbacks
func (s *PlexTarget) dispatchNotification(notification plexWebSocketNotification) {
	s.callbacksMu.RLock()
	callbacks := s.notificationCallbacks
	s.callbacksMu.RUnlock()

	for _, cb := range callbacks {
		cb(notification)
	}
}

// Scan triggers a library scan for the given path
func (s *PlexTarget) Scan(ctx context.Context, path string) error {
	// Get libraries
	libraries, err := s.GetLibraries(ctx)
	if err != nil {
		return fmt.Errorf("failed to get libraries: %w", err)
	}

	// Find libraries that contain this path, sorted by depth (most specific first)
	matchingLibraries := s.getMatchingLibraries(path, libraries)

	if len(matchingLibraries) == 0 {
		return fmt.Errorf("no library found for path: %s", path)
	}

	// Track which items we've already processed to avoid duplicates
	processedItems := make(map[string]bool)
	scanned := false

	for _, lib := range matchingLibraries {
		log.Debug().
			Str("scanner", s.Name()).
			Str("library", lib.Name).
			Str("path", path).
			Msg("Scanning library for path")

		if err := s.scanLibrary(ctx, lib.ID, path); err != nil {
			log.Error().Err(err).
				Str("scanner", s.Name()).
				Str("library", lib.Name).
				Msg("Failed to scan library")
			continue
		}
		scanned = true

		// If metadata refresh or analysis is enabled, search for the specific item
		if s.dbTarget.Config.RefreshMetadata || s.dbTarget.Config.AnalyzeMedia {
			items, err := s.searchItems(ctx, path)
			if err != nil {
				log.Warn().Err(err).
					Str("scanner", s.Name()).
					Str("path", path).
					Msg("Failed to search for items")
				continue
			}

			if len(items) == 0 {
				log.Debug().
					Str("scanner", s.Name()).
					Str("path", path).
					Msg("No items found for path, scan completed without metadata operations")
				continue
			}

			for _, item := range items {
				// Skip if already processed
				if processedItems[item.Key] {
					log.Debug().
						Str("scanner", s.Name()).
						Str("item_key", item.Key).
						Msg("Item already processed, skipping")
					continue
				}
				processedItems[item.Key] = true

				// Refresh metadata if enabled
				if s.dbTarget.Config.RefreshMetadata {
					if err := s.refreshItem(ctx, item.Key); err != nil {
						log.Error().Err(err).
							Str("scanner", s.Name()).
							Str("item_key", item.Key).
							Msg("Failed to refresh item metadata")
					}
				}

				// Analyze media if enabled
				if s.dbTarget.Config.AnalyzeMedia {
					if err := s.analyzeItem(ctx, item.Key); err != nil {
						log.Error().Err(err).
							Str("scanner", s.Name()).
							Str("item_key", item.Key).
							Msg("Failed to analyze item")
					}
				}
			}
		}
	}

	if !scanned {
		return fmt.Errorf("failed to scan any library for path: %s", path)
	}

	return nil
}

// scanLibrary triggers a scan for a specific library section
func (s *PlexTarget) scanLibrary(ctx context.Context, sectionID string, path string) error {
	// Build the scan URL
	scanURL := fmt.Sprintf("%s/library/sections/%s/refresh", s.dbTarget.URL, sectionID)

	// Add path parameter for targeted scan
	params := url.Values{}
	params.Set("path", path)

	req, err := http.NewRequestWithContext(ctx, "GET", scanURL+"?"+params.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add Plex token
	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("section_id", sectionID).
		Str("path", path).
		Bytes("response", body).
		Msg("Plex scan response")

	log.Info().
		Str("scanner", s.Name()).
		Str("section_id", sectionID).
		Str("path", path).
		Msg("Plex scan triggered")

	return nil
}

// TestConnection verifies the connection to Plex
func (s *PlexTarget) TestConnection(ctx context.Context) error {
	testURL := s.dbTarget.URL + "/identity"

	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("plex: connection to %s failed: %w", testURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("plex: invalid token for %s", testURL)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plex: %s returned status %d", testURL, resp.StatusCode)
	}

	return nil
}

// GetLibraries returns available Plex libraries
func (s *PlexTarget) GetLibraries(ctx context.Context) ([]Library, error) {
	librariesURL := s.dbTarget.URL + "/library/sections"

	req, err := http.NewRequestWithContext(ctx, "GET", librariesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("plex: request to %s failed: %w", librariesURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex: %s returned status %d: %s", librariesURL, resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		RawJSON("payload", body).
		Msg("Fetched libraries")

	var librariesResp plexLibrariesResponse
	if err := json.Unmarshal(body, &librariesResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	libraries := make([]Library, 0, len(librariesResp.MediaContainer.Directory))
	for _, dir := range librariesResp.MediaContainer.Directory {
		lib := Library{
			ID:   dir.Key,
			Name: dir.Title,
			Type: dir.Type,
		}
		// Get all location paths
		if len(dir.Location) > 0 {
			lib.Path = dir.Location[0].Path
			lib.Paths = make([]string, len(dir.Location))
			for i, loc := range dir.Location {
				lib.Paths[i] = loc.Path
			}
		}
		libraries = append(libraries, lib)
	}

	return libraries, nil
}

// GetSessions returns active playback sessions from Plex
func (s *PlexTarget) GetSessions(ctx context.Context) ([]Session, error) {
	sessionsURL := s.dbTarget.URL + "/status/sessions"

	req, err := http.NewRequestWithContext(ctx, "GET", sessionsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Trace().
		Str("target", s.Name()).
		RawJSON("response", body).
		Msg("Fetched sessions via polling")

	var sessionsResp plexSessionsResponse
	if err := json.Unmarshal(body, &sessionsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	sessions := make([]Session, 0, len(sessionsResp.MediaContainer.Metadata))

	for _, item := range sessionsResp.MediaContainer.Metadata {
		session := Session{
			ID:        item.SessionKey,
			MediaType: item.Type,
		}

		// Format title: "Show Name - Episode Name" for episodes with grandparentTitle
		if item.Type == "episode" && item.GrandparentTitle != "" {
			session.MediaTitle = item.GrandparentTitle + " - " + item.Title
		} else {
			session.MediaTitle = item.Title
		}

		// Get user info
		if item.User.Title != "" {
			session.Username = item.User.Title
		}

		// Get player info
		if item.Player.Product != "" {
			session.Player = item.Player.Product
		}

		// Get media info (format: resolution + codec)
		if len(item.Media) > 0 {
			session.Format = formatVideoInfo(item.Media[0].VideoResolution, item.Media[0].VideoCodec)
		}

		// Check if transcoding
		if item.TranscodeSession.Key != "" {
			session.Transcoding = true
		}

		// Use Session.bandwidth for actual streaming bitrate (kbps -> bps)
		if item.Session.Bandwidth > 0 {
			session.Bitrate = int64(item.Session.Bandwidth) * 1000
		} else if len(item.Media) > 0 && item.Media[0].Bitrate > 0 {
			// Fallback to media bitrate if session bandwidth not available (kbps -> bps)
			session.Bitrate = int64(item.Media[0].Bitrate) * 1000
		}

		sessions = append(sessions, session)
	}

	return sessions, nil
}

// GetActivities fetches the current activities from Plex
func (s *PlexTarget) GetActivities(ctx context.Context) ([]plexAPIActivity, error) {
	activitiesURL := s.dbTarget.URL + "/activities"

	req, err := http.NewRequestWithContext(ctx, "GET", activitiesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Trace().
		Str("target", s.Name()).
		RawJSON("response", body).
		Msg("Fetched activities")

	var activitiesResp plexActivitiesResponse
	if err := json.Unmarshal(body, &activitiesResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return activitiesResp.MediaContainer.Activities, nil
}

// formatVideoInfo formats resolution and codec into a display string like "1080p (H.264)"
func formatVideoInfo(resolution, codec string) string {
	if resolution == "" {
		return ""
	}
	// Normalize resolution (add 'p' if not present and it's a number)
	res := resolution
	if res != "" && res[len(res)-1] >= '0' && res[len(res)-1] <= '9' {
		res = res + "p"
	}
	if codec == "" {
		return res
	}
	// Format codec nicely (uppercase common codecs)
	c := strings.ToUpper(codec)
	switch strings.ToLower(codec) {
	case "h264", "avc":
		c = "H.264"
	case "hevc", "h265":
		c = "HEVC"
	case "av1":
		c = "AV1"
	case "vp9":
		c = "VP9"
	case "mpeg4":
		c = "MPEG-4"
	}
	return fmt.Sprintf("%s (%s)", res, c)
}

// getMatchingLibraries returns libraries that match the given path, sorted by path depth (deepest first)
func (s *PlexTarget) getMatchingLibraries(path string, libraries []Library) []Library {
	type libraryMatch struct {
		library Library
		depth   int
	}

	var matches []libraryMatch
	normalizedPath := strings.TrimRight(path, "/")

	for _, lib := range libraries {
		// Check all paths in the library, not just the first one
		for _, libPath := range lib.Paths {
			normalizedLibPath := strings.TrimRight(libPath, "/")
			if strings.HasPrefix(normalizedPath, normalizedLibPath) {
				// Count path components for depth
				depth := strings.Count(normalizedLibPath, "/")
				matches = append(matches, libraryMatch{library: lib, depth: depth})
				break // Don't add same library twice
			}
		}
	}

	// Sort by depth descending (deepest/most specific match first)
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].depth > matches[j].depth
	})

	result := make([]Library, len(matches))
	for i, m := range matches {
		result[i] = m.library
	}
	return result
}

// getSearchTerm extracts a search term from a file path (e.g., show name or movie name)
func getSearchTerm(path string) string {
	// Get the directory containing the file
	dir := filepath.Dir(path)
	parts := strings.Split(dir, "/")

	// Find the best part to use as search term (skip Season directories)
	var chosenPart string
	for i := len(parts) - 1; i >= 0; i-- {
		part := parts[i]
		if part == "" {
			continue
		}
		// Skip season directories
		if strings.HasPrefix(strings.ToLower(part), "season") ||
			strings.HasPrefix(strings.ToLower(part), "specials") {
			continue
		}
		chosenPart = part
		break
	}

	if chosenPart == "" {
		return ""
	}

	// Remove brackets, parentheses and their contents for cleaner search
	// e.g., "The Matrix (1999) [1080p]" -> "The Matrix"
	result := chosenPart

	// Remove [bracketed] content
	for {
		start := strings.Index(result, "[")
		end := strings.Index(result, "]")
		if start == -1 || end == -1 || end < start {
			break
		}
		result = result[:start] + result[end+1:]
	}

	// Remove (parenthesized) content
	for {
		start := strings.Index(result, "(")
		end := strings.Index(result, ")")
		if start == -1 || end == -1 || end < start {
			break
		}
		result = result[:start] + result[end+1:]
	}

	// Remove {braced} content
	for {
		start := strings.Index(result, "{")
		end := strings.Index(result, "}")
		if start == -1 || end == -1 || end < start {
			break
		}
		result = result[:start] + result[end+1:]
	}

	// Clean up whitespace
	result = strings.TrimSpace(result)
	// Collapse multiple spaces
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}

	return result
}

// searchItems searches Plex for items matching the given path
func (s *PlexTarget) searchItems(ctx context.Context, path string) ([]plexMetadata, error) {
	searchTerm := getSearchTerm(path)
	if searchTerm == "" {
		return nil, fmt.Errorf("could not extract search term from path")
	}

	log.Debug().
		Str("scanner", s.Name()).
		Str("path", path).
		Str("search_term", searchTerm).
		Msg("Searching for item in Plex")

	// Try progressively shorter search terms until we find matches
	terms := strings.Fields(searchTerm)
	for len(terms) > 0 {
		currentTerm := strings.Join(terms, " ")

		searchURL := fmt.Sprintf("%s/library/search", s.dbTarget.URL)
		params := url.Values{}
		params.Set("query", currentTerm)
		params.Set("includeCollections", "1")
		params.Set("includeExternalMedia", "1")
		params.Set("limit", "100")

		req, err := http.NewRequestWithContext(ctx, "GET", searchURL+"?"+params.Encode(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create search request: %w", err)
		}

		req.Header.Set("X-Plex-Token", s.dbTarget.Token)
		req.Header.Set("Accept", "application/json")

		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("search request failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("plex search returned status %d: %s", resp.StatusCode, string(body))
		}

		var searchResp plexSearchResponse
		if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
			return nil, fmt.Errorf("failed to parse search response: %w", err)
		}

		// Filter results to find items matching our path
		var matches []plexMetadata
		for _, result := range searchResp.MediaContainer.SearchResults {
			if result.Metadata == nil {
				continue
			}

			meta := result.Metadata

			// For TV shows, we need to get episodes and check their paths
			if meta.Type == "show" {
				episodes, err := s.getEpisodes(ctx, meta.Key)
				if err != nil {
					log.Warn().Err(err).Str("show", meta.Title).Msg("Failed to get episodes")
					continue
				}
				for _, ep := range episodes {
					if s.mediaMatchesPath(ep.Media, path) {
						matches = append(matches, ep)
					}
				}
			} else {
				// For movies and other content
				if s.mediaMatchesPath(meta.Media, path) {
					matches = append(matches, *meta)
				}
			}
		}

		if len(matches) > 0 {
			log.Debug().
				Str("scanner", s.Name()).
				Int("count", len(matches)).
				Str("search_term", currentTerm).
				Msg("Found matching items")
			return matches, nil
		}

		// Remove last word and try again
		terms = terms[:len(terms)-1]
	}

	return nil, nil // No matches found
}

// getEpisodes fetches all episodes for a TV show
func (s *PlexTarget) getEpisodes(ctx context.Context, showKey string) ([]plexMetadata, error) {
	// The key format is like "/library/metadata/12345"
	// We need to call "/library/metadata/12345/allLeaves" to get all episodes
	episodesURL := fmt.Sprintf("%s%s/allLeaves", s.dbTarget.URL, showKey)

	req, err := http.NewRequestWithContext(ctx, "GET", episodesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create episodes request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("episodes request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex episodes returned status %d", resp.StatusCode)
	}

	var episodesResp plexEpisodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&episodesResp); err != nil {
		return nil, fmt.Errorf("failed to parse episodes response: %w", err)
	}

	return episodesResp.MediaContainer.Metadata, nil
}

// mediaMatchesPath checks if any media part matches the given path
func (s *PlexTarget) mediaMatchesPath(media []plexMedia, path string) bool {
	normalizedPath := strings.TrimRight(path, "/")
	for _, m := range media {
		for _, part := range m.Part {
			normalizedFile := strings.TrimRight(part.File, "/")
			// Check if the file matches exactly or if the path is a directory containing the file
			if normalizedFile == normalizedPath || strings.HasPrefix(normalizedFile, normalizedPath+"/") {
				return true
			}
		}
	}
	return false
}

// refreshItem triggers a metadata refresh for a specific item
func (s *PlexTarget) refreshItem(ctx context.Context, itemKey string) error {
	refreshURL := fmt.Sprintf("%s%s/refresh", s.dbTarget.URL, itemKey)

	req, err := http.NewRequestWithContext(ctx, "PUT", refreshURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create refresh request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("refresh request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("plex refresh returned status %d", resp.StatusCode)
	}

	log.Debug().
		Str("scanner", s.Name()).
		Str("item_key", itemKey).
		Msg("Refreshed item metadata")

	return nil
}

// analyzeItem triggers a media analysis for a specific item
func (s *PlexTarget) analyzeItem(ctx context.Context, itemKey string) error {
	analyzeURL := fmt.Sprintf("%s%s/analyze", s.dbTarget.URL, itemKey)

	req, err := http.NewRequestWithContext(ctx, "PUT", analyzeURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create analyze request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("analyze request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("plex analyze returned status %d", resp.StatusCode)
	}

	log.Debug().
		Str("scanner", s.Name()).
		Str("item_key", itemKey).
		Msg("Analyzed item media")

	return nil
}

// Plex JSON structures for libraries API
type plexLibrariesResponse struct {
	MediaContainer plexLibrariesContainer `json:"MediaContainer"`
}

type plexLibrariesContainer struct {
	Directory []plexDirectory `json:"Directory"`
}

type plexDirectory struct {
	Key      string         `json:"key"`
	Title    string         `json:"title"`
	Type     string         `json:"type"`
	Location []plexLocation `json:"Location"`
}

type plexLocation struct {
	Path string `json:"path"`
}

// Plex JSON structures for sessions API
type plexSessionsResponse struct {
	MediaContainer plexSessionsContainer `json:"MediaContainer"`
}

type plexSessionsContainer struct {
	Metadata []plexSessionItem `json:"Metadata"`
}

type plexSessionItem struct {
	SessionKey       string               `json:"sessionKey"`
	Title            string               `json:"title"`
	GrandparentTitle string               `json:"grandparentTitle"` // Show name for episodes
	Type             string               `json:"type"`
	User             plexSessionUser      `json:"User"`
	Player           plexSessionPlayer    `json:"Player"`
	Media            []plexSessionMedia   `json:"Media"`
	Session          plexSession          `json:"Session"`
	TranscodeSession plexSessionTranscode `json:"TranscodeSession"`
}

type plexSession struct {
	Bandwidth int    `json:"bandwidth"` // kbps
	Location  string `json:"location"`  // wan/lan
}

type plexSessionUser struct {
	Title string `json:"title"`
}

type plexSessionPlayer struct {
	Product string `json:"product"`
}

type plexSessionMedia struct {
	VideoResolution string `json:"videoResolution"`
	VideoCodec      string `json:"videoCodec"`
	Bitrate         int    `json:"bitrate"`
}

type plexSessionTranscode struct {
	Key     string `json:"key"`
	Bitrate int    `json:"bitrate"`
}

// Plex JSON structures for search API
type plexSearchResponse struct {
	MediaContainer plexSearchContainer `json:"MediaContainer"`
}

type plexSearchContainer struct {
	SearchResults []plexSearchResult `json:"SearchResult"`
}

type plexSearchResult struct {
	Metadata *plexMetadata `json:"Metadata,omitempty"`
}

type plexMetadata struct {
	Key   string      `json:"key"`
	Title string      `json:"title"`
	Type  string      `json:"type"`
	Media []plexMedia `json:"Media,omitempty"`
}

type plexMedia struct {
	Part []plexPart `json:"Part"`
}

type plexPart struct {
	Key  string `json:"key"`
	File string `json:"file"`
}

// plexEpisodesResponse for fetching episodes from a show
type plexEpisodesResponse struct {
	MediaContainer plexEpisodesContainer `json:"MediaContainer"`
}

type plexEpisodesContainer struct {
	Metadata []plexMetadata `json:"Metadata"`
}

// SupportsWebSocket returns true as Plex supports WebSocket notifications
func (s *PlexTarget) SupportsWebSocket() bool {
	return true
}

// WatchSessions starts a WebSocket connection to Plex for real-time session updates
// It calls the callback whenever session state changes
// The function blocks until the context is cancelled or an unrecoverable error occurs
func (s *PlexTarget) WatchSessions(ctx context.Context, callback func(sessions []Session)) error {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 5 * time.Minute
	)

	// Start idle checker goroutine to log scan completions
	go s.runIdleChecker(ctx)

	pingInterval := config.GetTimeouts().WebSocketPing
	backoff := initialBackoff

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := s.watchSessionsOnce(ctx, callback, pingInterval)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Warn().
				Err(err).
				Str("target", s.Name()).
				Dur("backoff", backoff).
				Msg("Plex WebSocket disconnected, reconnecting")

			// Wait before reconnecting with backoff
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			// Exponential backoff with cap
			backoff = min(backoff*2, maxBackoff)
		} else {
			// Reset backoff on successful connection that ended gracefully
			backoff = initialBackoff
		}
	}
}

// watchSessionsOnce establishes a single WebSocket connection and handles messages
// Note: Plex WebSocket doesn't handle standard WebSocket ping frames well, so we don't send them.
// Plex servers send regular notifications that keep the connection alive.
func (s *PlexTarget) watchSessionsOnce(ctx context.Context, callback func(sessions []Session), _ time.Duration) error {
	// Build WebSocket URL
	wsURL, err := s.buildWebSocketURL()
	if err != nil {
		return fmt.Errorf("failed to build WebSocket URL: %w", err)
	}

	log.Debug().
		Str("target", s.Name()).
		Str("url", wsURL).
		Msg("Connecting to Plex WebSocket")

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("WebSocket dial failed: %w", err)
	}
	defer conn.Close()

	log.Info().
		Str("target", s.Name()).
		Msg("Connected to Plex WebSocket")

	// Fetch initial sessions
	sessions, err := s.GetSessions(ctx)
	if err != nil {
		log.Warn().Err(err).Str("target", s.Name()).Msg("Failed to fetch initial sessions")
	} else {
		callback(sessions)
	}

	// Create a channel for read errors
	readErrCh := make(chan error, 1)

	// Start reading messages in a goroutine
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				readErrCh <- err
				return
			}

			// Parse the notification
			var notification plexWebSocketNotification
			if err := json.Unmarshal(message, &notification); err != nil {
				log.Debug().
					Err(err).
					Str("target", s.Name()).
					RawJSON("message", message).
					Msg("Failed to parse WebSocket message")
				continue
			}

			// Dispatch notification to all registered callbacks (e.g., Plex Auto Languages)
			s.dispatchNotification(notification)

			// Log raw message for activity notifications, parsed for others
			if notification.NotificationContainer.Type == "activity" {
				log.Trace().
					Str("target", s.Name()).
					RawJSON("payload", message).
					Msg("Received activity notification")

				// Handle activity notifications (global tracking + notify waiting subscribers)
				s.handleActivityNotifications(notification.NotificationContainer.ActivityNotification)
			} else {
				s.logNotification(notification)
			}

			// Check if this is a session-related notification
			if s.isSessionNotification(notification) {
				log.Debug().
					Str("target", s.Name()).
					Str("type", notification.NotificationContainer.Type).
					Msg("Fetching sessions")

				// Fetch current sessions and call callback
				sessions, err := s.GetSessions(ctx)
				if err != nil {
					log.Warn().
						Err(err).
						Str("target", s.Name()).
						Msg("Failed to fetch sessions after notification")
					continue
				}
				callback(sessions)
			}
		}
	}()

	// Wait for context cancellation or read error
	// Note: We don't send pings as Plex doesn't handle WebSocket ping frames properly
	select {
	case <-ctx.Done():
		// Send close message
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		return ctx.Err()
	case err := <-readErrCh:
		return err
	}
}

// buildWebSocketURL constructs the Plex WebSocket URL
func (s *PlexTarget) buildWebSocketURL() (string, error) {
	parsed, err := url.Parse(s.dbTarget.URL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	// Convert HTTP(S) to WS(S)
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	default:
		parsed.Scheme = "ws"
	}

	// Build WebSocket path
	parsed.Path = "/:/websockets/notifications"

	// Add token as query parameter
	q := parsed.Query()
	q.Set("X-Plex-Token", s.dbTarget.Token)
	parsed.RawQuery = q.Encode()

	return parsed.String(), nil
}

// isSessionNotification checks if the notification is related to playback sessions
func (s *PlexTarget) isSessionNotification(notification plexWebSocketNotification) bool {
	notificationType := notification.NotificationContainer.Type
	return notificationType == "playing" ||
		notificationType == "status"
}

// handleActivityNotifications processes activity notifications, updating global tracking and notifying waiting subscribers
func (s *PlexTarget) handleActivityNotifications(activities []plexActivityNotification) {
	for _, activity := range activities {
		actType := activity.Activity.Type
		if !plexAnalysisActivityTypes[actType] {
			continue
		}

		subtitle := activity.Activity.Subtitle
		uuid := activity.Activity.UUID

		// Log all analysis activities at trace level for visibility
		log.Trace().
			Str("target", s.Name()).
			Str("event", activity.Event).
			Str("activity", actType).
			Str("item", subtitle).
			Str("uuid", uuid).
			Msg("Plex analysis activity")

		if subtitle == "" {
			continue
		}

		// 1. Update global tracker (always runs for visibility/logging)
		s.updateGlobalTracker(subtitle, uuid, actType, activity.Event)

		// 2. Notify waiting subscribers (only affects waiting uploaders)
		s.notifyWaitingSubscribers(subtitle, uuid, actType, activity.Event)
	}
}

// updateGlobalTracker updates the global activity tracker for an item (independent of waiting uploaders)
func (s *PlexTarget) updateGlobalTracker(itemName, uuid, actType, event string) {
	// Get or create tracker for this item
	val, _ := s.activeItems.LoadOrStore(itemName, &plexItemActivity{
		activeUUIDs: make(map[string]string),
	})
	tracker := val.(*plexItemActivity)

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.lastActivity = time.Now()

	switch event {
	case "started":
		tracker.scanStarted = true
		tracker.activeUUIDs[uuid] = actType
		log.Debug().
			Str("target", s.Name()).
			Str("item", itemName).
			Str("activity", actType).
			Msg("Plex activity started")

	case "ended":
		delete(tracker.activeUUIDs, uuid)
		log.Debug().
			Str("target", s.Name()).
			Str("item", itemName).
			Str("activity", actType).
			Msg("Plex activity ended")
	}
}

// notifyWaitingSubscribers notifies any uploaders waiting for scan completion
func (s *PlexTarget) notifyWaitingSubscribers(itemName, uuid, actType, event string) {
	s.subscribers.Range(func(key, value any) bool {
		tracker := value.(*plexActivityTracker)
		// Check if any of our match patterns appear in the Plex item name
		if tracker.matchesItem(itemName) {
			tracker.mu.Lock()
			tracker.lastActivity = time.Now()

			switch event {
			case "started":
				tracker.scanStarted = true
				tracker.activeUUIDs[uuid] = actType
			case "ended":
				delete(tracker.activeUUIDs, uuid)
			}
			tracker.mu.Unlock()
		}
		return true
	})
}

// checkIdleItems checks for items that have gone idle and logs completion
func (s *PlexTarget) checkIdleItems(idleThreshold time.Duration) {
	s.activeItems.Range(func(key, value any) bool {
		itemName := key.(string)
		tracker := value.(*plexItemActivity)

		tracker.mu.Lock()
		idle := time.Since(tracker.lastActivity)
		started := tracker.scanStarted
		activeCount := len(tracker.activeUUIDs)
		tracker.mu.Unlock()

		// Item is complete: scan started, no active activities, idle > threshold
		if started && activeCount == 0 && idle > idleThreshold {
			log.Debug().
				Str("target", s.Name()).
				Str("item", itemName).
				Float64("idle_secs", idle.Seconds()).
				Msg("Plex scan complete (all activities finished)")
			s.activeItems.Delete(key)
		}

		return true
	})
}

// runIdleChecker runs a goroutine that periodically checks for idle items and logs completion
func (s *PlexTarget) runIdleChecker(ctx context.Context) {
	// Get idle threshold from config, default to 10 seconds
	idleThreshold := time.Duration(s.dbTarget.Config.ScanCompletionIdleSeconds) * time.Second
	if idleThreshold == 0 {
		idleThreshold = 10 * time.Second
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkIdleItems(idleThreshold)
		}
	}
}

// WaitForScanCompletion waits for a Plex library scan to complete by monitoring activity notifications.
// It tracks all analysis activities (scan, metadata, thumbnails, voice, credits) and waits until
// there's been no activity for the configured idle threshold (default 10 seconds).
// Returns nil if scan completed, or the context error if timeout/cancelled.
func (s *PlexTarget) WaitForScanCompletion(ctx context.Context, path string, timeout time.Duration) error {
	// Build match info from path
	// For path like "/mnt/local/Media/TV/TV/Absentia (2017) (tvdb-330500)/Season 02":
	// - showName: "Absentia"
	// - seasonPatterns: ["Season 02", "S02", "S2"]
	matchInfo := buildPlexMatchInfo(path)
	if matchInfo == nil {
		log.Debug().
			Str("target", s.Name()).
			Str("path", path).
			Msg("Cannot extract match info from path, skipping scan completion wait")
		return nil
	}

	// Get idle threshold from config, default to 10 seconds
	idleThreshold := time.Duration(s.dbTarget.Config.ScanCompletionIdleSeconds) * time.Second
	if idleThreshold == 0 {
		idleThreshold = 10 * time.Second
	}

	// Create tracker
	subID := fmt.Sprintf("%s-%d", matchInfo.title, time.Now().UnixNano())
	tracker := &plexActivityTracker{
		title:          matchInfo.title,
		seasonPatterns: matchInfo.seasonPatterns,
		activeUUIDs:    make(map[string]string),
		lastActivity:   time.Now(),
	}
	s.subscribers.Store(subID, tracker)
	defer s.subscribers.Delete(subID)

	log.Debug().
		Str("target", s.Name()).
		Str("title", matchInfo.title).
		Strs("season_patterns", matchInfo.seasonPatterns).
		Str("timeout", timeout.String()).
		Str("idle_threshold", idleThreshold.String()).
		Msg("Waiting for all Plex activities to complete")

	// Check every 2 seconds for idle completion
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			tracker.mu.Lock()
			idle := time.Since(tracker.lastActivity)
			started := tracker.scanStarted
			activeCount := len(tracker.activeUUIDs)
			tracker.mu.Unlock()

			// Complete when scan started, no active tasks, and idle threshold exceeded
			if started && activeCount == 0 && idle > idleThreshold {
				log.Debug().
					Str("target", s.Name()).
					Str("title", matchInfo.title).
					Dur("idle", idle).
					Msg("All Plex activities completed (idle)")
				return nil
			}
		case <-timeoutCh:
			tracker.mu.Lock()
			remaining := len(tracker.activeUUIDs)
			tracker.mu.Unlock()
			log.Warn().
				Str("target", s.Name()).
				Str("title", matchInfo.title).
				Int("remaining_activities", remaining).
				Msg("Plex activity wait timed out")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// plexMatchInfo holds parsed match information from a scan path
type plexMatchInfo struct {
	title          string   // media title (show name or movie name)
	seasonPatterns []string // season patterns for TV, empty for movies
}

// buildPlexMatchInfo extracts match info from a path for Plex activity matching
// Handles both TV shows and movies:
//   - TV: "/Media/TV/Absentia (2017) (tvdb-330500)/Season 02" -> title="Absentia", seasonPatterns=["Season 02", "S02"]
//   - Movie: "/Media/Movies/Inception (2010)" -> title="Inception", seasonPatterns=[]
func buildPlexMatchInfo(path string) *plexMatchInfo {
	folderName := filepath.Base(path)
	if folderName == "" || folderName == "." || folderName == "/" {
		return nil
	}

	// Check if this is a season folder (TV show structure)
	isSeasonFolder := regexp.MustCompile(`(?i)^Season\s*\d+$`).MatchString(folderName)

	if isSeasonFolder {
		// TV show: folderName is "Season XX", parent is show folder
		parentPath := filepath.Dir(path)
		showFolder := filepath.Base(parentPath)
		if showFolder == "" || showFolder == "." || showFolder == "/" {
			return nil
		}

		// Extract show name from "Absentia (2017) (tvdb-330500)" -> "Absentia"
		showName := regexp.MustCompile(`^([^(]+)`).FindString(showFolder)
		showName = strings.TrimSpace(showName)
		if showName == "" {
			return nil
		}

		info := &plexMatchInfo{
			title:          showName,
			seasonPatterns: []string{folderName},
		}

		// Add abbreviated season format: "Season 02" -> "S02", "S2"
		if seasonMatch := regexp.MustCompile(`(?i)^Season\s*(\d+)$`).FindStringSubmatch(folderName); len(seasonMatch) == 2 {
			seasonNum := seasonMatch[1]
			info.seasonPatterns = append(info.seasonPatterns, fmt.Sprintf("S%s", seasonNum))
			if trimmed := strings.TrimLeft(seasonNum, "0"); trimmed != "" && trimmed != seasonNum {
				info.seasonPatterns = append(info.seasonPatterns, fmt.Sprintf("S%s", trimmed))
			}
		}

		return info
	}

	// Movie or show folder: extract title from the folder name itself
	// "Inception (2010)" -> "Inception"
	title := regexp.MustCompile(`^([^(]+)`).FindString(folderName)
	title = strings.TrimSpace(title)
	if title == "" {
		return nil
	}

	return &plexMatchInfo{
		title:          title,
		seasonPatterns: nil, // No season patterns for movies
	}
}

// logNotification logs the notification with type-specific content
func (s *PlexTarget) logNotification(notification plexWebSocketNotification) {
	container := notification.NotificationContainer
	logEvent := log.Trace().
		Str("target", s.Name()).
		Str("type", container.Type)

	switch container.Type {
	case "playing":
		for _, n := range container.PlaySessionStateNotification {
			logEvent.
				Str("sessionKey", n.SessionKey).
				Str("clientId", n.ClientID).
				Str("key", n.Key).
				Str("state", n.State).
				Int64("viewOffset", n.ViewOffset)
		}
	case "activity":
		logEvent.Interface("activities", container.ActivityNotification)
	case "status":
		for _, n := range container.StatusNotification {
			logEvent.
				Str("title", n.Title).
				Str("notificationName", n.NotificationName)
		}
	default:
		logEvent.Interface("container", container)
	}

	logEvent.Msg("Received WebSocket notification")
}

// Plex WebSocket notification structures
type plexWebSocketNotification struct {
	NotificationContainer plexNotificationContainer `json:"NotificationContainer"`
}

type plexNotificationContainer struct {
	Type                         string                        `json:"type"`
	Size                         int                           `json:"size"`
	PlaySessionStateNotification []plexPlaySessionNotification `json:"PlaySessionStateNotification,omitempty"`
	ActivityNotification         []plexActivityNotification    `json:"ActivityNotification,omitempty"`
	StatusNotification           []plexStatusNotification      `json:"StatusNotification,omitempty"`
}

type plexPlaySessionNotification struct {
	SessionKey string `json:"sessionKey"`
	ClientID   string `json:"clientIdentifier"`
	Key        string `json:"key"`
	ViewOffset int64  `json:"viewOffset"`
	State      string `json:"state"` // playing, paused, stopped
}

type plexActivityNotification struct {
	Event    string       `json:"event"` // started, updated, ended
	UUID     string       `json:"uuid"`
	Activity plexActivity `json:"Activity"`
}

type plexActivity struct {
	UUID     string  `json:"uuid"`
	Type     string  `json:"type"`
	Title    string  `json:"title"`
	Subtitle string  `json:"subtitle"`
	Progress float64 `json:"progress"`
	Context  struct {
		Key              string `json:"key"`
		LibrarySectionID string `json:"librarySectionID"`
	} `json:"Context"`
}

type plexStatusNotification struct {
	Title            string `json:"title"`
	NotificationName string `json:"notificationName"`
}

// Plex Activities API structures
type plexActivitiesResponse struct {
	MediaContainer struct {
		Size       int               `json:"size"`
		Activities []plexAPIActivity `json:"Activity"`
	} `json:"MediaContainer"`
}

type plexAPIActivity struct {
	UUID        string  `json:"uuid"`
	Type        string  `json:"type"`
	Cancellable bool    `json:"cancellable"`
	UserID      int     `json:"userID"`
	Title       string  `json:"title"`
	Subtitle    string  `json:"subtitle"`
	Progress    float64 `json:"progress"`
	Context     struct {
		LibrarySectionID   string `json:"librarySectionID"`
		LibrarySectionPath string `json:"key"`
	} `json:"Context"`
}

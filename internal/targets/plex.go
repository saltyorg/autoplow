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
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/httpclient"
)

// plexActivityTracker tracks all activities for an item during scan completion (used by waiting uploaders)
type plexActivityTracker struct {
	id              string            // identifier for logging
	title           string            // media title - show name or movie name (required match)
	titleNormalized string            // normalized title for fuzzy matching
	year            string            // optional year for movie matching
	seasonPatterns  []string          // season patterns like "Season 02", "S02" (any must match for TV)
	activeUUIDs     map[string]string // uuid -> activity type
	mu              sync.Mutex
	lastActivity    time.Time
	scanStarted     bool
	idleStart       time.Time
	searchDone      bool
	allowTitleProbe bool
}

type plexTitleMatchDetail struct {
	trackerNormalized string
	itemNormalized    string
	yearMatch         bool
	strategy          string
}

type plexMatchDetails struct {
	trackerCandidates    []plexTitleCandidate
	itemCandidates       []plexTitleCandidate
	titleMatch           plexTitleMatchDetail
	seasonMatchedPattern string
}

// matchItemWithDetails checks if the Plex item name matches our title and season.
// The title must match, plus at least one season pattern (if any are set).
func (t *plexActivityTracker) matchItemWithDetails(itemName string) (bool, plexMatchDetails) {
	details := plexMatchDetails{}
	if strings.TrimSpace(itemName) == "" {
		return false, details
	}

	details.trackerCandidates = plexTrackerTitleCandidates(t.title, t.year)
	details.itemCandidates = plexActivityTitleCandidates(itemName, t.seasonPatterns)
	titleMatched, matchDetail := plexCandidatesMatch(details.trackerCandidates, details.itemCandidates)
	details.titleMatch = matchDetail
	if !titleMatched {
		return false, details
	}

	// If no season patterns, title match is enough (e.g., movie or show-level scan)
	if len(t.seasonPatterns) == 0 {
		return true, details
	}

	// At least one season pattern must match
	itemLower := strings.ToLower(itemName)
	for _, pattern := range t.seasonPatterns {
		if strings.Contains(itemLower, strings.ToLower(pattern)) {
			details.seasonMatchedPattern = pattern
			return true, details
		}
	}
	return false, details
}

func (t *plexActivityTracker) applyMatchInfo(info *plexMatchInfo) {
	if info == nil || info.title == "" {
		return
	}
	t.title = info.title
	t.titleNormalized = normalizePlexTitle(info.title)
	t.year = info.year
	if len(info.seasonPatterns) > 0 {
		t.seasonPatterns = info.seasonPatterns
	}
}

func (t *plexActivityTracker) markSearchDone(found bool) {
	t.searchDone = true
	t.idleStart = time.Now()
	if !found {
		t.allowTitleProbe = true
	}
}

func (t *plexActivityTracker) applySubtitleOverride(subtitle string) {
	if !t.allowTitleProbe {
		return
	}
	title := plexTitleFromSubtitle(subtitle, t.seasonPatterns)
	if title == "" {
		return
	}
	year := extractPlexYear(subtitle)
	t.title = title
	t.titleNormalized = normalizePlexTitle(title)
	if year != "" {
		t.year = year
	}
	t.allowTitleProbe = false
}

var plexYearPattern = regexp.MustCompile(`\((\d{4})\)`)

func extractPlexYear(name string) string {
	if matches := plexYearPattern.FindStringSubmatch(name); len(matches) == 2 {
		return matches[1]
	}
	return ""
}

func plexTitleWithYear(title string, year int) string {
	if title == "" || year <= 0 {
		return title
	}
	yearToken := fmt.Sprintf("(%d)", year)
	if strings.Contains(title, yearToken) {
		return title
	}
	return fmt.Sprintf("%s %s", title, yearToken)
}

func plexYearFromString(year string) int {
	if year == "" {
		return 0
	}
	value, err := strconv.Atoi(year)
	if err != nil {
		return 0
	}
	return value
}

func plexItemTitle(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	if idx := strings.IndexAny(name, "(["); idx > 0 {
		name = strings.TrimSpace(name[:idx])
	}
	return name
}

func normalizePlexTitle(title string) string {
	var builder strings.Builder
	builder.Grow(len(title))
	for _, r := range strings.ToLower(title) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

type plexTitleCandidate struct {
	title      string
	normalized string
	year       string
}

func plexTitleFromSubtitle(subtitle string, seasonPatterns []string) string {
	candidate := strings.TrimSpace(subtitle)
	if candidate == "" {
		return ""
	}
	for _, pattern := range seasonPatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(pattern) + `\b`)
		candidate = re.ReplaceAllString(candidate, "")
	}
	candidate = strings.TrimSpace(strings.Trim(candidate, "-:"))
	return plexItemTitle(candidate)
}

func plexTrackerTitleCandidates(title, year string) []plexTitleCandidate {
	if strings.TrimSpace(title) == "" {
		return nil
	}
	base := plexItemTitle(title)
	yearHint := year
	if yearHint == "" {
		yearHint = extractPlexYear(title)
	}

	titles := []string{title}
	if base != "" && base != title {
		titles = append(titles, base)
	}
	if yearHint != "" && base != "" {
		titles = append(titles, fmt.Sprintf("%s (%s)", base, yearHint))
	}
	return plexBuildTitleCandidates(titles, yearHint)
}

func plexActivityTitleCandidates(itemName string, seasonPatterns []string) []plexTitleCandidate {
	if strings.TrimSpace(itemName) == "" {
		return nil
	}
	yearHint := extractPlexYear(itemName)
	base := plexTitleFromSubtitle(itemName, seasonPatterns)

	titles := []string{itemName}
	if base != "" && base != itemName {
		titles = append(titles, base)
	}
	if yearHint != "" && base != "" {
		titles = append(titles, fmt.Sprintf("%s (%s)", base, yearHint))
	}
	return plexBuildTitleCandidates(titles, yearHint)
}

func plexBuildTitleCandidates(titles []string, yearHint string) []plexTitleCandidate {
	seen := make(map[string]struct{}, len(titles))
	out := make([]plexTitleCandidate, 0, len(titles))
	for _, title := range titles {
		title = strings.TrimSpace(title)
		if title == "" {
			continue
		}
		if _, ok := seen[title]; ok {
			continue
		}
		seen[title] = struct{}{}
		year := extractPlexYear(title)
		if year == "" {
			year = yearHint
		}
		out = append(out, plexTitleCandidate{
			title:      title,
			normalized: normalizePlexTitle(title),
			year:       year,
		})
	}
	return out
}

func plexCandidateTitles(candidates []plexTitleCandidate) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.title == "" {
			continue
		}
		out = append(out, candidate.title)
	}
	return out
}

func plexCandidatesMatch(trackerCandidates, itemCandidates []plexTitleCandidate) (bool, plexTitleMatchDetail) {
	detail := plexTitleMatchDetail{}
	for _, tracker := range trackerCandidates {
		if tracker.normalized == "" {
			continue
		}
		for _, item := range itemCandidates {
			if item.normalized == "" {
				continue
			}
			if tracker.year != "" && item.year != "" && tracker.year != item.year {
				continue
			}
			yearMatch := tracker.year != "" && item.year != "" && tracker.year == item.year
			matched, strategy := plexNormalizedTitleMatch(tracker.normalized, item.normalized, yearMatch)
			if matched {
				detail = plexTitleMatchDetail{
					trackerNormalized: tracker.normalized,
					itemNormalized:    item.normalized,
					yearMatch:         yearMatch,
					strategy:          strategy,
				}
				return true, detail
			}
		}
	}
	return false, detail
}

func plexNormalizedTitleMatch(targetNormalized, itemNormalized string, yearMatch bool) (bool, string) {
	if itemNormalized == "" || targetNormalized == "" {
		return false, ""
	}
	if itemNormalized == targetNormalized {
		return true, "exact"
	}

	short := itemNormalized
	long := targetNormalized
	if len(short) > len(long) {
		short, long = long, short
	}
	if short == "" {
		return false, ""
	}

	if len(short) < 4 {
		if yearMatch && strings.HasPrefix(long, short) {
			return true, "short_prefix_year"
		}
		return false, ""
	}
	if strings.HasPrefix(long, short) {
		return true, "prefix"
	}
	if len(short) >= 6 {
		if strings.Contains(long, short) {
			return true, "contains"
		}
		return false, ""
	}
	if yearMatch && strings.Contains(long, short) {
		return true, "year_contains"
	}
	return false, ""
}

const plexMatchOverrideTTL = 30 * time.Minute
const plexSearchDelay = 3 * time.Second

type plexMatchOverride struct {
	info  plexMatchInfo
	setAt time.Time
}

func (s *PlexTarget) setMatchOverride(path string, info *plexMatchInfo) {
	if path == "" || info == nil || info.title == "" {
		return
	}
	s.matchOverrides.Store(path, plexMatchOverride{
		info:  *info,
		setAt: time.Now(),
	})
}

func (s *PlexTarget) logMatchOverride(path string, info *plexMatchInfo, source string) {
	if path == "" || info == nil || info.title == "" {
		return
	}
	log.Debug().
		Str("target", s.Name()).
		Str("path", path).
		Str("title", info.title).
		Str("year", info.year).
		Strs("season_patterns", info.seasonPatterns).
		Str("source", source).
		Msg("Plex match override applied")
}

func (s *PlexTarget) getMatchOverride(path string) *plexMatchInfo {
	if path == "" {
		return nil
	}
	value, ok := s.matchOverrides.Load(path)
	if !ok {
		return nil
	}
	override := value.(plexMatchOverride)
	if time.Since(override.setAt) > plexMatchOverrideTTL {
		s.matchOverrides.Delete(path)
		return nil
	}
	info := override.info
	return &info
}

func (s *PlexTarget) updateMatchInfoAfterDelay(ctx context.Context, path string, tracker *plexActivityTracker) {
	if path == "" || tracker == nil {
		return
	}

	timer := time.NewTimer(plexSearchDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	items, err := s.searchItems(ctx, path)
	found := false
	if err != nil {
		log.Warn().
			Err(err).
			Str("target", s.Name()).
			Str("path", path).
			Msg("Plex search failed while preparing scan tracking")
	} else if len(items) > 0 {
		if info := plexMatchInfoFromMetadata(path, items); info != nil {
			tracker.mu.Lock()
			tracker.applyMatchInfo(info)
			tracker.allowTitleProbe = false
			tracker.mu.Unlock()
			s.setMatchOverride(path, info)
			s.logMatchOverride(path, info, "plex_search")
			found = true
		}
	}

	tracker.mu.Lock()
	tracker.markSearchDone(found)
	trackerID := tracker.id
	trackerTitle := tracker.title
	allowTitleProbe := tracker.allowTitleProbe
	tracker.mu.Unlock()

	log.Debug().
		Str("target", s.Name()).
		Str("tracker_id", trackerID).
		Str("title", trackerTitle).
		Bool("found", found).
		Bool("allow_title_probe", allowTitleProbe).
		Msg("Plex tracker search complete")
}

// plexAnalysisActivityTypes are the Plex activity types we track for scan completion.
// All activities starting with "library." or "media.generate." are tracked.
var plexAnalysisActivityTypes = map[string]bool{
	"library.update.section":        true,
	"library.update.item.metadata":  true,
	"media.generate.chapter.thumbs": true,
	"media.generate.voice.activity": true,
	"media.generate.credits":        true,
	"media.generate.bif":            true, // video preview thumbnails
	"media.generate.intros":         true, // intro detection
}

// PlexNotificationCallback is called when WebSocket notifications are received
type PlexNotificationCallback func(notification PlexWebSocketNotification)

// PlexWebSocketNotification is the parsed WebSocket notification (exported for callbacks)
type PlexWebSocketNotification = plexWebSocketNotification

// PlexTarget implements the Target interface for Plex Media Server
type PlexTarget struct {
	dbTarget       *database.Target
	client         *http.Client
	subscribers    sync.Map // map[string]*plexActivityTracker - for waiting uploaders
	matchOverrides sync.Map // map[string]plexMatchOverride - scan path -> match override

	// Notification callbacks for external observers (e.g., Plex Auto Languages)
	notificationCallbacks []PlexNotificationCallback
	callbacksMu           sync.RWMutex
}

// NewPlexTarget creates a new Plex target
func NewPlexTarget(dbTarget *database.Target) *PlexTarget {
	return &PlexTarget{
		dbTarget: dbTarget,
		client:   httpclient.NewTraceClient(fmt.Sprintf("%s:%s", dbTarget.Type, dbTarget.Name), config.GetTimeouts().HTTPClient),
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
	candidatePath := filepath.Clean(path)
	if candidatePath == "." || candidatePath == string(filepath.Separator) {
		return ""
	}
	if isMediaFile(candidatePath) {
		candidatePath = filepath.Dir(candidatePath)
	}
	parts := strings.Split(filepath.ToSlash(candidatePath), "/")

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

var plexMediaExtensions = map[string]struct{}{
	".avi":  {},
	".m2ts": {},
	".m4v":  {},
	".mkv":  {},
	".mov":  {},
	".mp4":  {},
	".mpeg": {},
	".mpg":  {},
	".mts":  {},
	".ts":   {},
	".webm": {},
	".wmv":  {},
}

func isMediaFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	_, ok := plexMediaExtensions[ext]
	return ok
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

func plexMatchInfoFromMetadata(path string, items []plexMetadata) *plexMatchInfo {
	base := buildPlexMatchInfo(path)
	baseYear := 0
	if base != nil && base.year != "" {
		baseYear = plexYearFromString(base.year)
	}
	for _, item := range items {
		if item.Title == "" {
			continue
		}
		switch strings.ToLower(item.Type) {
		case "movie":
			year := item.Year
			if year == 0 {
				year = baseYear
			}
			title := plexTitleWithYear(item.Title, year)
			info := &plexMatchInfo{
				title:          title,
				seasonPatterns: nil,
			}
			if base != nil && len(base.seasonPatterns) > 0 {
				info.seasonPatterns = base.seasonPatterns
			}
			if year > 0 {
				info.year = fmt.Sprintf("%d", year)
			}
			return info
		case "show":
			year := item.Year
			if year == 0 {
				year = baseYear
			}
			title := plexTitleWithYear(item.Title, year)
			info := &plexMatchInfo{
				title:          title,
				seasonPatterns: nil,
			}
			if year > 0 {
				info.year = fmt.Sprintf("%d", year)
			}
			return info
		case "season":
			title := item.ParentTitle
			if title == "" {
				title = item.GrandparentTitle
			}
			if title == "" {
				title = item.Title
			}
			if title == "" {
				continue
			}
			year := baseYear
			if year == 0 {
				year = item.Year
			}
			title = plexTitleWithYear(title, year)
			info := &plexMatchInfo{
				title:          title,
				seasonPatterns: nil,
			}
			if base != nil && len(base.seasonPatterns) > 0 {
				info.seasonPatterns = base.seasonPatterns
			} else if item.Index > 0 {
				info.seasonPatterns = seasonPatternsFromNumber(item.Index)
			}
			if year > 0 {
				info.year = fmt.Sprintf("%d", year)
			}
			return info
		case "episode":
			title := item.GrandparentTitle
			if title == "" {
				title = item.ParentTitle
			}
			if title == "" {
				continue
			}
			year := baseYear
			if year == 0 {
				year = item.Year
			}
			title = plexTitleWithYear(title, year)
			info := &plexMatchInfo{
				title:          title,
				seasonPatterns: nil,
			}
			if base != nil && len(base.seasonPatterns) > 0 {
				info.seasonPatterns = base.seasonPatterns
			} else if item.ParentIndex > 0 {
				info.seasonPatterns = seasonPatternsFromNumber(item.ParentIndex)
			}
			if year > 0 {
				info.year = fmt.Sprintf("%d", year)
			}
			return info
		}
	}
	return nil
}

func seasonPatternsFromNumber(seasonNum int) []string {
	if seasonNum <= 0 {
		return nil
	}
	patterns := []string{
		fmt.Sprintf("Season %02d", seasonNum),
		fmt.Sprintf("Season %d", seasonNum),
		fmt.Sprintf("S%02d", seasonNum),
		fmt.Sprintf("S%d", seasonNum),
	}
	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}
	return out
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
	Key              string      `json:"key"`
	Title            string      `json:"title"`
	Type             string      `json:"type"`
	Year             int         `json:"year"`
	ParentTitle      string      `json:"parentTitle"`
	ParentIndex      int         `json:"parentIndex"`
	GrandparentTitle string      `json:"grandparentTitle"`
	Index            int         `json:"index"`
	Media            []plexMedia `json:"Media,omitempty"`
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
				Str("backoff", backoff.String()).
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
				log.Trace().
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

		// Notify waiting subscribers (only affects waiting uploaders)
		s.notifyWaitingSubscribers(subtitle, uuid, actType, activity.Event)
	}
}

// notifyWaitingSubscribers notifies any uploaders waiting for scan completion
func (s *PlexTarget) notifyWaitingSubscribers(itemName, uuid, actType, event string) {
	s.subscribers.Range(func(key, value any) bool {
		tracker := value.(*plexActivityTracker)
		tracker.mu.Lock()
		if tracker.allowTitleProbe {
			tracker.applySubtitleOverride(itemName)
		}
		matched, details := tracker.matchItemWithDetails(itemName)
		if !matched {
			tracker.mu.Unlock()
			return true
		}

		tracker.lastActivity = time.Now()
		tracker.scanStarted = true

		uuidUpdated := false
		switch event {
		case "started":
			tracker.activeUUIDs[uuid] = actType
			uuidUpdated = true
		case "updated":
			if _, ok := tracker.activeUUIDs[uuid]; !ok {
				tracker.activeUUIDs[uuid] = actType
				uuidUpdated = true
			}
		case "ended":
			if _, ok := tracker.activeUUIDs[uuid]; ok {
				delete(tracker.activeUUIDs, uuid)
				uuidUpdated = true
			}
		}

		trackerID := tracker.id
		trackerTitle := tracker.title
		trackerYear := tracker.year
		seasonPatterns := append([]string(nil), tracker.seasonPatterns...)
		activeCount := len(tracker.activeUUIDs)
		tracker.mu.Unlock()

		log.Debug().
			Str("target", s.Name()).
			Str("tracker_id", trackerID).
			Str("event", event).
			Str("activity", actType).
			Str("item", itemName).
			Str("title", trackerTitle).
			Str("year", trackerYear).
			Strs("season_patterns", seasonPatterns).
			Int("active_count", activeCount).
			Bool("idle_reset", true).
			Bool("scan_started", true).
			Msg("Plex activity matched tracker")

		if uuidUpdated {
			log.Debug().
				Str("target", s.Name()).
				Str("tracker_id", trackerID).
				Str("uuid", uuid).
				Str("event", event).
				Int("active_count", activeCount).
				Msg("Plex activity UUID updated")
		}

		log.Trace().
			Str("target", s.Name()).
			Str("tracker_id", trackerID).
			Strs("tracker_candidates", plexCandidateTitles(details.trackerCandidates)).
			Strs("item_candidates", plexCandidateTitles(details.itemCandidates)).
			Msg("Plex title candidates built")

		log.Trace().
			Str("target", s.Name()).
			Str("tracker_id", trackerID).
			Str("tracker_norm", details.titleMatch.trackerNormalized).
			Str("item_norm", details.titleMatch.itemNormalized).
			Bool("year_match", details.titleMatch.yearMatch).
			Str("match_strategy", details.titleMatch.strategy).
			Msg("Plex title compare")

		if len(seasonPatterns) > 0 {
			log.Trace().
				Str("target", s.Name()).
				Str("tracker_id", trackerID).
				Str("item", itemName).
				Strs("patterns", seasonPatterns).
				Str("matched_pattern", details.seasonMatchedPattern).
				Msg("Plex season pattern check")
		}
		return true
	})
}

// WaitForScanCompletion waits for a Plex library scan to complete by monitoring activity notifications.
// It tracks all analysis activities (scan, metadata, thumbnails, voice, credits) and waits until
// there's been no activity for the configured idle threshold (default 30 seconds).
// Returns nil if scan completed, or the context error if timeout/cancelled.
func (s *PlexTarget) WaitForScanCompletion(ctx context.Context, path string, timeout time.Duration) error {
	// Default idle threshold (30s) for legacy interface.
	return s.WaitForScanCompletionWithIdle(ctx, path, 30*time.Second)
}

// WaitForScanCompletionWithIdle waits for a Plex library scan to complete using a caller-provided idle threshold.
func (s *PlexTarget) WaitForScanCompletionWithIdle(ctx context.Context, path string, idleThreshold time.Duration) error {
	// Build match info from Plex metadata or path
	// For path like "/mnt/local/Media/TV/TV/Absentia (2017) (tvdb-330500)/Season 02":
	// - showName: "Absentia"
	// - seasonPatterns: ["Season 02", "S02", "S2"]
	matchInfo := s.getMatchOverride(path)
	overrideUsed := matchInfo != nil
	if matchInfo == nil {
		matchInfo = buildPlexMatchInfo(path)
	}
	if matchInfo == nil {
		log.Debug().
			Str("target", s.Name()).
			Str("path", path).
			Msg("Cannot extract match info from path, skipping scan completion wait")
		return nil
	}

	// Default idle threshold to 30 seconds if unset or invalid.
	if idleThreshold <= 0 {
		idleThreshold = 30 * time.Second
	}

	// Create tracker
	subID := fmt.Sprintf("%s-%d", matchInfo.title, time.Now().UnixNano())
	tracker := &plexActivityTracker{
		id:              subID,
		title:           matchInfo.title,
		titleNormalized: normalizePlexTitle(matchInfo.title),
		year:            matchInfo.year,
		seasonPatterns:  matchInfo.seasonPatterns,
		activeUUIDs:     make(map[string]string),
		lastActivity:    time.Now(),
	}
	if overrideUsed {
		tracker.searchDone = true
		tracker.idleStart = time.Now()
	}
	s.subscribers.Store(subID, tracker)
	defer s.subscribers.Delete(subID)

	log.Debug().
		Str("target", s.Name()).
		Str("title", matchInfo.title).
		Strs("season_patterns", matchInfo.seasonPatterns).
		Str("idle_threshold", idleThreshold.String()).
		Msg("Waiting for all Plex activities to complete")

	if !overrideUsed {
		go s.updateMatchInfoAfterDelay(ctx, path, tracker)
	}

	// Check every 2 seconds for idle completion
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tracker.mu.Lock()
			searchDone := tracker.searchDone
			idleStart := tracker.idleStart
			lastActivity := tracker.lastActivity
			started := tracker.scanStarted
			activeCount := len(tracker.activeUUIDs)
			currentTitle := tracker.title
			trackerID := tracker.id
			tracker.mu.Unlock()

			if !searchDone {
				continue
			}
			if lastActivity.Before(idleStart) {
				lastActivity = idleStart
			}
			idle := time.Since(lastActivity)

			if started {
				log.Trace().
					Str("target", s.Name()).
					Str("tracker_id", trackerID).
					Str("title", currentTitle).
					Dur("idle", idle).
					Dur("idle_threshold", idleThreshold).
					Int("active_count", activeCount).
					Time("last_activity", lastActivity).
					Msg("Plex idle tick")
			}

			// Complete when no active tasks and idle threshold exceeded
			if activeCount == 0 && idle > idleThreshold {
				if started {
					log.Debug().
						Str("target", s.Name()).
						Str("title", currentTitle).
						Str("idle", idle.String()).
						Bool("tracked_activities", true).
						Msg("Plex scan complete (tracked activities, idle reached)")
				} else {
					log.Debug().
						Str("target", s.Name()).
						Str("title", currentTitle).
						Str("idle", idle.String()).
						Bool("tracked_activities", false).
						Msg("Plex scan complete (idle only, no activities tracked)")
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// plexMatchInfo holds parsed match information from a scan path
type plexMatchInfo struct {
	title          string   // media title (show name or movie name)
	seasonPatterns []string // season patterns for TV, empty for movies
	year           string   // optional year extracted from path
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
			year:           extractPlexYear(showFolder),
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
		year:           extractPlexYear(folderName),
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

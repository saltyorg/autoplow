package plexautolang

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

// PlexTargetInterface defines the methods needed from PlexTarget for Plex Auto Languages
type PlexTargetInterface interface {
	Name() string
	DBTarget() *database.Target
	RegisterActivityCallback(cb func(ActivityNotification))
	RegisterNotificationCallbackAny(cb func(notification any))
	GetEpisodeWithStreams(ctx context.Context, ratingKey string) (*Episode, error)
	GetEpisodeWithStreamsAsUser(ctx context.Context, ratingKey string, userToken string) (*Episode, error)
	GetSessionEpisodeWithStreams(ctx context.Context, clientIdentifier string, ratingKey string) (*Episode, error)
	GetShowEpisodes(ctx context.Context, showKey string) ([]Episode, error)
	GetSeasonEpisodes(ctx context.Context, seasonKey string) ([]Episode, error)
	SetStreams(ctx context.Context, partID int, audioStreamID, subtitleStreamID int) error
	SetStreamsAsUser(ctx context.Context, partID int, audioStreamID, subtitleStreamID int, userToken string) error
	GetSessionUserMapping(ctx context.Context) (map[string]PlexUser, error)
	GetSystemAccounts(ctx context.Context) ([]PlexUser, error)
	GetMachineIdentifier(ctx context.Context) (string, error)
	GetUserTokenWithMachineID(ctx context.Context, userID string, machineID string) (string, error)
}

// PlexWebSocketNotification is the type alias for the WebSocket notification
// This allows the manager to work with the notification without importing targets package
type PlexWebSocketNotification struct {
	NotificationContainer struct {
		Type                         string                    `json:"type"`
		PlaySessionStateNotification []PlaySessionNotification `json:"PlaySessionStateNotification,omitempty"`
		ActivityNotification         []ActivityNotification    `json:"ActivityNotification,omitempty"`
		TimelineEntry                []TimelineEntry           `json:"TimelineEntry,omitempty"`
	} `json:"NotificationContainer"`
}

// PlaySessionNotification represents a playing session notification from WebSocket
type PlaySessionNotification struct {
	SessionKey       string `json:"sessionKey"`
	ClientIdentifier string `json:"clientIdentifier"`
	Key              string `json:"key"` // e.g., "/library/metadata/12345"
	RatingKey        string `json:"ratingKey"`
	ViewOffset       int64  `json:"viewOffset"`
	State            string `json:"state"` // playing, paused, stopped
}

// ActivityNotification represents an activity notification from WebSocket
type ActivityNotification struct {
	Event    string `json:"event"`
	UUID     string `json:"uuid"`
	Activity struct {
		Type     string `json:"type"`
		Subtitle string `json:"subtitle"`
		Context  struct {
			Key string `json:"key"`
		} `json:"Context"`
	} `json:"Activity"`
}

// TargetGetter provides access to Plex targets
type TargetGetter interface {
	GetPlexTarget(targetID int64) (PlexTargetInterface, error)
}

// Manager manages Plex Auto Languages for all targets
type Manager struct {
	db           *database.Manager
	targetGetter TargetGetter
	sseBroker    *sse.Broker
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	running      bool

	// Per-target state
	caches map[int64]*Cache // targetID -> cache
	// Enabled targets (callbacks registered or explicitly enabled)
	enabledTargets map[int64]bool
	// Per-target work queues
	queues map[int64]*targetQueue
}

// NewManager creates a new Plex Auto Languages manager
func NewManager(db *database.Manager, targetGetter TargetGetter) *Manager {
	return &Manager{
		db:             db,
		targetGetter:   targetGetter,
		caches:         make(map[int64]*Cache),
		enabledTargets: make(map[int64]bool),
		queues:         make(map[int64]*targetQueue),
	}
}

type targetQueue struct {
	mu            sync.Mutex
	tasks         chan func()
	maxConcurrent int
	closed        bool
}

func newTargetQueue(maxConcurrent int) *targetQueue {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	q := &targetQueue{
		tasks:         make(chan func(), 100),
		maxConcurrent: maxConcurrent,
	}
	for i := 0; i < maxConcurrent; i++ {
		go func() {
			for task := range q.tasks {
				if task != nil {
					task()
				}
			}
		}()
	}
	return q
}

func (q *targetQueue) enqueue(task func()) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.tasks <- task
	return true
}

func (q *targetQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.tasks)
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

// Start starts the manager and registers notification callbacks for all enabled targets
func (m *Manager) Start() error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.running = true
	m.mu.Unlock()

	// Register callbacks for enabled targets (must be called without holding the lock)
	if err := m.registerTargetCallbacks(); err != nil {
		log.Warn().Err(err).Msg("Failed to register some target callbacks for Plex Auto Languages")
	}

	// Start cache cleanup goroutine
	go m.runCacheCleanup()

	log.Info().Msg("Plex Auto Languages manager started")
	return nil
}

// Stop stops the manager
func (m *Manager) Stop() error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	if m.cancel != nil {
		m.cancel()
	}

	m.running = false
	for id, queue := range m.queues {
		queue.close()
		delete(m.queues, id)
	}
	m.mu.Unlock()
	log.Info().Msg("Plex Auto Languages manager stopped")
	return nil
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// EnableTarget registers callbacks for a target and marks it enabled.
func (m *Manager) EnableTarget(targetID int64) {
	m.mu.RLock()
	if m.enabledTargets[targetID] {
		m.mu.RUnlock()
		return
	}
	running := m.running
	m.mu.RUnlock()

	if !running {
		return
	}

	if err := m.registerTargetCallback(targetID); err != nil {
		log.Warn().Err(err).Int64("target_id", targetID).Msg("Failed to register target callback")
		return
	}
}

// DisableTarget marks a target disabled and clears its cache.
func (m *Manager) DisableTarget(targetID int64) {
	m.mu.Lock()
	delete(m.enabledTargets, targetID)
	delete(m.caches, targetID)
	if queue, ok := m.queues[targetID]; ok {
		queue.close()
		delete(m.queues, targetID)
	}
	m.mu.Unlock()
}

func (m *Manager) isTargetEnabled(targetID int64) bool {
	m.mu.RLock()
	enabled := m.enabledTargets[targetID]
	running := m.running
	m.mu.RUnlock()
	return running && enabled
}

func (m *Manager) enqueueTask(targetID int64, maxConcurrent int, task func()) {
	queue := m.getOrCreateQueue(targetID, maxConcurrent)
	if queue == nil {
		return
	}
	if !queue.enqueue(task) {
		log.Debug().Int64("target_id", targetID).Msg("Plex Auto Languages: Dropped queued task")
	}
}

func (m *Manager) getOrCreateQueue(targetID int64, maxConcurrent int) *targetQueue {
	maxConcurrent = normalizeMaxConcurrent(maxConcurrent)

	m.mu.Lock()
	defer m.mu.Unlock()

	queue := m.queues[targetID]
	if queue != nil && queue.maxConcurrent == maxConcurrent {
		return queue
	}
	if queue != nil {
		queue.close()
	}
	newQueue := newTargetQueue(maxConcurrent)
	m.queues[targetID] = newQueue
	return newQueue
}

func normalizeMaxConcurrent(maxConcurrent int) int {
	if maxConcurrent < 1 {
		return 1
	}
	return maxConcurrent
}

// registerTargetCallbacks registers notification callbacks for all enabled targets
func (m *Manager) registerTargetCallbacks() error {
	targets, err := m.db.ListPlexAutoLanguagesEnabledTargets()
	if err != nil {
		return fmt.Errorf("failed to list enabled targets: %w", err)
	}

	for _, target := range targets {
		if err := m.registerTargetCallback(target.ID); err != nil {
			log.Warn().Err(err).Str("target", target.Name).Msg("Failed to register callback")
		}
	}

	log.Info().Int("targets", len(targets)).Msg("Registered Plex Auto Languages callbacks")
	return nil
}

// registerTargetCallback registers a notification callback for a single target
func (m *Manager) registerTargetCallback(targetID int64) error {
	m.mu.RLock()
	if m.enabledTargets[targetID] {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	plexTarget, err := m.targetGetter.GetPlexTarget(targetID)
	if err != nil {
		return fmt.Errorf("failed to get plex target: %w", err)
	}

	// Create cache for this target if not exists
	m.mu.Lock()
	if _, exists := m.caches[targetID]; !exists {
		m.caches[targetID] = NewCache()
	}
	m.mu.Unlock()

	// Register callback - use Any variant to avoid circular imports
	// The notification is converted via JSON marshaling since the types are structurally identical
	plexTarget.RegisterActivityCallback(func(activity ActivityNotification) {
		m.handleActivityNotification(targetID, plexTarget, activity)
	})

	plexTarget.RegisterNotificationCallbackAny(func(notificationAny any) {
		// Convert via JSON to our local type
		data, err := json.Marshal(notificationAny)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to marshal notification")
			return
		}
		var notification PlexWebSocketNotification
		if err := json.Unmarshal(data, &notification); err != nil {
			log.Warn().Err(err).Msg("Failed to unmarshal notification")
			return
		}
		if notification.NotificationContainer.Type == "activity" {
			return
		}
		m.handleNotification(targetID, plexTarget, notification)
	})

	m.mu.Lock()
	m.enabledTargets[targetID] = true
	m.mu.Unlock()

	log.Debug().Int64("target_id", targetID).Str("target", plexTarget.Name()).Msg("Registered notification callback")
	return nil
}

// handleNotification processes a WebSocket notification
func (m *Manager) handleNotification(targetID int64, target PlexTargetInterface, notification PlexWebSocketNotification) {
	if !m.isTargetEnabled(targetID) {
		return
	}

	notifType := notification.NotificationContainer.Type

	switch notifType {
	case "playing":
		for _, playing := range notification.NotificationContainer.PlaySessionStateNotification {
			m.handlePlayingNotification(targetID, target, playing)
		}
	case "timeline":
		for _, entry := range notification.NotificationContainer.TimelineEntry {
			m.handleTimelineNotification(targetID, target, entry)
		}
	case "activity":
		for _, activity := range notification.NotificationContainer.ActivityNotification {
			m.handleActivityNotification(targetID, target, activity)
		}
	}
}

// handlePlayingNotification processes a playing session notification
func (m *Manager) handlePlayingNotification(targetID int64, target PlexTargetInterface, playing PlaySessionNotification) {
	// Get cache for this target
	m.mu.RLock()
	cache, exists := m.caches[targetID]
	m.mu.RUnlock()
	if !exists {
		log.Debug().Int64("target_id", targetID).Msg("Plex Auto Languages: No cache for target")
		return
	}

	// Get config for this target
	config, err := m.db.GetPlexAutoLanguagesConfig(targetID)
	if err != nil {
		log.Error().Err(err).Int64("target_id", targetID).Msg("Plex Auto Languages: Failed to get config")
		return
	}
	if !config.TriggerOnPlay {
		log.Trace().Int64("target_id", targetID).Msg("Plex Auto Languages: Trigger on play disabled")
		return
	}

	// Check if this is a state transition we care about
	sessionKey := playing.SessionKey
	prevState, _ := cache.GetSessionState(sessionKey)

	var cachedUsername string
	if cached, ok := cache.GetUserClient(playing.ClientIdentifier); ok {
		cachedUsername = cached.Username
	}

	logEvent := log.Debug().
		Str("target", target.Name()).
		Str("session", sessionKey).
		Str("state", playing.State).
		Str("prev_state", prevState).
		Str("client", playing.ClientIdentifier).
		Str("key", playing.Key)
	if cachedUsername != "" {
		logEvent = logEvent.Str("user", cachedUsername)
	}
	logEvent.Msg("Plex Auto Languages: Play session state change")

	// Update state
	cache.SetSessionState(sessionKey, playing.State)

	// Extract rating key from the key path (e.g., "/library/metadata/12345")
	ratingKey := extractRatingKey(playing.Key)
	if ratingKey == "" {
		log.Debug().Str("key", playing.Key).Msg("Plex Auto Languages: Could not extract rating key")
		return
	}

	// Process track changes for any non-stopped state
	// We want to detect when user changes tracks during playback
	if playing.State != "stopped" {
		// Debounce rapid notifications - only process once per second per client/episode
		if !cache.ShouldProcessPlaying(playing.ClientIdentifier, ratingKey, 1*time.Second) {
			log.Trace().
				Str("client", playing.ClientIdentifier).
				Str("ratingKey", ratingKey).
				Msg("Plex Auto Languages: Skipping rapid notification (debounced)")
			return
		}
		m.enqueueTask(targetID, config.MaxConcurrent, func() {
			m.processPlayingSession(targetID, target, cache, playing.ClientIdentifier, ratingKey, config)
		})
	} else {
		// Session stopped - cleanup but also check for final track changes
		m.enqueueTask(targetID, config.MaxConcurrent, func() {
			m.processPlaybackStopped(targetID, target, playing.ClientIdentifier, ratingKey, config)
		})
	}
}

// processPlayingSession handles an active playing session to detect track changes
func (m *Manager) processPlayingSession(targetID int64, target PlexTargetInterface, cache *Cache, clientIdentifier string, ratingKey string, config *database.PlexAutoLanguagesConfig) {
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	// First, identify the user for this client - we need their token to fetch their stream preferences
	user, err := m.getUserForClient(ctx, targetID, target, clientIdentifier)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			log.Trace().Err(err).Str("clientIdentifier", clientIdentifier).Msg("Plex Auto Languages: User not yet mapped for client")
		} else {
			log.Debug().Err(err).Str("clientIdentifier", clientIdentifier).Msg("Plex Auto Languages: Failed to get user for client")
		}
		return
	}

	// Cache the user for later use
	cache.SetUserClient(clientIdentifier, user.ID, user.Name)

	userLabel := user.Name
	if userLabel == "" {
		userLabel = user.ID
	}

	userToken, err := m.getUserToken(ctx, cache, target, user.ID)
	if err != nil {
		log.Debug().Err(err).Str("user", userLabel).Msg("Plex Auto Languages: Failed to get user token, falling back to admin token")
		userToken = ""
	}

	// Fetch episode metadata using the user's token to see their stream preferences
	var episode *Episode
	if userToken != "" {
		episode, err = target.GetEpisodeWithStreamsAsUser(ctx, ratingKey, userToken)
		if err != nil {
			log.Debug().Err(err).Str("ratingKey", ratingKey).Str("user", userLabel).Msg("Plex Auto Languages: Failed to get episode as user, clearing token cache")
			// Token might be invalid, clear it and try again next time
			cache.ClearUserToken(user.ID)
			userToken = ""
			// Fall back to admin token
			episode, err = target.GetEpisodeWithStreams(ctx, ratingKey)
			if err != nil {
				log.Debug().Err(err).Str("ratingKey", ratingKey).Str("user", userLabel).Msg("Plex Auto Languages: Failed to get episode")
				return
			}
		}
	} else {
		// No user token available, use admin token
		episode, err = target.GetEpisodeWithStreams(ctx, ratingKey)
		if err != nil {
			log.Debug().Err(err).Str("ratingKey", ratingKey).Str("user", userLabel).Msg("Plex Auto Languages: Failed to get episode")
			return
		}
	}

	if episode.GrandparentKey == "" || len(episode.Parts) == 0 {
		log.Debug().Str("ratingKey", ratingKey).Str("user", userLabel).Msg("Plex Auto Languages: Not a TV episode or no media parts")
		return // Not a TV episode or no media parts
	}

	// Get selected streams from the first part
	part := &episode.Parts[0]
	selectedAudio := GetSelectedAudioStream(part)
	selectedSubtitle := GetSelectedSubtitleStream(part)

	if selectedAudio == nil {
		log.Debug().Str("ratingKey", ratingKey).Str("user", userLabel).Msg("Plex Auto Languages: No audio stream selected")
		return // No audio selected, nothing to do
	}

	// Get the stream IDs
	audioID := selectedAudio.ID
	subtitleID := 0
	if selectedSubtitle != nil {
		subtitleID = selectedSubtitle.ID
	}

	// Check if streams have changed since we last saw this episode
	cachedStreams, hasCached := cache.GetDefaultStreams(user.ID, ratingKey)

	// Always update cache with current selection to keep it fresh for stop handler
	cache.SetDefaultStreams(user.ID, ratingKey, audioID, subtitleID)

	if hasCached && cachedStreams.AudioStreamID == audioID && cachedStreams.SubtitleStreamID == subtitleID {
		// Streams unchanged, nothing to do
		return
	}

	// If this is the first time seeing this episode, just cache it and return
	if !hasCached {
		log.Debug().
			Str("show", episode.GrandparentTitle).
			Str("episode", episode.Title).
			Int("audio_id", audioID).
			Int("subtitle_id", subtitleID).
			Str("user", userLabel).
			Msg("Plex Auto Languages: Caching initial stream selection")
		return
	}

	// Streams have changed! Log and process the change
	log.Info().
		Str("show", episode.GrandparentTitle).
		Str("episode", episode.Title).
		Int("prev_audio", cachedStreams.AudioStreamID).
		Int("new_audio", audioID).
		Int("prev_subtitle", cachedStreams.SubtitleStreamID).
		Int("new_subtitle", subtitleID).
		Str("user", userLabel).
		Msg("Plex Auto Languages: Stream selection changed")

	// Save the new preference
	newPref := m.createPreference(targetID, user.ID, user.Name, episode, selectedAudio, selectedSubtitle)
	if err := m.db.UpsertPlexAutoLanguagesPreference(newPref); err != nil {
		log.Error().Err(err).Str("user", userLabel).Msg("Plex Auto Languages: Failed to save preference")
		return
	}

	log.Info().
		Str("target", target.Name()).
		Str("user", user.Name).
		Str("show", episode.GrandparentTitle).
		Str("episode", episode.Title).
		Str("audio", selectedAudio.DisplayTitle).
		Str("subtitle", subtitleDisplayTitle(selectedSubtitle)).
		Msg("Plex Auto Languages: User track preference saved")

	// Apply preference to other episodes
	result := m.applyPreferenceToShow(ctx, target, *user, userToken, cache, episode, selectedAudio, selectedSubtitle, config)

	// Log history
	if result.EpisodesChanged > 0 {
		history := &database.PlexAutoLanguagesHistory{
			TargetID:         targetID,
			PlexUserID:       user.ID,
			PlexUsername:     user.Name,
			ShowTitle:        episode.GrandparentTitle,
			ShowRatingKey:    episode.GrandparentKey,
			EpisodeTitle:     episode.Title,
			EpisodeRatingKey: episode.RatingKey,
			EventType:        database.PlexAutoLanguagesEventTypePlay,
			AudioChanged:     result.AudioChanges > 0,
			AudioTo:          selectedAudio.DisplayTitle,
			SubtitleChanged:  result.SubtitleChanges > 0,
			SubtitleTo:       subtitleDisplayTitle(selectedSubtitle),
			EpisodesUpdated:  result.EpisodesChanged,
		}
		if err := m.db.CreatePlexAutoLanguagesHistory(history); err != nil {
			log.Warn().Err(err).Str("user", userLabel).Msg("Plex Auto Languages: Failed to create history entry")
		}

		// Broadcast SSE event
		m.broadcastEvent(sse.EventPlexAutoLanguagesTrackChanged, map[string]any{
			"target_id":        targetID,
			"target_name":      target.Name(),
			"user":             user.Name,
			"show":             episode.GrandparentTitle,
			"episode":          episode.Title,
			"audio":            selectedAudio.DisplayTitle,
			"subtitle":         subtitleDisplayTitle(selectedSubtitle),
			"episodes_updated": result.EpisodesChanged,
		})
	}
}

// processPlaybackStopped handles when a user stops playing an episode
func (m *Manager) processPlaybackStopped(targetID int64, target PlexTargetInterface, clientIdentifier string, ratingKey string, config *database.PlexAutoLanguagesConfig) {
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	// Get the episode with streams to see what was selected (prefer live session data)
	liveData := true
	episode, err := target.GetSessionEpisodeWithStreams(ctx, clientIdentifier, ratingKey)
	if err != nil {
		liveData = false
		if errors.Is(err, ErrNoActiveSessions) {
			log.Trace().
				Str("ratingKey", ratingKey).
				Str("clientIdentifier", clientIdentifier).
				Msg("No active sessions found, falling back to library metadata")
		} else {
			log.Trace().
				Err(err).
				Str("ratingKey", ratingKey).
				Msg("Failed to get live session episode, falling back to library metadata")
		}
		episode, err = target.GetEpisodeWithStreams(ctx, ratingKey)
		if err != nil {
			log.Debug().Err(err).Str("ratingKey", ratingKey).Msg("Failed to get episode")
			return
		}
	}

	if episode.GrandparentKey == "" || len(episode.Parts) == 0 {
		return // Not a TV episode or no media parts
	}

	// Get the user for this client
	user, err := m.getUserForClient(ctx, targetID, target, clientIdentifier)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			log.Trace().Err(err).Str("clientIdentifier", clientIdentifier).Msg("Plex Auto Languages: User not yet mapped for client")
		} else {
			log.Debug().Err(err).Str("clientIdentifier", clientIdentifier).Msg("Failed to get user for client")
		}
		return
	}

	userLabel := user.Name
	if userLabel == "" {
		userLabel = user.ID
	}

	// If live data is gone (session already stopped), try to reapply the last known stream
	// selection from cache so we don't lose mid-playback changes.
	if !liveData {
		var cache *Cache
		m.mu.RLock()
		cache = m.caches[targetID]
		m.mu.RUnlock()

		if cache != nil {
			if cachedStreams, ok := cache.GetDefaultStreams(user.ID, ratingKey); ok {
				part := &episode.Parts[0]
				applyCachedSelection(part, cachedStreams)
				log.Trace().
					Str("ratingKey", ratingKey).
					Int("audio_id", cachedStreams.AudioStreamID).
					Int("subtitle_id", cachedStreams.SubtitleStreamID).
					Str("user", userLabel).
					Msg("Applied cached stream selection after session stop")
			}
		}
	}

	// Get selected streams from the first part
	part := &episode.Parts[0]
	selectedAudio := GetSelectedAudioStream(part)
	selectedSubtitle := GetSelectedSubtitleStream(part)

	if selectedAudio == nil {
		return // No audio selected, nothing to do
	}

	// Check if preferences changed
	pref, err := m.db.GetPlexAutoLanguagesPreference(targetID, user.ID, episode.GrandparentKey)
	if err != nil {
		log.Error().Err(err).Str("user", userLabel).Msg("Failed to get preference")
		return
	}

	// If we only have fallback metadata and an existing preference, avoid overwriting it with possibly stale data
	if !liveData && pref != nil {
		log.Trace().
			Str("show", episode.GrandparentTitle).
			Str("user", user.Name).
			Msg("Skipping preference update on stop because live session data was unavailable and preference already exists")
		return
	}

	// Check if tracks match existing preferences
	tracksMatch := m.tracksMatchPreference(selectedAudio, selectedSubtitle, pref)
	if tracksMatch {
		log.Trace().
			Str("show", episode.GrandparentTitle).
			Str("user", user.Name).
			Msg("Tracks match existing preference, no update needed")
		return
	}

	// User changed tracks - save new preference
	newPref := m.createPreference(targetID, user.ID, user.Name, episode, selectedAudio, selectedSubtitle)
	if err := m.db.UpsertPlexAutoLanguagesPreference(newPref); err != nil {
		log.Error().Err(err).Str("user", userLabel).Msg("Failed to save preference")
		return
	}

	log.Info().
		Str("target", target.Name()).
		Str("user", user.Name).
		Str("show", episode.GrandparentTitle).
		Str("episode", episode.Title).
		Str("audio", selectedAudio.DisplayTitle).
		Str("subtitle", subtitleDisplayTitle(selectedSubtitle)).
		Msg("Plex Auto Languages: User track preference saved")

	// Apply preference to other episodes
	m.mu.RLock()
	cache := m.caches[targetID]
	m.mu.RUnlock()

	userToken, err := m.getUserToken(ctx, cache, target, user.ID)
	if err != nil {
		log.Debug().Err(err).Str("user", userLabel).Msg("Plex Auto Languages: Failed to get user token, skipping apply")
		userToken = ""
	}
	result := m.applyPreferenceToShow(ctx, target, *user, userToken, cache, episode, selectedAudio, selectedSubtitle, config)

	// Log history
	if result.EpisodesChanged > 0 {
		history := &database.PlexAutoLanguagesHistory{
			TargetID:         targetID,
			PlexUserID:       user.ID,
			PlexUsername:     user.Name,
			ShowTitle:        episode.GrandparentTitle,
			ShowRatingKey:    episode.GrandparentKey,
			EpisodeTitle:     episode.Title,
			EpisodeRatingKey: episode.RatingKey,
			EventType:        database.PlexAutoLanguagesEventTypePlay,
			AudioChanged:     result.AudioChanges > 0,
			AudioTo:          selectedAudio.DisplayTitle,
			SubtitleChanged:  result.SubtitleChanges > 0,
			SubtitleTo:       subtitleDisplayTitle(selectedSubtitle),
			EpisodesUpdated:  result.EpisodesChanged,
		}
		if err := m.db.CreatePlexAutoLanguagesHistory(history); err != nil {
			log.Warn().Err(err).Msg("Failed to create history entry")
		}

		// Broadcast SSE event
		m.broadcastEvent(sse.EventPlexAutoLanguagesTrackChanged, map[string]any{
			"target_id":        targetID,
			"target_name":      target.Name(),
			"user":             user.Name,
			"show":             episode.GrandparentTitle,
			"episode":          episode.Title,
			"audio":            selectedAudio.DisplayTitle,
			"subtitle":         subtitleDisplayTitle(selectedSubtitle),
			"episodes_updated": result.EpisodesChanged,
		})
	}
}

// handleTimelineNotification processes timeline notifications (new episodes added)
func (m *Manager) handleTimelineNotification(targetID int64, target PlexTargetInterface, entry TimelineEntry) {
	// We're interested in new episodes that are completed processing
	if entry.Type != MediaTypeEpisode || entry.State != TimelineStateCompleted {
		return
	}

	ratingKey := fmt.Sprintf("%d", entry.ItemID)

	// Get cache
	m.mu.RLock()
	cache, exists := m.caches[targetID]
	m.mu.RUnlock()
	if !exists {
		return
	}

	// Check if recently processed to avoid duplicates
	if cache.HasRecentActivity("timeline", ratingKey, 5*time.Minute) {
		return
	}
	cache.MarkRecentActivity("timeline", ratingKey)

	// Mark as newly added for potential preference application
	cache.MarkNewlyAdded(ratingKey)

	// Get config
	config, err := m.db.GetPlexAutoLanguagesConfig(targetID)
	if err != nil || !config.TriggerOnScan {
		return
	}

	// Process asynchronously
	m.enqueueTask(targetID, config.MaxConcurrent, func() {
		m.processNewEpisode(targetID, target, ratingKey, config)
	})
}

// processNewEpisode applies user preferences to a newly added episode
func (m *Manager) processNewEpisode(targetID int64, target PlexTargetInterface, ratingKey string, config *database.PlexAutoLanguagesConfig) {
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	// Get the episode
	episode, err := target.GetEpisodeWithStreams(ctx, ratingKey)
	if err != nil {
		log.Debug().Err(err).Str("ratingKey", ratingKey).Msg("Failed to get new episode")
		return
	}

	if episode.GrandparentKey == "" || len(episode.Parts) == 0 {
		return
	}

	// Get all user preferences for this show
	prefs, err := m.db.ListPlexAutoLanguagesPreferencesForShow(targetID, episode.GrandparentKey)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get preferences for show")
		return
	}

	if len(prefs) == 0 {
		return // No preferences for this show
	}

	var episodeAddedAt time.Time
	if episode.AddedAt > 0 {
		episodeAddedAt = time.Unix(episode.AddedAt, 0)
	}

	m.mu.RLock()
	cache := m.caches[targetID]
	m.mu.RUnlock()

	log.Debug().
		Str("show", episode.GrandparentTitle).
		Str("episode", episode.Title).
		Int("preferences", len(prefs)).
		Msg("Applying preferences to new episode")

	// Apply each user's preference to this episode
	part := &episode.Parts[0]
	for _, pref := range prefs {
		userLabel := pref.PlexUsername
		if userLabel == "" {
			userLabel = pref.PlexUserID
		}

		if config.UpdateStrategy == database.PlexAutoLanguagesUpdateStrategyNext && !episodeAddedAt.IsZero() {
			if pref.UpdatedAt.After(episodeAddedAt) {
				log.Trace().
					Str("show", episode.GrandparentTitle).
					Str("episode", episode.Title).
					Str("user", userLabel).
					Msg("Skipping preference for new episode due to update strategy")
				continue
			}
		}

		// Build reference streams from preference
		refAudio := m.preferenceToAudioStream(pref)
		refSubtitle := m.preferenceToSubtitleStream(pref)

		// Match streams
		matchedAudio := MatchAudioStream(refAudio, part.AudioStreams)
		matchedSubtitle := MatchSubtitleStream(refSubtitle, refAudio, part.SubtitleStreams)

		audioID := 0
		if matchedAudio != nil {
			audioID = matchedAudio.ID
		}
		subtitleID := 0
		if matchedSubtitle != nil {
			subtitleID = matchedSubtitle.ID
		}

		// Check if anything needs to change
		currentAudio := GetSelectedAudioStream(part)
		currentSubtitle := GetSelectedSubtitleStream(part)

		audioChanged := matchedAudio != nil && (currentAudio == nil || currentAudio.ID != matchedAudio.ID)
		subtitleChanged := matchedSubtitle != nil && (currentSubtitle == nil || currentSubtitle.ID != matchedSubtitle.ID)

		// Also handle case where we need to disable subtitles
		if matchedSubtitle == nil && currentSubtitle != nil {
			subtitleChanged = true
			subtitleID = 0 // Disable subtitles
		}

		if !audioChanged && !subtitleChanged {
			continue
		}

		userToken, err := m.getUserToken(ctx, cache, target, pref.PlexUserID)
		if err != nil {
			log.Warn().Err(err).
				Str("episode", episode.Title).
				Str("user", userLabel).
				Msg("Failed to get user token for new episode preference")
			continue
		}

		// Apply changes
		if err := target.SetStreamsAsUser(ctx, part.ID, audioID, subtitleID, userToken); err != nil {
			if cache != nil && errors.Is(err, ErrInvalidUserToken) {
				cache.ClearUserToken(pref.PlexUserID)
			}
			log.Warn().Err(err).
				Str("episode", episode.Title).
				Str("user", userLabel).
				Msg("Failed to apply preference to new episode")
			continue
		}

		log.Info().
			Str("target", target.Name()).
			Str("show", episode.GrandparentTitle).
			Str("episode", episode.Title).
			Str("user", userLabel).
			Msg("Plex Auto Languages: Applied preferences to new episode")

		// Log history
		history := &database.PlexAutoLanguagesHistory{
			TargetID:         targetID,
			PlexUserID:       pref.PlexUserID,
			ShowTitle:        episode.GrandparentTitle,
			ShowRatingKey:    episode.GrandparentKey,
			EpisodeTitle:     episode.Title,
			EpisodeRatingKey: episode.RatingKey,
			EventType:        database.PlexAutoLanguagesEventTypeNewEpisode,
			AudioChanged:     audioChanged,
			AudioTo:          audioDisplayTitle(matchedAudio),
			SubtitleChanged:  subtitleChanged,
			SubtitleTo:       subtitleDisplayTitle(matchedSubtitle),
			EpisodesUpdated:  1,
		}
		if err := m.db.CreatePlexAutoLanguagesHistory(history); err != nil {
			log.Warn().Err(err).Str("user", userLabel).Msg("Failed to create history entry")
		}
	}
}

// handleActivityNotification processes activity notifications
func (m *Manager) handleActivityNotification(targetID int64, target PlexTargetInterface, activity ActivityNotification) {
	// Get config
	config, err := m.db.GetPlexAutoLanguagesConfig(targetID)
	if err != nil || !config.TriggerOnActivity {
		return
	}

	// Currently we handle media.generate activities similar to timeline
	if !strings.HasPrefix(activity.Activity.Type, "library.update") &&
		!strings.HasPrefix(activity.Activity.Type, "media.generate") {
		return
	}

	ratingKey := extractRatingKey(activity.Activity.Context.Key)
	if ratingKey == "" {
		return
	}

	// Get cache
	m.mu.RLock()
	cache, exists := m.caches[targetID]
	m.mu.RUnlock()
	if !exists {
		return
	}

	// Check if recently processed to avoid duplicates
	if cache.HasRecentActivity("activity", ratingKey, 5*time.Minute) {
		return
	}
	cache.MarkRecentActivity("activity", ratingKey)
	cache.MarkNewlyAdded(ratingKey)

	log.Trace().
		Int64("target_id", targetID).
		Str("type", activity.Activity.Type).
		Str("event", activity.Event).
		Str("ratingKey", ratingKey).
		Msg("Plex Auto Languages activity notification")

	// Process asynchronously
	m.enqueueTask(targetID, config.MaxConcurrent, func() {
		m.processNewEpisode(targetID, target, ratingKey, config)
	})
}

// applyPreferenceToShow applies the user's preference to other episodes in the show
func (m *Manager) applyPreferenceToShow(
	ctx context.Context,
	target PlexTargetInterface,
	user PlexUser,
	userToken string,
	cache *Cache,
	triggerEpisode *Episode,
	refAudio *AudioStream,
	refSubtitle *SubtitleStream,
	config *database.PlexAutoLanguagesConfig,
) *ChangeResult {
	result := &ChangeResult{}

	if userToken == "" {
		userLabel := user.Name
		if userLabel == "" {
			userLabel = user.ID
		}
		result.Errors = append(result.Errors, "missing user token")
		log.Debug().
			Str("target", target.Name()).
			Str("user", userLabel).
			Msg("Plex Auto Languages: Missing user token for stream updates")
		return result
	}

	// Get episodes based on update level
	// Note: This returns lightweight episode metadata without stream info
	var episodes []Episode
	var err error

	switch config.UpdateLevel {
	case database.PlexAutoLanguagesUpdateLevelSeason:
		if triggerEpisode.ParentKey == "" {
			err = fmt.Errorf("missing parent key for season update level")
			result.Errors = append(result.Errors, err.Error())
			userLabel := user.Name
			if userLabel == "" {
				userLabel = user.ID
			}
			log.Warn().
				Str("show", triggerEpisode.GrandparentTitle).
				Str("episode", triggerEpisode.Title).
				Str("user", userLabel).
				Msg("Plex Auto Languages: Missing season key, skipping update")
			return result
		}
		episodes, err = target.GetSeasonEpisodes(ctx, triggerEpisode.ParentKey)
	default: // show
		episodes, err = target.GetShowEpisodes(ctx, triggerEpisode.GrandparentKey)
	}

	if err != nil {
		result.Errors = append(result.Errors, err.Error())
		return result
	}

	// Ensure deterministic ordering for update strategy evaluation.
	sort.SliceStable(episodes, func(i, j int) bool {
		if episodes[i].ParentIndex != episodes[j].ParentIndex {
			return episodes[i].ParentIndex < episodes[j].ParentIndex
		}
		if episodes[i].Index != episodes[j].Index {
			return episodes[i].Index < episodes[j].Index
		}
		return episodes[i].RatingKey < episodes[j].RatingKey
	})

	// Filter episodes based on update strategy
	episodesToUpdate := m.filterEpisodesByStrategy(episodes, triggerEpisode, config.UpdateStrategy)

	userLabel := user.Name
	if userLabel == "" {
		userLabel = user.ID
	}
	log.Debug().
		Str("show", triggerEpisode.GrandparentTitle).
		Int("total_episodes", len(episodes)).
		Int("episodes_to_update", len(episodesToUpdate)).
		Str("strategy", string(config.UpdateStrategy)).
		Str("user", userLabel).
		Msg("Plex Auto Languages: Applying preference to episodes")

	// Apply preference to each episode
	for i := range episodesToUpdate {
		ep := &episodesToUpdate[i]

		// Skip the trigger episode (already has correct tracks)
		if ep.RatingKey == triggerEpisode.RatingKey {
			continue
		}

		// The episode list from GetShowEpisodes doesn't include stream info
		// We need to reload each episode to get the full metadata with streams
		fullEpisode, err := target.GetEpisodeWithStreams(ctx, ep.RatingKey)
		if err != nil {
			log.Warn().Err(err).
				Str("episode", ep.Title).
				Str("ratingKey", ep.RatingKey).
				Str("user", userLabel).
				Msg("Plex Auto Languages: Failed to reload episode, skipping")
			result.Errors = append(result.Errors, err.Error())
			continue
		}

		if len(fullEpisode.Parts) == 0 {
			log.Debug().
				Str("episode", ep.Title).
				Str("user", userLabel).
				Msg("Plex Auto Languages: Episode has no media parts, skipping")
			continue
		}

		part := &fullEpisode.Parts[0]
		result.EpisodesProcessed++
		changed, invalidToken := m.applyPreferenceToEpisode(ctx, target, user.ID, userToken, cache, part, refAudio, refSubtitle)
		if invalidToken {
			result.Errors = append(result.Errors, "invalid user token")
			log.Warn().
				Str("user", userLabel).
				Msg("Plex Auto Languages: Invalid user token, skipping remaining updates")
			break
		}

		if changed.AudioChanged || changed.SubtitleChanged {
			result.EpisodesChanged++
			if changed.AudioChanged {
				result.AudioChanges++
			}
			if changed.SubtitleChanged {
				result.SubtitleChanges++
			}
			result.Changes = append(result.Changes, *changed)

			log.Debug().
				Str("episode", fullEpisode.Title).
				Str("user", userLabel).
				Bool("audio_changed", changed.AudioChanged).
				Bool("subtitle_changed", changed.SubtitleChanged).
				Msg("Plex Auto Languages: Updated episode streams")
		} else {
			log.Debug().
				Str("episode", fullEpisode.Title).
				Str("user", userLabel).
				Msg("Plex Auto Languages: Episode already has correct streams, skipping")
		}
	}

	if result.EpisodesProcessed > 0 {
		log.Info().
			Str("show", triggerEpisode.GrandparentTitle).
			Int("processed", result.EpisodesProcessed).
			Int("changed", result.EpisodesChanged).
			Int("skipped", result.EpisodesProcessed-result.EpisodesChanged).
			Str("user", userLabel).
			Msg("Plex Auto Languages: Finished applying preferences")
	}

	return result
}

// applyPreferenceToEpisode applies the preference to a single episode's part
func (m *Manager) applyPreferenceToEpisode(
	ctx context.Context,
	target PlexTargetInterface,
	userID string,
	userToken string,
	cache *Cache,
	part *MediaPart,
	refAudio *AudioStream,
	refSubtitle *SubtitleStream,
) (*TrackChange, bool) {
	change := &TrackChange{
		PartID: part.ID,
	}

	// Match audio stream
	matchedAudio := MatchAudioStream(refAudio, part.AudioStreams)
	matchedSubtitle := MatchSubtitleStream(refSubtitle, refAudio, part.SubtitleStreams)

	// Check current selections
	currentAudio := GetSelectedAudioStream(part)
	currentSubtitle := GetSelectedSubtitleStream(part)

	// Determine what needs to change
	audioID := 0
	subtitleID := -1 // -1 means don't change

	if matchedAudio != nil && (currentAudio == nil || currentAudio.ID != matchedAudio.ID) {
		change.AudioChanged = true
		change.NewAudioID = matchedAudio.ID
		if currentAudio != nil {
			change.OldAudioID = currentAudio.ID
		}
		audioID = matchedAudio.ID
	}

	if matchedSubtitle != nil {
		if currentSubtitle == nil || currentSubtitle.ID != matchedSubtitle.ID {
			change.SubtitleChanged = true
			change.NewSubtitleID = matchedSubtitle.ID
			if currentSubtitle != nil {
				change.OldSubtitleID = currentSubtitle.ID
			}
			subtitleID = matchedSubtitle.ID
		}
	} else if refSubtitle == nil && currentSubtitle != nil {
		// User had no subtitles, but this episode has them selected - disable
		change.SubtitleChanged = true
		change.OldSubtitleID = currentSubtitle.ID
		change.NewSubtitleID = 0
		subtitleID = 0
	}

	if !change.AudioChanged && !change.SubtitleChanged {
		return change, false // Nothing to do
	}

	// Apply the changes
	if err := target.SetStreamsAsUser(ctx, part.ID, audioID, subtitleID, userToken); err != nil {
		if cache != nil && errors.Is(err, ErrInvalidUserToken) {
			cache.ClearUserToken(userID)
			log.Warn().Err(err).Int("partID", part.ID).Str("user", userID).Msg("Invalid user token, aborting updates")
			return change, true
		}
		log.Warn().Err(err).Int("partID", part.ID).Str("user", userID).Msg("Failed to set streams")
		change.AudioChanged = false
		change.SubtitleChanged = false
		return change, false
	}

	return change, false
}

// filterEpisodesByStrategy filters episodes based on the update strategy
func (m *Manager) filterEpisodesByStrategy(episodes []Episode, triggerEpisode *Episode, strategy database.PlexAutoLanguagesUpdateStrategy) []Episode {
	switch strategy {
	case database.PlexAutoLanguagesUpdateStrategyNext:
		// Only update episodes after the trigger episode
		var result []Episode
		foundTrigger := false
		for _, ep := range episodes {
			if ep.RatingKey == triggerEpisode.RatingKey {
				foundTrigger = true
				result = append(result, ep) // Include the trigger for reference
				continue
			}
			if foundTrigger {
				// Include if same season and higher episode, or higher season
				if ep.ParentIndex > triggerEpisode.ParentIndex ||
					(ep.ParentIndex == triggerEpisode.ParentIndex && ep.Index > triggerEpisode.Index) {
					result = append(result, ep)
				}
			}
		}
		return result
	default: // all
		return episodes
	}
}

// getUserForClient looks up the user associated with a client identifier
func (m *Manager) getUserForClient(ctx context.Context, targetID int64, target PlexTargetInterface, clientIdentifier string) (*PlexUser, error) {
	// Check cache first
	m.mu.RLock()
	cache, exists := m.caches[targetID]
	m.mu.RUnlock()

	if exists {
		if cached, ok := cache.GetUserClient(clientIdentifier); ok {
			return &PlexUser{
				ID:   cached.UserID,
				Name: cached.Username,
			}, nil
		}
	}

	// Fetch from session mapping
	mapping, err := target.GetSessionUserMapping(ctx)
	if err != nil {
		return nil, err
	}

	if user, ok := mapping[clientIdentifier]; ok {
		// Cache for future use
		if cache != nil {
			cache.SetUserClient(clientIdentifier, user.ID, user.Name)
		}
		return &user, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrUserNotFound, clientIdentifier)
}

// getUserToken retrieves or fetches a user's token for this server.
func (m *Manager) getUserToken(ctx context.Context, cache *Cache, target PlexTargetInterface, userID string) (string, error) {
	if cache != nil {
		if token, ok := cache.GetUserToken(userID); ok {
			return token, nil
		}
	}

	var machineID string
	if cache != nil {
		if cachedID, ok := cache.GetMachineIdentifier(); ok {
			machineID = cachedID
		}
	}

	if machineID == "" {
		id, err := target.GetMachineIdentifier(ctx)
		if err != nil {
			return "", err
		}
		machineID = id
		if cache != nil {
			cache.SetMachineIdentifier(machineID)
		}
	}

	token, err := target.GetUserTokenWithMachineID(ctx, userID, machineID)
	if err != nil {
		return "", err
	}
	if cache != nil {
		cache.SetUserToken(userID, token)
	}
	return token, nil
}

// tracksMatchPreference checks if selected tracks match the stored preference
func (m *Manager) tracksMatchPreference(audio *AudioStream, subtitle *SubtitleStream, pref *database.PlexAutoLanguagesPreference) bool {
	if pref == nil {
		return false // No preference stored
	}

	// Check audio
	if audio.LanguageCode != pref.AudioLanguageCode {
		return false
	}
	if audio.Codec != pref.AudioCodec {
		return false
	}
	if audio.Channels != pref.AudioChannels {
		return false
	}
	if pref.AudioChannelLayout != "" && audio.AudioChannelLayout != pref.AudioChannelLayout {
		return false
	}
	if pref.AudioTitle != "" && audio.Title != pref.AudioTitle {
		return false
	}
	if pref.AudioDisplayTitle != "" && audio.DisplayTitle != pref.AudioDisplayTitle {
		return false
	}
	if audio.VisualImpaired != pref.AudioVisualImpaired {
		return false
	}

	// Check subtitle
	if subtitle == nil {
		return pref.SubtitleLanguageCode == nil
	}
	if pref.SubtitleLanguageCode == nil {
		return false
	}
	if subtitle.LanguageCode != *pref.SubtitleLanguageCode {
		return false
	}
	if subtitle.Forced != pref.SubtitleForced {
		return false
	}
	if subtitle.HearingImpaired != pref.SubtitleHearingImpaired {
		return false
	}
	if pref.SubtitleCodec != nil && subtitle.Codec != *pref.SubtitleCodec {
		return false
	}
	if pref.SubtitleTitle != nil && subtitle.Title != *pref.SubtitleTitle {
		return false
	}
	if pref.SubtitleDisplayTitle != nil && subtitle.DisplayTitle != *pref.SubtitleDisplayTitle {
		return false
	}

	return true
}

// createPreference creates a preference record from the selected streams
func (m *Manager) createPreference(targetID int64, userID, username string, episode *Episode, audio *AudioStream, subtitle *SubtitleStream) *database.PlexAutoLanguagesPreference {
	pref := &database.PlexAutoLanguagesPreference{
		TargetID:            targetID,
		PlexUserID:          userID,
		PlexUsername:        username,
		ShowRatingKey:       episode.GrandparentKey,
		ShowTitle:           episode.GrandparentTitle,
		AudioLanguageCode:   audio.LanguageCode,
		AudioCodec:          audio.Codec,
		AudioChannels:       audio.Channels,
		AudioChannelLayout:  audio.AudioChannelLayout,
		AudioTitle:          audio.Title,
		AudioDisplayTitle:   audio.DisplayTitle,
		AudioVisualImpaired: audio.VisualImpaired,
	}

	if subtitle != nil {
		pref.SubtitleLanguageCode = &subtitle.LanguageCode
		pref.SubtitleForced = subtitle.Forced
		pref.SubtitleHearingImpaired = subtitle.HearingImpaired
		pref.SubtitleCodec = &subtitle.Codec
		pref.SubtitleTitle = &subtitle.Title
		pref.SubtitleDisplayTitle = &subtitle.DisplayTitle
	}

	return pref
}

// preferenceToAudioStream converts a preference to a reference AudioStream for matching
func (m *Manager) preferenceToAudioStream(pref *database.PlexAutoLanguagesPreference) *AudioStream {
	return &AudioStream{
		LanguageCode:       pref.AudioLanguageCode,
		Codec:              pref.AudioCodec,
		Channels:           pref.AudioChannels,
		AudioChannelLayout: pref.AudioChannelLayout,
		Title:              pref.AudioTitle,
		DisplayTitle:       pref.AudioDisplayTitle,
		VisualImpaired:     pref.AudioVisualImpaired,
	}
}

// preferenceToSubtitleStream converts a preference to a reference SubtitleStream for matching
func (m *Manager) preferenceToSubtitleStream(pref *database.PlexAutoLanguagesPreference) *SubtitleStream {
	if pref.SubtitleLanguageCode == nil {
		return nil
	}

	sub := &SubtitleStream{
		LanguageCode:    *pref.SubtitleLanguageCode,
		Forced:          pref.SubtitleForced,
		HearingImpaired: pref.SubtitleHearingImpaired,
	}
	if pref.SubtitleCodec != nil {
		sub.Codec = *pref.SubtitleCodec
	}
	if pref.SubtitleTitle != nil {
		sub.Title = *pref.SubtitleTitle
	}
	if pref.SubtitleDisplayTitle != nil {
		sub.DisplayTitle = *pref.SubtitleDisplayTitle
	}

	return sub
}

// runCacheCleanup periodically cleans up expired cache entries
func (m *Manager) runCacheCleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			for _, cache := range m.caches {
				cache.CleanupExpired(1 * time.Hour)
			}
			m.mu.RUnlock()
		}
	}
}

// Helper functions

// extractRatingKey extracts the rating key from a Plex key path
func extractRatingKey(key string) string {
	// Key format: /library/metadata/12345
	parts := strings.Split(key, "/")
	if len(parts) >= 4 && parts[1] == "library" && parts[2] == "metadata" {
		return parts[3]
	}
	return ""
}

// subtitleDisplayTitle returns a display title for a subtitle stream, or "None" if nil
func subtitleDisplayTitle(s *SubtitleStream) string {
	if s == nil {
		return "None"
	}
	if s.DisplayTitle != "" {
		return s.DisplayTitle
	}
	return s.LanguageCode
}

// audioDisplayTitle returns a display title for an audio stream
func audioDisplayTitle(s *AudioStream) string {
	if s == nil {
		return "Unknown"
	}
	if s.DisplayTitle != "" {
		return s.DisplayTitle
	}
	return s.LanguageCode
}

// applyCachedSelection marks the given part's streams as selected based on cached IDs.
// This is used when live session data is unavailable (e.g., after stop) but we saw the
// selection earlier during playback.
func applyCachedSelection(part *MediaPart, cached *StreamSelection) {
	// Clear existing selections
	for i := range part.AudioStreams {
		part.AudioStreams[i].Selected = false
	}
	for i := range part.SubtitleStreams {
		part.SubtitleStreams[i].Selected = false
	}

	// Apply audio
	for i := range part.AudioStreams {
		if part.AudioStreams[i].ID == cached.AudioStreamID {
			part.AudioStreams[i].Selected = true
			break
		}
	}

	// Apply subtitle
	if cached.SubtitleStreamID > 0 {
		for i := range part.SubtitleStreams {
			if part.SubtitleStreams[i].ID == cached.SubtitleStreamID {
				part.SubtitleStreams[i].Selected = true
				break
			}
		}
	}
}

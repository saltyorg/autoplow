package plexautolang

import (
	"sync"
	"time"
)

// StreamSelection represents the selected audio/subtitle streams for an episode
type StreamSelection struct {
	AudioStreamID    int
	SubtitleStreamID int // 0 = no subtitle
	CachedAt         time.Time
}

// UserClient maps a client identifier to a user
type UserClient struct {
	UserID   string
	Username string
	CachedAt time.Time
}

// UserToken caches a user's access token
type UserToken struct {
	Token    string
	CachedAt time.Time
}

// MachineIdentifier caches the server's machine identifier
type MachineIdentifier struct {
	ID       string
	CachedAt time.Time
}

// SessionEpisode caches episode metadata for an active session
type SessionEpisode struct {
	Episode   *Episode
	RatingKey string
	CachedAt  time.Time
}

// Cache holds in-memory caches for Plex Auto Languages
type Cache struct {
	mu sync.RWMutex

	// sessionStates tracks session state (playing/paused/stopped) by session key
	sessionStates map[string]string

	// defaultStreams caches the default stream selections for episodes
	// Key: userID:ratingKey
	defaultStreams map[string]*StreamSelection

	// userClients maps client identifiers to users
	userClients map[string]*UserClient

	// userTokens caches user access tokens by user ID
	userTokens map[string]*UserToken

	// newlyAdded tracks recently added episodes (rating key -> added timestamp)
	newlyAdded map[string]time.Time

	// recentActivities tracks recent activity notifications to avoid duplicates
	// Key: "userID:ratingKey"
	recentActivities map[string]time.Time

	// machineIdentifier caches the server's machine identifier
	machineIdentifier *MachineIdentifier

	// lastProcessed tracks when we last processed a client/ratingKey combo
	// Key: "clientIdentifier:ratingKey"
	lastProcessed map[string]time.Time

	// sessionEpisodes caches episode metadata per session key
	sessionEpisodes map[string]*SessionEpisode
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{
		sessionStates:    make(map[string]string),
		defaultStreams:   make(map[string]*StreamSelection),
		userClients:      make(map[string]*UserClient),
		userTokens:       make(map[string]*UserToken),
		newlyAdded:       make(map[string]time.Time),
		recentActivities: make(map[string]time.Time),
		lastProcessed:    make(map[string]time.Time),
		sessionEpisodes:  make(map[string]*SessionEpisode),
	}
}

// GetSessionState returns the cached session state for a session key
func (c *Cache) GetSessionState(sessionKey string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, ok := c.sessionStates[sessionKey]
	return state, ok
}

// SetSessionState updates the cached session state
func (c *Cache) SetSessionState(sessionKey, state string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state == "stopped" {
		delete(c.sessionStates, sessionKey)
	} else {
		c.sessionStates[sessionKey] = state
	}
}

// GetSessionEpisode returns the cached episode for a session key
func (c *Cache) GetSessionEpisode(sessionKey, ratingKey string) (*Episode, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sessionEpisode, ok := c.sessionEpisodes[sessionKey]
	if !ok || sessionEpisode == nil {
		return nil, false
	}
	if ratingKey != "" && sessionEpisode.RatingKey != ratingKey {
		return nil, false
	}
	return sessionEpisode.Episode, true
}

// SetSessionEpisode caches episode metadata for a session key
func (c *Cache) SetSessionEpisode(sessionKey, ratingKey string, episode *Episode) {
	if sessionKey == "" || episode == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionEpisodes[sessionKey] = &SessionEpisode{
		Episode:   episode,
		RatingKey: ratingKey,
		CachedAt:  time.Now(),
	}
}

// ClearSessionEpisode removes cached episode metadata for a session key
func (c *Cache) ClearSessionEpisode(sessionKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessionEpisodes, sessionKey)
}

// GetDefaultStreams returns the cached default streams for an episode and user
func (c *Cache) GetDefaultStreams(userID, ratingKey string) (*StreamSelection, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sel, ok := c.defaultStreams[defaultStreamKey(userID, ratingKey)]
	return sel, ok
}

// SetDefaultStreams caches the default streams for an episode and user
func (c *Cache) SetDefaultStreams(userID, ratingKey string, audioID, subtitleID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultStreams[defaultStreamKey(userID, ratingKey)] = &StreamSelection{
		AudioStreamID:    audioID,
		SubtitleStreamID: subtitleID,
		CachedAt:         time.Now(),
	}
}

// InvalidateDefaultStreams removes the cached default streams for an episode and user
func (c *Cache) InvalidateDefaultStreams(userID, ratingKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.defaultStreams, defaultStreamKey(userID, ratingKey))
}

// GetUserClient returns the cached user for a client identifier
func (c *Cache) GetUserClient(clientIdentifier string) (*UserClient, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	user, ok := c.userClients[clientIdentifier]
	return user, ok
}

// SetUserClient caches the user for a client identifier
func (c *Cache) SetUserClient(clientIdentifier, userID, username string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userClients[clientIdentifier] = &UserClient{
		UserID:   userID,
		Username: username,
		CachedAt: time.Now(),
	}
}

// GetUserToken returns the cached token for a user ID
func (c *Cache) GetUserToken(userID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	token, ok := c.userTokens[userID]
	if !ok {
		return "", false
	}
	return token.Token, true
}

// SetUserToken caches the token for a user ID
func (c *Cache) SetUserToken(userID, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userTokens[userID] = &UserToken{
		Token:    token,
		CachedAt: time.Now(),
	}
}

// ClearUserToken removes the cached token for a user ID
func (c *Cache) ClearUserToken(userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.userTokens, userID)
}

// GetMachineIdentifier returns the cached machine identifier
func (c *Cache) GetMachineIdentifier() (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.machineIdentifier == nil {
		return "", false
	}
	return c.machineIdentifier.ID, true
}

// SetMachineIdentifier caches the machine identifier
func (c *Cache) SetMachineIdentifier(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.machineIdentifier = &MachineIdentifier{
		ID:       id,
		CachedAt: time.Now(),
	}
}

// ShouldProcessPlaying checks if we should process this client/ratingKey combo
// Returns true if enough time has passed since last processing (debounce)
func (c *Cache) ShouldProcessPlaying(clientIdentifier, ratingKey string, minInterval time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := clientIdentifier + ":" + ratingKey
	lastTime, exists := c.lastProcessed[key]
	if !exists || time.Since(lastTime) >= minInterval {
		c.lastProcessed[key] = time.Now()
		return true
	}
	return false
}

// IsNewlyAdded checks if an episode was recently added
func (c *Cache) IsNewlyAdded(ratingKey string, maxAge time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	addedAt, ok := c.newlyAdded[ratingKey]
	if !ok {
		return false
	}
	return time.Since(addedAt) < maxAge
}

// MarkNewlyAdded marks an episode as newly added
func (c *Cache) MarkNewlyAdded(ratingKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.newlyAdded[ratingKey] = time.Now()
}

// ClearNewlyAdded removes an episode from the newly added cache
func (c *Cache) ClearNewlyAdded(ratingKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.newlyAdded, ratingKey)
}

// HasRecentActivity checks if there was recent activity for a user/item combo
func (c *Cache) HasRecentActivity(userID, ratingKey string, maxAge time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := userID + ":" + ratingKey
	activityAt, ok := c.recentActivities[key]
	if !ok {
		return false
	}
	return time.Since(activityAt) < maxAge
}

// MarkRecentActivity marks that activity occurred for a user/item combo
func (c *Cache) MarkRecentActivity(userID, ratingKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := userID + ":" + ratingKey
	c.recentActivities[key] = time.Now()
}

// CleanupExpired removes expired entries from all caches
func (c *Cache) CleanupExpired(maxAge time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Clean up default streams cache
	for key, sel := range c.defaultStreams {
		if now.Sub(sel.CachedAt) > maxAge {
			delete(c.defaultStreams, key)
		}
	}

	// Clean up user clients cache
	for key, user := range c.userClients {
		if now.Sub(user.CachedAt) > maxAge {
			delete(c.userClients, key)
		}
	}

	// Clean up user tokens cache (use longer expiry - 12 hours)
	tokenMaxAge := 12 * time.Hour
	for key, token := range c.userTokens {
		if now.Sub(token.CachedAt) > tokenMaxAge {
			delete(c.userTokens, key)
		}
	}

	// Clean up newly added cache
	for key, addedAt := range c.newlyAdded {
		if now.Sub(addedAt) > maxAge {
			delete(c.newlyAdded, key)
		}
	}

	// Clean up recent activities cache
	for key, activityAt := range c.recentActivities {
		if now.Sub(activityAt) > maxAge {
			delete(c.recentActivities, key)
		}
	}

	// Clean up last processed cache
	for key, processedAt := range c.lastProcessed {
		if now.Sub(processedAt) > maxAge {
			delete(c.lastProcessed, key)
		}
	}

	// Clean up session episode cache
	for key, sessionEpisode := range c.sessionEpisodes {
		if sessionEpisode == nil {
			delete(c.sessionEpisodes, key)
			continue
		}
		if now.Sub(sessionEpisode.CachedAt) > maxAge {
			delete(c.sessionEpisodes, key)
		}
	}
}

// Clear removes all entries from all caches
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sessionStates = make(map[string]string)
	c.defaultStreams = make(map[string]*StreamSelection)
	c.userClients = make(map[string]*UserClient)
	c.userTokens = make(map[string]*UserToken)
	c.newlyAdded = make(map[string]time.Time)
	c.recentActivities = make(map[string]time.Time)
	c.lastProcessed = make(map[string]time.Time)
	c.sessionEpisodes = make(map[string]*SessionEpisode)
}

func defaultStreamKey(userID, ratingKey string) string {
	return userID + ":" + ratingKey
}

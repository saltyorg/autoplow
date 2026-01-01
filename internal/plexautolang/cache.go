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

// Cache holds in-memory caches for Plex Auto Languages
type Cache struct {
	mu sync.RWMutex

	// sessionStates tracks session state (playing/paused/stopped) by session key
	sessionStates map[string]string

	// defaultStreams caches the default stream selections for episodes
	// Key: rating key of episode
	defaultStreams map[string]*StreamSelection

	// userClients maps client identifiers to users
	userClients map[string]*UserClient

	// newlyAdded tracks recently added episodes (rating key -> added timestamp)
	newlyAdded map[string]time.Time

	// recentActivities tracks recent activity notifications to avoid duplicates
	// Key: "userID:ratingKey"
	recentActivities map[string]time.Time
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{
		sessionStates:    make(map[string]string),
		defaultStreams:   make(map[string]*StreamSelection),
		userClients:      make(map[string]*UserClient),
		newlyAdded:       make(map[string]time.Time),
		recentActivities: make(map[string]time.Time),
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

// GetDefaultStreams returns the cached default streams for an episode
func (c *Cache) GetDefaultStreams(ratingKey string) (*StreamSelection, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sel, ok := c.defaultStreams[ratingKey]
	return sel, ok
}

// SetDefaultStreams caches the default streams for an episode
func (c *Cache) SetDefaultStreams(ratingKey string, audioID, subtitleID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultStreams[ratingKey] = &StreamSelection{
		AudioStreamID:    audioID,
		SubtitleStreamID: subtitleID,
		CachedAt:         time.Now(),
	}
}

// InvalidateDefaultStreams removes the cached default streams for an episode
func (c *Cache) InvalidateDefaultStreams(ratingKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.defaultStreams, ratingKey)
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
}

// Clear removes all entries from all caches
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sessionStates = make(map[string]string)
	c.defaultStreams = make(map[string]*StreamSelection)
	c.userClients = make(map[string]*UserClient)
	c.newlyAdded = make(map[string]time.Time)
	c.recentActivities = make(map[string]time.Time)
}

package targets

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/saltyorg/autoplow/internal/database"
)

// JellyfinTarget implements the Target interface for Jellyfin Media Server
// It wraps MediaBrowserTarget with Jellyfin-specific configuration
type JellyfinTarget struct {
	*MediaBrowserTarget
}

// NewJellyfinTarget creates a new Jellyfin target
func NewJellyfinTarget(dbTarget *database.Target) *JellyfinTarget {
	config := MediaBrowserConfig{
		ServerName: "Jellyfin",
		TargetType: database.TargetTypeJellyfin,
		SetAuthHeader: func(req *http.Request, apiKey string) {
			req.Header.Set("Authorization", fmt.Sprintf("MediaBrowser Token=\"%s\"", apiKey))
		},
		WebSocketPath: "/socket",
		WebSocketQueryParams: func(apiKey string) url.Values {
			q := url.Values{}
			q.Set("api_key", apiKey)
			return q
		},
		// Format: "InitialDelay,Interval" in ms
		SessionsStartData: "0,1500",
	}

	return &JellyfinTarget{
		MediaBrowserTarget: NewMediaBrowserTarget(dbTarget, config),
	}
}

// Type returns the scanner type (override to ensure correct type is returned)
func (s *JellyfinTarget) Type() database.TargetType {
	return database.TargetTypeJellyfin
}

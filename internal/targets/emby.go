package targets

import (
	"net/http"
	"net/url"

	"github.com/saltyorg/autoplow/internal/database"
)

// EmbyTarget implements the Target interface for Emby Media Server
// It wraps MediaBrowserTarget with Emby-specific configuration
type EmbyTarget struct {
	*MediaBrowserTarget
}

// NewEmbyTarget creates a new Emby target
func NewEmbyTarget(dbTarget *database.Target) *EmbyTarget {
	config := MediaBrowserConfig{
		ServerName: "Emby",
		TargetType: database.TargetTypeEmby,
		SetAuthHeader: func(req *http.Request, apiKey string) {
			req.Header.Set("X-Emby-Token", apiKey)
		},
		WebSocketPath: "/embywebsocket",
		WebSocketQueryParams: func(apiKey string) url.Values {
			q := url.Values{}
			q.Set("api_key", apiKey)
			q.Set("deviceId", "autoplow")
			return q
		},
		// Format: "InitialDelay,Interval,InactiveSessionThreshold" in ms
		SessionsStartData: "0,1500,300",
	}

	return &EmbyTarget{
		MediaBrowserTarget: NewMediaBrowserTarget(dbTarget, config),
	}
}

// Type returns the scanner type (override to ensure correct type is returned)
func (s *EmbyTarget) Type() database.TargetType {
	return database.TargetTypeEmby
}

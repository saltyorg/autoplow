package targets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/media"
)

// MediaBrowserConfig holds configuration specific to each media server type
// Named after MediaBrowser, the original project that both Emby and Jellyfin forked from
type MediaBrowserConfig struct {
	// ServerName is used in log messages (e.g., "Emby", "Jellyfin")
	ServerName string

	// TargetType is the database target type
	TargetType database.TargetType

	// SetAuthHeader sets the appropriate authentication header for requests
	SetAuthHeader func(req *http.Request, apiKey string)

	// WebSocketPath is the path for WebSocket connections (e.g., "/embywebsocket", "/socket")
	WebSocketPath string

	// WebSocketQueryParams returns additional query params for WebSocket URL
	WebSocketQueryParams func(apiKey string) url.Values

	// SessionsStartData is the data field for the SessionsStart WebSocket message
	SessionsStartData string
}

// MediaBrowserTarget is a shared base implementation for Emby and Jellyfin targets
// Both media servers share nearly identical APIs, so this consolidates the common code
type MediaBrowserTarget struct {
	dbTarget *database.Target
	client   *http.Client
	config   MediaBrowserConfig
}

// NewMediaBrowserTarget creates a new media server target with the given configuration
func NewMediaBrowserTarget(dbTarget *database.Target, cfg MediaBrowserConfig) *MediaBrowserTarget {
	return &MediaBrowserTarget{
		dbTarget: dbTarget,
		client: &http.Client{
			Timeout: config.GetTimeouts().HTTPClient,
		},
		config: cfg,
	}
}

// Name returns the target name
func (s *MediaBrowserTarget) Name() string {
	return s.dbTarget.Name
}

// Type returns the target type
func (s *MediaBrowserTarget) Type() database.TargetType {
	return s.config.TargetType
}

// SupportsSessionMonitoring returns true as both Emby and Jellyfin support session monitoring
func (s *MediaBrowserTarget) SupportsSessionMonitoring() bool {
	return true
}

// SupportsWebSocket returns true as both Emby and Jellyfin support WebSocket notifications
func (s *MediaBrowserTarget) SupportsWebSocket() bool {
	return true
}

// Scan triggers a library scan for the given path
// If RefreshMetadata is enabled, it will attempt to find the item and refresh it directly
func (s *MediaBrowserTarget) Scan(ctx context.Context, path string) error {
	// Default RefreshMetadata to true unless explicitly disabled
	// If MetadataRefreshMode is empty, the user hasn't configured scan options yet, so use defaults
	refreshMetadata := s.dbTarget.Config.RefreshMetadata || s.dbTarget.Config.MetadataRefreshMode == ""

	// If refresh metadata is enabled, try to find and refresh the item directly
	if refreshMetadata {
		refreshed, err := s.tryRefreshItem(ctx, path)
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("Failed to refresh item directly, falling back to path scan")
		} else if refreshed {
			return nil
		}
		// Fall through to path-based scan if item not found or refresh failed
	}

	return s.scanPath(ctx, path)
}

// scanPath notifies the media server about a path change using /Library/Media/Updated
func (s *MediaBrowserTarget) scanPath(ctx context.Context, path string) error {
	scanURL := fmt.Sprintf("%s/Library/Media/Updated", strings.TrimRight(s.dbTarget.URL, "/"))

	// Build request body
	reqBody := map[string]any{
		"Updates": []map[string]any{
			{
				"Path":       path,
				"UpdateType": "Modified",
			},
		},
	}

	bodyJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", scanURL, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	s.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Both Emby and Jellyfin return 204 No Content on success
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s returned status %d: %s", s.config.ServerName, resp.StatusCode, string(body))
	}

	log.Info().
		Str("scanner", s.Name()).
		Str("path", path).
		Msgf("%s scan triggered", s.config.ServerName)

	return nil
}

// tryRefreshItem attempts to find an item by path and refresh it directly
// Returns true if the item was found and refreshed successfully
func (s *MediaBrowserTarget) tryRefreshItem(ctx context.Context, path string) (bool, error) {
	// First, get the libraries to find which one contains this path
	libraries, err := s.GetLibraries(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get libraries: %w", err)
	}

	// Find the library that contains this path
	var matchedLibrary *Library
	for i := range libraries {
		lib := &libraries[i]
		for _, libPath := range lib.Paths {
			if strings.HasPrefix(path, libPath) {
				matchedLibrary = lib
				break
			}
		}
		if matchedLibrary != nil {
			break
		}
	}

	if matchedLibrary == nil {
		log.Debug().Str("path", path).Msg("No matching library found for path")
		return false, nil
	}

	log.Debug().
		Str("path", path).
		Str("library", matchedLibrary.Name).
		Msg("Found matching library, searching for item")

	// Search for the item in the library
	item, err := s.findItemByPath(ctx, matchedLibrary, path)
	if err != nil {
		return false, fmt.Errorf("failed to find item: %w", err)
	}

	if item == nil {
		log.Debug().Str("path", path).Msg("Item not found in library, will use path scan")
		return false, nil
	}

	// Refresh the item
	if err := s.refreshItem(ctx, item.ID); err != nil {
		return false, fmt.Errorf("failed to refresh item: %w", err)
	}

	log.Info().
		Str("scanner", s.Name()).
		Str("path", path).
		Str("item_id", item.ID).
		Msgf("%s item refreshed directly", s.config.ServerName)

	return true, nil
}

// mediaBrowserItem represents an item in the media server library
type mediaBrowserItem struct {
	ID   string  `json:"Id"`
	Path *string `json:"Path"`
	Name string  `json:"Name"`
	Type string  `json:"Type"`
}

// mediaBrowserItemsResponse represents the response from /Items endpoint
type mediaBrowserItemsResponse struct {
	Items []mediaBrowserItem `json:"Items"`
}

// findItemByPath searches for an item in a library by its file path
func (s *MediaBrowserTarget) findItemByPath(ctx context.Context, library *Library, path string) (*mediaBrowserItem, error) {
	baseURL := strings.TrimRight(s.dbTarget.URL, "/")
	limit := 1000
	startIndex := 0

	for {
		itemsURL, err := url.Parse(fmt.Sprintf("%s/Items", baseURL))
		if err != nil {
			return nil, fmt.Errorf("failed to parse URL: %w", err)
		}

		q := itemsURL.Query()
		q.Set("ParentId", library.ID)
		q.Set("Recursive", "true")
		q.Set("Fields", "Path")
		q.Set("EnableImages", "false")
		q.Set("EnableTotalRecordCount", "false")
		q.Set("Limit", fmt.Sprintf("%d", limit))
		q.Set("StartIndex", fmt.Sprintf("%d", startIndex))

		// Filter by item type based on library type
		if library.Type != "" {
			switch library.Type {
			case "tvshows":
				q.Set("IncludeItemTypes", "Episode")
			case "movies":
				q.Set("IncludeItemTypes", "Movie")
			case "music":
				q.Set("IncludeItemTypes", "Audio")
			case "books":
				q.Set("IncludeItemTypes", "Book")
			}
		}

		itemsURL.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, "GET", itemsURL.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		s.setHeaders(req)

		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("%s returned status %d: %s", s.config.ServerName, resp.StatusCode, string(body))
		}

		var itemsResp mediaBrowserItemsResponse
		if err := json.NewDecoder(resp.Body).Decode(&itemsResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		// Search for matching item
		for i := range itemsResp.Items {
			item := &itemsResp.Items[i]
			if item.Path != nil && *item.Path == path {
				return item, nil
			}
		}

		// Check if we've retrieved all items
		if len(itemsResp.Items) < limit {
			break
		}

		startIndex += limit
	}

	return nil, nil
}

// refreshItem refreshes an item's metadata by ID
func (s *MediaBrowserTarget) refreshItem(ctx context.Context, itemID string) error {
	baseURL := strings.TrimRight(s.dbTarget.URL, "/")

	refreshURL, err := url.Parse(fmt.Sprintf("%s/Items/%s/Refresh", baseURL, itemID))
	if err != nil {
		return fmt.Errorf("failed to parse URL: %w", err)
	}

	// Get refresh mode from config, default to FullRefresh
	refreshMode := s.dbTarget.Config.MetadataRefreshMode
	if refreshMode == "" {
		refreshMode = database.MetadataRefreshModeFullRefresh
	}

	q := refreshURL.Query()
	q.Set("MetadataRefreshMode", string(refreshMode))
	q.Set("ImageRefreshMode", string(refreshMode))
	q.Set("ReplaceAllMetadata", "true")
	q.Set("Recursive", "true")
	q.Set("ReplaceAllImages", "false")
	q.Set("RegenerateTrickplay", "false")
	refreshURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", refreshURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	s.setHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s returned status %d: %s", s.config.ServerName, resp.StatusCode, string(body))
	}

	return nil
}

// setHeaders sets the authentication headers for requests
func (s *MediaBrowserTarget) setHeaders(req *http.Request) {
	s.config.SetAuthHeader(req, s.dbTarget.APIKey)
	req.Header.Set("Accept", "application/json")
}

// TestConnection verifies the connection to the media server
func (s *MediaBrowserTarget) TestConnection(ctx context.Context) error {
	testURL := fmt.Sprintf("%s/System/Info", strings.TrimRight(s.dbTarget.URL, "/"))

	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	s.setHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("invalid API key")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

// GetLibraries returns available libraries from the media server
func (s *MediaBrowserTarget) GetLibraries(ctx context.Context) ([]Library, error) {
	librariesURL := fmt.Sprintf("%s/Library/VirtualFolders", strings.TrimRight(s.dbTarget.URL, "/"))

	req, err := http.NewRequestWithContext(ctx, "GET", librariesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	s.setHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var folders []mediaBrowserVirtualFolder
	if err := json.NewDecoder(resp.Body).Decode(&folders); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	libraries := make([]Library, 0, len(folders))
	for _, folder := range folders {
		lib := Library{
			ID:   folder.ItemID,
			Name: folder.Name,
			Type: folder.CollectionType,
		}
		if len(folder.Locations) > 0 {
			lib.Path = folder.Locations[0]
			lib.Paths = folder.Locations
		}
		libraries = append(libraries, lib)
	}

	return libraries, nil
}

// GetSessions returns active playback sessions from the media server
func (s *MediaBrowserTarget) GetSessions(ctx context.Context) ([]Session, error) {
	sessionsURL := fmt.Sprintf("%s/Sessions", strings.TrimRight(s.dbTarget.URL, "/"))

	req, err := http.NewRequestWithContext(ctx, "GET", sessionsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	s.setHeaders(req)

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

	var serverSessions []mediaBrowserSession
	if err := json.Unmarshal(body, &serverSessions); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	sessions := make([]Session, 0)
	for _, ss := range serverSessions {
		// Only include sessions with active playback
		if ss.NowPlayingItem.Name == "" {
			continue
		}

		session := Session{
			ID:        ss.ID,
			Username:  ss.UserName,
			MediaType: ss.NowPlayingItem.Type,
			Player:    ss.Client,
		}

		// Format title: "Show Name - Episode Name" for episodes with SeriesName
		if ss.NowPlayingItem.Type == "Episode" && ss.NowPlayingItem.SeriesName != "" {
			session.MediaTitle = ss.NowPlayingItem.SeriesName + " - " + ss.NowPlayingItem.Name
		} else {
			session.MediaTitle = ss.NowPlayingItem.Name
		}

		// Get playback info
		if ss.PlayState.PlayMethod == "Transcode" {
			session.Transcoding = true
		}

		// Get video format (resolution + codec) from video stream
		if len(ss.NowPlayingItem.MediaStreams) > 0 {
			for _, stream := range ss.NowPlayingItem.MediaStreams {
				if stream.Type == "Video" {
					session.Format = media.ParseVideoFormat(stream.DisplayTitle, stream.Height, stream.Codec)
					break
				}
			}
		}

		// Get streaming bitrate (already in bps)
		if session.Transcoding && ss.TranscodingInfo.Bitrate > 0 {
			session.Bitrate = int64(ss.TranscodingInfo.Bitrate)
		} else if ss.NowPlayingItem.Bitrate > 0 {
			session.Bitrate = int64(ss.NowPlayingItem.Bitrate)
		}

		sessions = append(sessions, session)
	}

	return sessions, nil
}

// Media server JSON structures (shared by Emby and Jellyfin)
type mediaBrowserVirtualFolder struct {
	Name           string   `json:"Name"`
	Locations      []string `json:"Locations"`
	CollectionType string   `json:"CollectionType"`
	ItemID         string   `json:"ItemId"`
}

type mediaBrowserSession struct {
	ID              string                      `json:"Id"`
	UserName        string                      `json:"UserName"`
	Client          string                      `json:"Client"`
	NowPlayingItem  mediaBrowserNowPlaying      `json:"NowPlayingItem"`
	PlayState       mediaBrowserPlayState       `json:"PlayState"`
	TranscodingInfo mediaBrowserTranscodingInfo `json:"TranscodingInfo"`
}

type mediaBrowserNowPlaying struct {
	Name         string                    `json:"Name"`
	SeriesName   string                    `json:"SeriesName"` // Show name for episodes
	Type         string                    `json:"Type"`
	Bitrate      int                       `json:"Bitrate"`
	MediaStreams []mediaBrowserMediaStream `json:"MediaStreams"`
}

type mediaBrowserMediaStream struct {
	Type         string `json:"Type"`
	Codec        string `json:"Codec"`
	Height       int    `json:"Height"`
	BitRate      int    `json:"BitRate"`
	DisplayTitle string `json:"DisplayTitle"`
}

type mediaBrowserPlayState struct {
	PlayMethod string `json:"PlayMethod"`
}

type mediaBrowserTranscodingInfo struct {
	Bitrate int `json:"Bitrate"`
}

// WatchSessions starts a WebSocket connection to the media server for real-time session updates
// It calls the callback whenever session state changes
// The function blocks until the context is cancelled or an unrecoverable error occurs
func (s *MediaBrowserTarget) WatchSessions(ctx context.Context, callback func(sessions []Session)) error {
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
				Dur("backoff", backoff).
				Msgf("%s WebSocket disconnected, reconnecting", s.config.ServerName)

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
func (s *MediaBrowserTarget) watchSessionsOnce(ctx context.Context, callback func(sessions []Session), pingInterval time.Duration) error {
	// Build WebSocket URL
	wsURL, err := s.buildWebSocketURL()
	if err != nil {
		return fmt.Errorf("failed to build WebSocket URL: %w", err)
	}

	log.Debug().
		Str("target", s.Name()).
		Str("url", wsURL).
		Msgf("Connecting to %s WebSocket", s.config.ServerName)

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("WebSocket dial failed: %w", err)
	}
	defer conn.Close()

	log.Info().
		Str("target", s.Name()).
		Msgf("Connected to %s WebSocket", s.config.ServerName)

	// Subscribe to session updates
	subscribeMsg := mediaBrowserWSMessage{
		MessageType: "SessionsStart",
		Data:        s.config.SessionsStartData,
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		return fmt.Errorf("failed to send subscription message: %w", err)
	}

	// Fetch initial sessions
	sessions, err := s.GetSessions(ctx)
	if err != nil {
		log.Warn().Err(err).Str("target", s.Name()).Msg("Failed to fetch initial sessions")
	} else {
		callback(sessions)
	}

	// Start ping ticker to keep connection alive
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

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

			log.Trace().
				Str("target", s.Name()).
				RawJSON("message", message).
				Msg("Received WebSocket message")

			// Parse the message
			var msg mediaBrowserWSResponse
			if err := json.Unmarshal(message, &msg); err != nil {
				log.Debug().
					Err(err).
					Str("target", s.Name()).
					Msg("Failed to parse WebSocket message")
				continue
			}

			// Check if this is a session-related message
			if s.isSessionMessage(msg.MessageType) {
				log.Debug().
					Str("target", s.Name()).
					Str("type", msg.MessageType).
					Msg("Received session notification, fetching sessions")

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

	// Main loop: handle pings and context cancellation
	for {
		select {
		case <-ctx.Done():
			// Send close message
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return ctx.Err()
		case err := <-readErrCh:
			return err
		case <-pingTicker.C:
			// Send KeepAlive message
			keepAlive := mediaBrowserWSMessage{MessageType: "KeepAlive"}
			if err := conn.WriteJSON(keepAlive); err != nil {
				return fmt.Errorf("keep-alive failed: %w", err)
			}
		}
	}
}

// buildWebSocketURL constructs the WebSocket URL for the media server
func (s *MediaBrowserTarget) buildWebSocketURL() (string, error) {
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

	// Set WebSocket path
	parsed.Path = s.config.WebSocketPath

	// Get query parameters from config
	parsed.RawQuery = s.config.WebSocketQueryParams(s.dbTarget.APIKey).Encode()

	return parsed.String(), nil
}

// isSessionMessage checks if the message type is related to sessions
func (s *MediaBrowserTarget) isSessionMessage(messageType string) bool {
	return messageType == "Sessions" ||
		messageType == "SessionEnded" ||
		messageType == "PlaybackStart" ||
		messageType == "PlaybackStopped" ||
		messageType == "PlaybackProgress"
}

// WebSocket message structures
type mediaBrowserWSMessage struct {
	MessageType string `json:"MessageType"`
	Data        string `json:"Data,omitempty"`
}

type mediaBrowserWSResponse struct {
	MessageType string          `json:"MessageType"`
	Data        json.RawMessage `json:"Data,omitempty"`
}

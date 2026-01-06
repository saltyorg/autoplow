package targets

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/plexautolang"
)

// plexMetadataResponse represents the response from /library/metadata/{id}
type plexMetadataResponse struct {
	MediaContainer plexMetadataContainer `json:"MediaContainer"`
}

type plexMetadataContainer struct {
	Size     int                    `json:"size"`
	Metadata []plexDetailedMetadata `json:"Metadata"`
}

type plexDetailedMetadata struct {
	RatingKey        string              `json:"ratingKey"`
	Key              string              `json:"key"`
	GUID             string              `json:"guid"`
	Guid             []plexGuid          `json:"Guid,omitempty"` // External GUIDs (array)
	Type             string              `json:"type"`
	Title            string              `json:"title"`
	GrandparentTitle string              `json:"grandparentTitle,omitempty"` // Show title
	GrandparentKey   string              `json:"grandparentKey,omitempty"`   // Show key
	ParentIndex      int                 `json:"parentIndex,omitempty"`      // Season number
	Index            int                 `json:"index,omitempty"`            // Episode number
	AddedAt          int64               `json:"addedAt"`
	UpdatedAt        int64               `json:"updatedAt"`
	Media            []plexDetailedMedia `json:"Media,omitempty"`
}

type plexGuid struct {
	ID string `json:"id"`
}

type plexDetailedMedia struct {
	ID              int                `json:"id"`
	Duration        int                `json:"duration"`
	VideoResolution string             `json:"videoResolution"`
	VideoCodec      string             `json:"videoCodec"`
	AudioCodec      string             `json:"audioCodec"`
	Part            []plexDetailedPart `json:"Part"`
}

type plexDetailedPart struct {
	ID     int          `json:"id"`
	Key    string       `json:"key"`
	File   string       `json:"file"`
	Stream []plexStream `json:"Stream,omitempty"`
}

type plexStream struct {
	ID                   int    `json:"id"`
	StreamType           int    `json:"streamType"` // 1=video, 2=audio, 3=subtitle
	Default              bool   `json:"default"`
	Selected             bool   `json:"selected"`
	LanguageCode         string `json:"languageCode,omitempty"`
	LanguageTag          string `json:"languageTag,omitempty"`
	Codec                string `json:"codec,omitempty"`
	Channels             int    `json:"channels,omitempty"`
	AudioChannelLayout   string `json:"audioChannelLayout,omitempty"`
	Title                string `json:"title,omitempty"`
	DisplayTitle         string `json:"displayTitle,omitempty"`
	ExtendedDisplayTitle string `json:"extendedDisplayTitle,omitempty"`
	Forced               bool   `json:"forced,omitempty"`
	HearingImpaired      bool   `json:"hearingImpaired,omitempty"`
	VisualImpaired       bool   `json:"visualImpaired,omitempty"`
	Index                int    `json:"index"`
}

// plexSystemAccountsResponse represents the response from /accounts
type plexSystemAccountsResponse struct {
	MediaContainer struct {
		Size    int                 `json:"size"`
		Account []plexSystemAccount `json:"Account"`
	} `json:"MediaContainer"`
}

type plexSystemAccount struct {
	ID    int    `json:"id"`
	Key   string `json:"key"`
	Name  string `json:"name"`
	Thumb string `json:"thumb,omitempty"`
}

// GetSessionEpisodeWithStreams fetches the current playing item's streams from /status/sessions for a client
// This captures the user's live selections, which may differ from library metadata
func (s *PlexTarget) GetSessionEpisodeWithStreams(ctx context.Context, clientIdentifier string, ratingKey string) (*plexautolang.Episode, error) {
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("clientIdentifier", clientIdentifier).
		Str("ratingKey", ratingKey).
		RawJSON("response", body).
		Msg("Fetched session data")

	// Parse sessions to locate the matching client/ratingKey
	var sessionsResp struct {
		MediaContainer struct {
			Metadata []struct {
				RatingKey        string `json:"ratingKey"`
				Key              string `json:"key"`
				Title            string `json:"title"`
				GrandparentTitle string `json:"grandparentTitle"`
				GrandparentKey   string `json:"grandparentKey"`
				ParentIndex      int    `json:"parentIndex"`
				Index            int    `json:"index"`
				AddedAt          int64  `json:"addedAt"`
				UpdatedAt        int64  `json:"updatedAt"`
				Media            []struct {
					Part []struct {
						ID     string `json:"id"`
						Key    string `json:"key"`
						File   string `json:"file"`
						Stream []struct {
							ID                   string `json:"id"`
							StreamType           int    `json:"streamType"`
							Default              bool   `json:"default"`
							Selected             bool   `json:"selected"`
							LanguageCode         string `json:"languageCode"`
							LanguageTag          string `json:"languageTag"`
							Codec                string `json:"codec"`
							Channels             int    `json:"channels"`
							AudioChannelLayout   string `json:"audioChannelLayout"`
							Title                string `json:"title"`
							DisplayTitle         string `json:"displayTitle"`
							ExtendedDisplayTitle string `json:"extendedDisplayTitle"`
							Forced               bool   `json:"forced"`
							HearingImpaired      bool   `json:"hearingImpaired"`
							VisualImpaired       bool   `json:"visualImpaired"`
							Index                int    `json:"index"`
						} `json:"Stream"`
					} `json:"Part"`
				} `json:"Media"`
				Player struct {
					MachineIdentifier string `json:"machineIdentifier"`
				} `json:"Player"`
			} `json:"Metadata"`
		} `json:"MediaContainer"`
	}

	if err := json.Unmarshal(body, &sessionsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	for _, meta := range sessionsResp.MediaContainer.Metadata {
		if meta.RatingKey != ratingKey {
			continue
		}
		if meta.Player.MachineIdentifier != clientIdentifier {
			continue
		}

		// Convert to Episode using the live stream selection
		ep := &plexautolang.Episode{
			RatingKey:        meta.RatingKey,
			Key:              meta.Key,
			Title:            meta.Title,
			GrandparentTitle: meta.GrandparentTitle,
			GrandparentKey:   meta.GrandparentKey,
			ParentIndex:      meta.ParentIndex,
			Index:            meta.Index,
			AddedAt:          meta.AddedAt,
			UpdatedAt:        meta.UpdatedAt,
		}

		for _, media := range meta.Media {
			for _, part := range media.Part {
				mediaPart := plexautolang.MediaPart{
					ID:   parseStringID(part.ID),
					Key:  part.Key,
					File: part.File,
				}

				for _, stream := range part.Stream {
					switch stream.StreamType {
					case 2:
						mediaPart.AudioStreams = append(mediaPart.AudioStreams, plexautolang.AudioStream{
							ID:                   parseStringID(stream.ID),
							StreamType:           stream.StreamType,
							LanguageCode:         stream.LanguageCode,
							LanguageTag:          stream.LanguageTag,
							Codec:                stream.Codec,
							Channels:             stream.Channels,
							AudioChannelLayout:   stream.AudioChannelLayout,
							Title:                stream.Title,
							DisplayTitle:         stream.DisplayTitle,
							ExtendedDisplayTitle: stream.ExtendedDisplayTitle,
							VisualImpaired:       stream.VisualImpaired,
							Selected:             stream.Selected,
							Default:              stream.Default,
							Index:                stream.Index,
						})
					case 3:
						mediaPart.SubtitleStreams = append(mediaPart.SubtitleStreams, plexautolang.SubtitleStream{
							ID:                   parseStringID(stream.ID),
							StreamType:           stream.StreamType,
							LanguageCode:         stream.LanguageCode,
							LanguageTag:          stream.LanguageTag,
							Codec:                stream.Codec,
							Title:                stream.Title,
							DisplayTitle:         stream.DisplayTitle,
							ExtendedDisplayTitle: stream.ExtendedDisplayTitle,
							Forced:               stream.Forced,
							HearingImpaired:      stream.HearingImpaired,
							Selected:             stream.Selected,
							Default:              stream.Default,
							Index:                stream.Index,
						})
					}
				}

				ep.Parts = append(ep.Parts, mediaPart)
			}
		}

		return ep, nil
	}

	return nil, fmt.Errorf("no matching session for client %s and ratingKey %s", clientIdentifier, ratingKey)
}

// GetEpisodeWithStreams fetches episode metadata including all audio/subtitle streams
func (s *PlexTarget) GetEpisodeWithStreams(ctx context.Context, ratingKey string) (*plexautolang.Episode, error) {
	metadataURL := fmt.Sprintf("%s/library/metadata/%s", s.dbTarget.URL, ratingKey)

	req, err := http.NewRequestWithContext(ctx, "GET", metadataURL, nil)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("ratingKey", ratingKey).
		RawJSON("response", body).
		Msg("Fetched episode metadata")

	var metaResp plexMetadataResponse
	if err := json.Unmarshal(body, &metaResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if len(metaResp.MediaContainer.Metadata) == 0 {
		return nil, fmt.Errorf("no metadata found for rating key %s", ratingKey)
	}

	meta := metaResp.MediaContainer.Metadata[0]
	return s.convertToEpisode(&meta), nil
}

// GetShowEpisodes fetches all episodes for a show with their stream information
func (s *PlexTarget) GetShowEpisodes(ctx context.Context, showKey string) ([]plexautolang.Episode, error) {
	// Clean up the show key if it contains the full path
	showKey = strings.TrimPrefix(showKey, "/library/metadata/")

	// Get all episodes (allLeaves)
	episodesURL := fmt.Sprintf("%s/library/metadata/%s/allLeaves", s.dbTarget.URL, showKey)

	req, err := http.NewRequestWithContext(ctx, "GET", episodesURL, nil)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("showKey", showKey).
		Int("bodyLength", len(body)).
		Msg("Fetched show episodes")

	var episodesResp plexMetadataResponse
	if err := json.Unmarshal(body, &episodesResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	episodes := make([]plexautolang.Episode, 0, len(episodesResp.MediaContainer.Metadata))
	for i := range episodesResp.MediaContainer.Metadata {
		ep := s.convertToEpisode(&episodesResp.MediaContainer.Metadata[i])
		episodes = append(episodes, *ep)
	}

	log.Debug().
		Str("target", s.Name()).
		Str("showKey", showKey).
		Int("episodes", len(episodes)).
		Msg("Fetched show episodes")

	return episodes, nil
}

// GetSeasonEpisodes fetches episodes for a specific season
func (s *PlexTarget) GetSeasonEpisodes(ctx context.Context, seasonKey string) ([]plexautolang.Episode, error) {
	// Clean up the season key if it contains the full path
	seasonKey = strings.TrimPrefix(seasonKey, "/library/metadata/")

	episodesURL := fmt.Sprintf("%s/library/metadata/%s/children", s.dbTarget.URL, seasonKey)

	req, err := http.NewRequestWithContext(ctx, "GET", episodesURL, nil)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	var episodesResp plexMetadataResponse
	if err := json.Unmarshal(body, &episodesResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	episodes := make([]plexautolang.Episode, 0, len(episodesResp.MediaContainer.Metadata))
	for i := range episodesResp.MediaContainer.Metadata {
		ep := s.convertToEpisode(&episodesResp.MediaContainer.Metadata[i])
		episodes = append(episodes, *ep)
	}

	return episodes, nil
}

// SetStreams sets the audio and/or subtitle streams for a media part
// audioStreamID: the stream ID to set, or 0 to not change
// subtitleStreamID: the stream ID to set, or 0 to disable subtitles, or -1 to not change
func (s *PlexTarget) SetStreams(ctx context.Context, partID int, audioStreamID, subtitleStreamID int) error {
	setURL := fmt.Sprintf("%s/library/parts/%d", s.dbTarget.URL, partID)

	params := url.Values{}

	if audioStreamID > 0 {
		params.Set("audioStreamID", fmt.Sprintf("%d", audioStreamID))
	}

	// subtitleStreamID: 0 = disable, >0 = set specific stream, -1 = don't change
	if subtitleStreamID >= 0 {
		params.Set("subtitleStreamID", fmt.Sprintf("%d", subtitleStreamID))
	}

	if len(params) == 0 {
		return nil // Nothing to change
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", setURL+"?"+params.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Debug().
		Str("target", s.Name()).
		Int("partID", partID).
		Int("audioStreamID", audioStreamID).
		Int("subtitleStreamID", subtitleStreamID).
		Msg("Set stream selection")

	return nil
}

// GetSystemAccounts fetches all system accounts (users) from Plex
func (s *PlexTarget) GetSystemAccounts(ctx context.Context) ([]plexautolang.PlexUser, error) {
	accountsURL := s.dbTarget.URL + "/accounts"

	req, err := http.NewRequestWithContext(ctx, "GET", accountsURL, nil)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		RawJSON("response", body).
		Msg("Fetched system accounts")

	var accountsResp plexSystemAccountsResponse
	if err := json.Unmarshal(body, &accountsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	users := make([]plexautolang.PlexUser, 0, len(accountsResp.MediaContainer.Account))
	for _, acc := range accountsResp.MediaContainer.Account {
		users = append(users, plexautolang.PlexUser{
			ID:       fmt.Sprintf("%d", acc.ID),
			Name:     acc.Name,
			Username: acc.Name, // Plex uses Name for both
			Thumb:    acc.Thumb,
		})
	}

	log.Debug().
		Str("target", s.Name()).
		Int("users", len(users)).
		Msg("Fetched system accounts")

	return users, nil
}

// GetRecentlyAdded fetches recently added episodes from a library section
func (s *PlexTarget) GetRecentlyAddedEpisodes(ctx context.Context, libraryID string, limit int) ([]plexautolang.Episode, error) {
	recentURL := fmt.Sprintf("%s/library/sections/%s/recentlyAdded", s.dbTarget.URL, libraryID)

	params := url.Values{}
	params.Set("type", "4") // Type 4 = episodes
	if limit > 0 {
		params.Set("X-Plex-Container-Size", fmt.Sprintf("%d", limit))
	}

	req, err := http.NewRequestWithContext(ctx, "GET", recentURL+"?"+params.Encode(), nil)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	var recentResp plexMetadataResponse
	if err := json.Unmarshal(body, &recentResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	episodes := make([]plexautolang.Episode, 0, len(recentResp.MediaContainer.Metadata))
	for i := range recentResp.MediaContainer.Metadata {
		ep := s.convertToEpisode(&recentResp.MediaContainer.Metadata[i])
		episodes = append(episodes, *ep)
	}

	return episodes, nil
}

// convertToEpisode converts internal Plex metadata to the plexautolang Episode type
func (s *PlexTarget) convertToEpisode(meta *plexDetailedMetadata) *plexautolang.Episode {
	ep := &plexautolang.Episode{
		RatingKey:        meta.RatingKey,
		Key:              meta.Key,
		Title:            meta.Title,
		GrandparentTitle: meta.GrandparentTitle,
		GrandparentKey:   meta.GrandparentKey,
		ParentIndex:      meta.ParentIndex,
		Index:            meta.Index,
		AddedAt:          meta.AddedAt,
		UpdatedAt:        meta.UpdatedAt,
	}

	// Convert media parts and streams
	for _, media := range meta.Media {
		for _, part := range media.Part {
			mediaPart := plexautolang.MediaPart{
				ID:   part.ID,
				Key:  part.Key,
				File: part.File,
			}

			for _, stream := range part.Stream {
				switch stream.StreamType {
				case 2: // Audio
					mediaPart.AudioStreams = append(mediaPart.AudioStreams, plexautolang.AudioStream{
						ID:                   stream.ID,
						StreamType:           stream.StreamType,
						LanguageCode:         stream.LanguageCode,
						LanguageTag:          stream.LanguageTag,
						Codec:                stream.Codec,
						Channels:             stream.Channels,
						AudioChannelLayout:   stream.AudioChannelLayout,
						Title:                stream.Title,
						DisplayTitle:         stream.DisplayTitle,
						ExtendedDisplayTitle: stream.ExtendedDisplayTitle,
						VisualImpaired:       stream.VisualImpaired,
						Selected:             stream.Selected,
						Default:              stream.Default,
						Index:                stream.Index,
					})
				case 3: // Subtitle
					mediaPart.SubtitleStreams = append(mediaPart.SubtitleStreams, plexautolang.SubtitleStream{
						ID:                   stream.ID,
						StreamType:           stream.StreamType,
						LanguageCode:         stream.LanguageCode,
						LanguageTag:          stream.LanguageTag,
						Codec:                stream.Codec,
						Title:                stream.Title,
						DisplayTitle:         stream.DisplayTitle,
						ExtendedDisplayTitle: stream.ExtendedDisplayTitle,
						Forced:               stream.Forced,
						HearingImpaired:      stream.HearingImpaired,
						Selected:             stream.Selected,
						Default:              stream.Default,
						Index:                stream.Index,
					})
				}
			}

			ep.Parts = append(ep.Parts, mediaPart)
		}
	}

	return ep
}

// GetSessionUserMapping returns a mapping of client identifiers to user info
// This is used to determine which user is watching on which client
func (s *PlexTarget) GetSessionUserMapping(ctx context.Context) (map[string]plexautolang.PlexUser, error) {
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse sessions to extract user/client mapping
	var sessionsResp struct {
		MediaContainer struct {
			Metadata []struct {
				User struct {
					ID    string `json:"id"`
					Title string `json:"title"`
					Thumb string `json:"thumb"`
				} `json:"User"`
				Player struct {
					MachineIdentifier string `json:"machineIdentifier"`
				} `json:"Player"`
			} `json:"Metadata"`
		} `json:"MediaContainer"`
	}

	if err := json.Unmarshal(body, &sessionsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	mapping := make(map[string]plexautolang.PlexUser)
	for _, session := range sessionsResp.MediaContainer.Metadata {
		if session.Player.MachineIdentifier != "" {
			mapping[session.Player.MachineIdentifier] = plexautolang.PlexUser{
				ID:       session.User.ID,
				Name:     session.User.Title,
				Username: session.User.Title,
				Thumb:    session.User.Thumb,
			}
		}
	}

	return mapping, nil
}

// DBTarget returns the underlying database target for external access
func (s *PlexTarget) DBTarget() *database.Target {
	return s.dbTarget
}

// parseStringID safely converts a string numeric ID to int, returning 0 on error
func parseStringID(id string) int {
	v, err := strconv.Atoi(id)
	if err != nil {
		return 0
	}
	return v
}

// GetMachineIdentifier returns the Plex server's machine identifier
func (s *PlexTarget) GetMachineIdentifier(ctx context.Context) (string, error) {
	identityURL := s.dbTarget.URL + "/identity"

	req, err := http.NewRequestWithContext(ctx, "GET", identityURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	var identityResp struct {
		MediaContainer struct {
			MachineIdentifier string `json:"machineIdentifier"`
		} `json:"MediaContainer"`
	}
	if err := json.Unmarshal(body, &identityResp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	return identityResp.MediaContainer.MachineIdentifier, nil
}

// GetUserTokenWithMachineID retrieves the access token for a specific user from plex.tv
// This token allows making API requests as that user to see their stream preferences
// The machineID should be cached by the caller to avoid repeated /identity calls
func (s *PlexTarget) GetUserTokenWithMachineID(ctx context.Context, userID string, machineID string) (string, error) {
	// Query plex.tv for shared server tokens
	sharedServersURL := fmt.Sprintf("https://plex.tv/api/servers/%s/shared_servers", machineID)

	req, err := http.NewRequestWithContext(ctx, "GET", sharedServersURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	// Note: plex.tv always returns XML for this endpoint regardless of Accept header

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("plex.tv returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("userID", userID).
		Str("response", string(body)).
		Msg("Fetched shared servers from plex.tv")

	// Parse the response - plex.tv returns XML
	var sharedResp struct {
		XMLName      xml.Name `xml:"MediaContainer"`
		SharedServer []struct {
			UserID      int    `xml:"userID,attr"`
			AccessToken string `xml:"accessToken,attr"`
		} `xml:"SharedServer"`
	}
	if err := xml.Unmarshal(body, &sharedResp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	// Find the matching user
	userIDInt, _ := strconv.Atoi(userID)
	for _, shared := range sharedResp.SharedServer {
		if shared.UserID == userIDInt {
			return shared.AccessToken, nil
		}
	}

	return "", fmt.Errorf("no token found for user %s", userID)
}

// GetEpisodeWithStreamsAsUser fetches episode metadata using a specific user's token
// This returns the stream selections as that user sees them (their preferences)
func (s *PlexTarget) GetEpisodeWithStreamsAsUser(ctx context.Context, ratingKey string, userToken string) (*plexautolang.Episode, error) {
	metadataURL := fmt.Sprintf("%s/library/metadata/%s", s.dbTarget.URL, ratingKey)

	req, err := http.NewRequestWithContext(ctx, "GET", metadataURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Use the user's token instead of the admin token
	req.Header.Set("X-Plex-Token", userToken)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Trace().
		Str("target", s.Name()).
		Str("ratingKey", ratingKey).
		RawJSON("response", body).
		Msg("Fetched episode metadata as user")

	var metaResp plexMetadataResponse
	if err := json.Unmarshal(body, &metaResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if len(metaResp.MediaContainer.Metadata) == 0 {
		return nil, fmt.Errorf("no metadata found for rating key %s", ratingKey)
	}

	meta := metaResp.MediaContainer.Metadata[0]
	return s.convertToEpisode(&meta), nil
}

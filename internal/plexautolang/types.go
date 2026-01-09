// Package plexautolang implements automatic audio/subtitle track selection for Plex.
// It monitors playback events and applies user preferences to other episodes in the same show.
package plexautolang

import "errors"

// ErrInvalidUserToken indicates a Plex user token is invalid or unauthorized.
var ErrInvalidUserToken = errors.New("plex user token invalid")

// ErrNoActiveSessions indicates Plex returned no active sessions.
var ErrNoActiveSessions = errors.New("plex has no active sessions")

// AudioStream represents a Plex audio stream
type AudioStream struct {
	ID                   int    `json:"id"`
	StreamType           int    `json:"streamType"` // Always 2 for audio
	LanguageCode         string `json:"languageCode"`
	LanguageTag          string `json:"languageTag"`
	Codec                string `json:"codec"`
	Channels             int    `json:"channels"`
	AudioChannelLayout   string `json:"audioChannelLayout"`
	Title                string `json:"title"`
	DisplayTitle         string `json:"displayTitle"`
	ExtendedDisplayTitle string `json:"extendedDisplayTitle"`
	VisualImpaired       bool   `json:"visualImpaired"`
	Selected             bool   `json:"selected"`
	Default              bool   `json:"default"`
	Index                int    `json:"index"`
}

// SubtitleStream represents a Plex subtitle stream
type SubtitleStream struct {
	ID                   int    `json:"id"`
	StreamType           int    `json:"streamType"` // Always 3 for subtitles
	LanguageCode         string `json:"languageCode"`
	LanguageTag          string `json:"languageTag"`
	Codec                string `json:"codec"`
	Title                string `json:"title"`
	DisplayTitle         string `json:"displayTitle"`
	ExtendedDisplayTitle string `json:"extendedDisplayTitle"`
	Forced               bool   `json:"forced"`
	HearingImpaired      bool   `json:"hearingImpaired"`
	Selected             bool   `json:"selected"`
	Default              bool   `json:"default"`
	Index                int    `json:"index"`
}

// MediaPart represents a single media file part with its streams
type MediaPart struct {
	ID              int              `json:"id"`
	Key             string           `json:"key"`
	File            string           `json:"file"`
	AudioStreams    []AudioStream    `json:"audioStreams"`
	SubtitleStreams []SubtitleStream `json:"subtitleStreams"`
}

// Episode represents a Plex episode with minimal metadata
type Episode struct {
	RatingKey        string      `json:"ratingKey"`
	Key              string      `json:"key"`
	Title            string      `json:"title"`
	GrandparentTitle string      `json:"grandparentTitle"` // Show title
	GrandparentKey   string      `json:"grandparentKey"`   // Show key
	ParentKey        string      `json:"parentKey"`        // Season key
	ParentIndex      int         `json:"parentIndex"`      // Season number
	Index            int         `json:"index"`            // Episode number
	AddedAt          int64       `json:"addedAt"`
	UpdatedAt        int64       `json:"updatedAt"`
	Parts            []MediaPart `json:"parts"`
}

// TrackChange represents a single track change operation
type TrackChange struct {
	Episode         Episode `json:"episode"`
	PartID          int     `json:"partId"`
	AudioChanged    bool    `json:"audioChanged"`
	OldAudioID      int     `json:"oldAudioId,omitempty"`
	NewAudioID      int     `json:"newAudioId,omitempty"`
	SubtitleChanged bool    `json:"subtitleChanged"`
	OldSubtitleID   int     `json:"oldSubtitleId,omitempty"`
	NewSubtitleID   int     `json:"newSubtitleId,omitempty"` // 0 = disable
}

// ChangeResult summarizes the result of applying track changes
type ChangeResult struct {
	EpisodesProcessed int           `json:"episodesProcessed"`
	EpisodesChanged   int           `json:"episodesChanged"`
	AudioChanges      int           `json:"audioChanges"`
	SubtitleChanges   int           `json:"subtitleChanges"`
	Errors            []string      `json:"errors,omitempty"`
	Changes           []TrackChange `json:"changes,omitempty"`
}

// PlexUser represents a Plex user account
type PlexUser struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Thumb    string `json:"thumb"`
}

// PlayingNotification represents a WebSocket playing notification
type PlayingNotification struct {
	SessionKey       string `json:"sessionKey"`
	ClientIdentifier string `json:"clientIdentifier"`
	Key              string `json:"key"` // e.g., /library/metadata/12345
	RatingKey        string `json:"ratingKey"`
	State            string `json:"state"` // playing, paused, stopped
	ViewOffset       int64  `json:"viewOffset"`
}

// TimelineEntry represents a WebSocket timeline notification entry
type TimelineEntry struct {
	ItemID     int    `json:"itemID"`
	Identifier string `json:"identifier"`
	State      int    `json:"state"` // 5 = completed
	Type       int    `json:"type"`  // 4 = episode
	UpdatedAt  int64  `json:"updatedAt"`
}

// TimelineState constants
const (
	TimelineStateCreated             = 0
	TimelineStateProcessing          = 1
	TimelineStateMatching            = 2
	TimelineStateDownloadingMetadata = 3
	TimelineStateProcessingMetadata  = 4
	TimelineStateCompleted           = 5
	TimelineStateDeleted             = 9
)

// MediaType constants for timeline entries
const (
	MediaTypeMovie   = 1
	MediaTypeShow    = 2
	MediaTypeSeason  = 3
	MediaTypeEpisode = 4
)

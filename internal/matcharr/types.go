package matcharr

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/saltyorg/autoplow/internal/database"
)

// ArrMedia represents a media item from Sonarr/Radarr
type ArrMedia struct {
	Title     string `json:"title"`
	Path      string `json:"path"`
	TMDBID    int    `json:"tmdb_id,omitempty"` // Radarr movies
	TVDBID    int    `json:"tvdb_id,omitempty"` // Sonarr series
	IMDBID    string `json:"imdb_id,omitempty"` // Both
	TitleSlug string `json:"title_slug,omitempty"`
	HasFile   bool   `json:"has_file"`
}

// GetPrimaryID returns the primary ID for this media item based on Arr type
func (m *ArrMedia) GetPrimaryID(arrType database.ArrType) (idType string, idValue string) {
	switch arrType {
	case database.ArrTypeRadarr:
		if m.TMDBID > 0 {
			return "tmdb", intToString(m.TMDBID)
		}
		if m.IMDBID != "" {
			return "imdb", m.IMDBID
		}
	case database.ArrTypeSonarr:
		if m.TVDBID > 0 {
			return "tvdb", intToString(m.TVDBID)
		}
		if m.IMDBID != "" {
			return "imdb", m.IMDBID
		}
	}
	return "", ""
}

// MediaServerItem represents a media item from Plex/Emby/Jellyfin
type MediaServerItem struct {
	ServerType  string            `json:"server_type"` // plex, emby, jellyfin
	ItemID      string            `json:"item_id"`     // Metadata ID for API calls
	Title       string            `json:"title"`
	Path        string            `json:"path"`
	ProviderIDs map[string]string `json:"provider_ids"` // tmdb, tvdb, imdb -> value
}

// GetProviderID returns the provider ID value for a given type
func (m *MediaServerItem) GetProviderID(idType string) string {
	if m.ProviderIDs == nil {
		return ""
	}
	return m.ProviderIDs[idType]
}

// Mismatch represents a detected mismatch between Arr and media server
type Mismatch struct {
	ArrInstance    *database.MatcharrArr
	Target         *database.Target
	ArrMedia       ArrMedia
	ServerItem     MediaServerItem
	ExpectedIDType string
	ExpectedID     string
	ActualID       string
}

// MissingPath captures an unmatched path between Arr and target
type MissingPath struct {
	ArrInstance *database.MatcharrArr
	Target      *database.Target
	ArrMedia    ArrMedia
	ServerItem  MediaServerItem
}

// RunResult contains the result of a matcharr comparison run
type RunResult struct {
	RunID           int64
	TotalCompared   int
	MismatchesFound int
	MismatchesFixed int
	Mismatches      []Mismatch
	Errors          []error
	Duration        time.Duration
}

// ManagerStatus represents the current status of the matcharr manager
type ManagerStatus struct {
	Running      bool       `json:"running"`
	Enabled      bool       `json:"enabled"`
	Schedule     string     `json:"schedule,omitempty"`
	AutoFix      bool       `json:"auto_fix"`
	LastRun      *time.Time `json:"last_run,omitempty"`
	NextRun      *time.Time `json:"next_run,omitempty"`
	CurrentRunID *int64     `json:"current_run_id,omitempty"`
	IsComparing  bool       `json:"is_comparing"`
}

// ManagerConfig holds configuration for the matcharr manager
type ManagerConfig struct {
	Enabled           bool          `json:"enabled"`
	Schedule          string        `json:"schedule"`            // Cron expression
	AutoFix           bool          `json:"auto_fix"`            // Auto-fix on scheduled runs
	DelayBetweenFixes time.Duration `json:"delay_between_fixes"` // Delay between fix operations
}

// DefaultConfig returns default manager configuration
func DefaultConfig() ManagerConfig {
	return ManagerConfig{
		Enabled:           false,
		Schedule:          "",
		AutoFix:           true,
		DelayBetweenFixes: 2 * time.Second,
	}
}

// intToString converts an int to string
func intToString(i int) string {
	return strconv.Itoa(i)
}

// RunLogger collects logs during a matcharr run
type RunLogger struct {
	mu      sync.Mutex
	entries []string
}

// NewRunLogger creates a new run logger
func NewRunLogger() *RunLogger {
	return &RunLogger{
		entries: make([]string, 0),
	}
}

// Log adds a log entry with timestamp
func (l *RunLogger) Log(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("15:04:05")
	entry := fmt.Sprintf("[%s] %s", timestamp, fmt.Sprintf(format, args...))
	l.entries = append(l.entries, entry)
}

// Info logs an info message
func (l *RunLogger) Info(format string, args ...any) {
	l.Log("INFO: "+format, args...)
}

// Debug logs a debug message
func (l *RunLogger) Debug(format string, args ...any) {
	l.Log("DEBUG: "+format, args...)
}

// Warn logs a warning message
func (l *RunLogger) Warn(format string, args ...any) {
	l.Log("WARN: "+format, args...)
}

// Error logs an error message
func (l *RunLogger) Error(format string, args ...any) {
	l.Log("ERROR: "+format, args...)
}

// String returns all log entries as a single string
func (l *RunLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.entries, "\n")
}

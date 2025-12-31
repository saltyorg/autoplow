package matcharr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// ArrClient is a client for Sonarr/Radarr API
type ArrClient struct {
	URL     string
	APIKey  string
	ArrType database.ArrType
	client  *http.Client
}

// NewArrClient creates a new Arr API client
func NewArrClient(arrURL, apiKey string, arrType database.ArrType) *ArrClient {
	return &ArrClient{
		URL:     strings.TrimSuffix(arrURL, "/"),
		APIKey:  apiKey,
		ArrType: arrType,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// TestConnection verifies the connection to the Arr instance
func (c *ArrClient) TestConnection(ctx context.Context) error {
	endpoint := "/api/v3/system/status"
	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetMedia fetches all media items from the Arr instance
func (c *ArrClient) GetMedia(ctx context.Context) ([]ArrMedia, error) {
	var endpoint string
	switch c.ArrType {
	case database.ArrTypeSonarr:
		endpoint = "/api/v3/series"
	case database.ArrTypeRadarr:
		endpoint = "/api/v3/movie"
	default:
		return nil, fmt.Errorf("unknown arr type: %s", c.ArrType)
	}

	log.Trace().
		Str("arr_type", string(c.ArrType)).
		Str("url", c.URL+endpoint).
		Msg("Fetching media from Arr")

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch media: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Trace().
		Str("arr_type", string(c.ArrType)).
		Int("status_code", resp.StatusCode).
		Int("body_length", len(body)).
		RawJSON("response_body", body).
		Msg("Arr media response")

	switch c.ArrType {
	case database.ArrTypeSonarr:
		return c.parseSonarrResponse(body)
	case database.ArrTypeRadarr:
		return c.parseRadarrResponse(body)
	}

	return nil, fmt.Errorf("unknown arr type: %s", c.ArrType)
}

// RootFolder represents a root folder from Arr
type RootFolder struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
}

// GetRootFolders fetches root folders from the Arr instance
func (c *ArrClient) GetRootFolders(ctx context.Context) ([]RootFolder, error) {
	endpoint := "/api/v3/rootfolder"

	log.Trace().
		Str("arr_type", string(c.ArrType)).
		Str("url", c.URL+endpoint).
		Msg("Fetching root folders from Arr")

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch root folders: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Trace().
		Str("arr_type", string(c.ArrType)).
		Int("status_code", resp.StatusCode).
		Int("body_length", len(body)).
		RawJSON("response_body", body).
		Msg("Arr root folders response")

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var folders []RootFolder
	if err := json.Unmarshal(body, &folders); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return folders, nil
}

// sonarrSeries represents a series from Sonarr API
type sonarrSeries struct {
	Title     string `json:"title"`
	Path      string `json:"path"`
	TvdbID    int    `json:"tvdbId"`
	ImdbID    string `json:"imdbId"`
	TitleSlug string `json:"titleSlug"`
}

// radarrMovie represents a movie from Radarr API
type radarrMovie struct {
	Title     string `json:"title"`
	Path      string `json:"path"`
	TmdbID    int    `json:"tmdbId"`
	ImdbID    string `json:"imdbId"`
	TitleSlug string `json:"titleSlug"`
}

func (c *ArrClient) parseSonarrResponse(body []byte) ([]ArrMedia, error) {
	var series []sonarrSeries
	if err := json.Unmarshal(body, &series); err != nil {
		return nil, fmt.Errorf("failed to parse sonarr response: %w", err)
	}

	media := make([]ArrMedia, 0, len(series))
	for _, s := range series {
		media = append(media, ArrMedia{
			Title:     s.Title,
			Path:      s.Path,
			TVDBID:    s.TvdbID,
			IMDBID:    s.ImdbID,
			TitleSlug: s.TitleSlug,
		})
	}

	return media, nil
}

func (c *ArrClient) parseRadarrResponse(body []byte) ([]ArrMedia, error) {
	var movies []radarrMovie
	if err := json.Unmarshal(body, &movies); err != nil {
		return nil, fmt.Errorf("failed to parse radarr response: %w", err)
	}

	media := make([]ArrMedia, 0, len(movies))
	for _, m := range movies {
		media = append(media, ArrMedia{
			Title:     m.Title,
			Path:      m.Path,
			TMDBID:    m.TmdbID,
			IMDBID:    m.ImdbID,
			TitleSlug: m.TitleSlug,
		})
	}

	return media, nil
}

func (c *ArrClient) doRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Response, error) {
	// Build URL with API key
	reqURL, err := url.Parse(c.URL + endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	q := reqURL.Query()
	q.Set("apikey", c.APIKey)
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	return c.client.Do(req)
}

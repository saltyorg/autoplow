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
	"github.com/saltyorg/autoplow/internal/httpclient"
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
		client:  httpclient.NewTraceClient(fmt.Sprintf("arr:%s", arrType), 60*time.Second),
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
	ID         int                     `json:"id"`
	Title      string                  `json:"title"`
	Path       string                  `json:"path"`
	TvdbID     int                     `json:"tvdbId"`
	TmdbID     int                     `json:"tmdbId"`
	ImdbID     string                  `json:"imdbId"`
	TitleSlug  string                  `json:"titleSlug"`
	Statistics *sonarrSeriesStatistics `json:"statistics"`
}

type sonarrSeriesStatistics struct {
	EpisodeFileCount int   `json:"episodeFileCount"`
	SizeOnDisk       int64 `json:"sizeOnDisk"`
}

// radarrMovie represents a movie from Radarr API
type radarrMovie struct {
	ID        int              `json:"id"`
	Title     string           `json:"title"`
	Path      string           `json:"path"`
	TmdbID    int              `json:"tmdbId"`
	ImdbID    string           `json:"imdbId"`
	TitleSlug string           `json:"titleSlug"`
	HasFile   bool             `json:"hasFile"`
	MovieFile *radarrMovieFile `json:"movieFile"`
}

type sonarrEpisode struct {
	SeasonNumber  int  `json:"seasonNumber"`
	EpisodeNumber int  `json:"episodeNumber"`
	EpisodeFileID int  `json:"episodeFileId"`
	HasFile       bool `json:"hasFile"`
}

type sonarrEpisodeFile struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
}

type radarrMovieFile struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
}

func (c *ArrClient) parseSonarrResponse(body []byte) ([]ArrMedia, error) {
	var series []sonarrSeries
	if err := json.Unmarshal(body, &series); err != nil {
		return nil, fmt.Errorf("failed to parse sonarr response: %w", err)
	}

	media := make([]ArrMedia, 0, len(series))
	for _, s := range series {
		hasFiles := false
		if s.Statistics != nil {
			hasFiles = s.Statistics.EpisodeFileCount > 0 || s.Statistics.SizeOnDisk > 0
		}

		media = append(media, ArrMedia{
			ID:        int64(s.ID),
			Title:     s.Title,
			Path:      s.Path,
			TVDBID:    s.TvdbID,
			TMDBID:    s.TmdbID,
			IMDBID:    s.ImdbID,
			TitleSlug: s.TitleSlug,
			HasFile:   hasFiles,
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
		movieFilePath := ""
		if m.MovieFile != nil {
			movieFilePath = strings.TrimSpace(m.MovieFile.Path)
		}

		media = append(media, ArrMedia{
			ID:            int64(m.ID),
			Title:         m.Title,
			Path:          m.Path,
			TMDBID:        m.TmdbID,
			IMDBID:        m.ImdbID,
			TitleSlug:     m.TitleSlug,
			HasFile:       m.HasFile,
			MovieFilePath: movieFilePath,
		})
	}

	return media, nil
}

// GetEpisodeFiles fetches episode file paths for a Sonarr series.
func (c *ArrClient) GetEpisodeFiles(ctx context.Context, seriesID int64) ([]ArrEpisodeFile, error) {
	if c.ArrType != database.ArrTypeSonarr {
		return nil, fmt.Errorf("episode files are only available for Sonarr")
	}

	episodesURL := fmt.Sprintf("/api/v3/episode?seriesId=%d", seriesID)
	episodeFilesURL := fmt.Sprintf("/api/v3/episodefile?seriesId=%d", seriesID)

	episodesResp, err := c.doRequest(ctx, "GET", episodesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch episodes: %w", err)
	}
	defer episodesResp.Body.Close()

	if episodesResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(episodesResp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", episodesResp.StatusCode, string(body))
	}

	episodesBody, err := io.ReadAll(episodesResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read episodes response: %w", err)
	}

	var episodes []sonarrEpisode
	if err := json.Unmarshal(episodesBody, &episodes); err != nil {
		return nil, fmt.Errorf("failed to decode episodes: %w", err)
	}

	filesResp, err := c.doRequest(ctx, "GET", episodeFilesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch episode files: %w", err)
	}
	defer filesResp.Body.Close()

	if filesResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(filesResp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", filesResp.StatusCode, string(body))
	}

	filesBody, err := io.ReadAll(filesResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read episode files response: %w", err)
	}

	var episodeFiles []sonarrEpisodeFile
	if err := json.Unmarshal(filesBody, &episodeFiles); err != nil {
		return nil, fmt.Errorf("failed to decode episode files: %w", err)
	}

	filePaths := make(map[int]string, len(episodeFiles))
	for _, file := range episodeFiles {
		if strings.TrimSpace(file.Path) == "" {
			continue
		}
		filePaths[file.ID] = file.Path
	}

	results := make([]ArrEpisodeFile, 0, len(episodes))
	for _, ep := range episodes {
		if !ep.HasFile || ep.EpisodeFileID == 0 {
			continue
		}
		path := filePaths[ep.EpisodeFileID]
		if strings.TrimSpace(path) == "" {
			continue
		}
		results = append(results, ArrEpisodeFile{
			SeasonNumber:  ep.SeasonNumber,
			EpisodeNumber: ep.EpisodeNumber,
			FilePath:      path,
		})
	}

	return results, nil
}

// GetMovieFiles fetches movie file paths for a Radarr movie.
func (c *ArrClient) GetMovieFiles(ctx context.Context, movieID int64) ([]ArrMovieFile, error) {
	if c.ArrType != database.ArrTypeRadarr {
		return nil, fmt.Errorf("movie files are only available for Radarr")
	}

	endpoint := fmt.Sprintf("/api/v3/moviefile?movieId=%d", movieID)
	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch movie files: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read movie files response: %w", err)
	}

	var files []radarrMovieFile
	if err := json.Unmarshal(body, &files); err != nil {
		return nil, fmt.Errorf("failed to decode movie files: %w", err)
	}

	results := make([]ArrMovieFile, 0, len(files))
	for _, file := range files {
		if strings.TrimSpace(file.Path) == "" {
			continue
		}
		results = append(results, ArrMovieFile{FilePath: file.Path})
	}

	return results, nil
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

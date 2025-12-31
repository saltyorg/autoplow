package targets

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/matcharr"
)

// PlexLibraryItem represents a Plex library item with GUIDs for matcharr
type PlexLibraryItem struct {
	RatingKey string   `json:"ratingKey"`
	Title     string   `json:"title"`
	Type      string   `json:"type"`
	GUID      string   `json:"guid"`  // Legacy agent format
	GUIDs     []string `json:"guids"` // Modern Plex agent format (array of guids)
	Media     []plexMedia
}

// plexLibraryItemsResponse represents the response from library/sections/{id}/all
type plexLibraryItemsResponse struct {
	MediaContainer plexLibraryItemsContainer `json:"MediaContainer"`
}

type plexLibraryItemsContainer struct {
	ViewGroup string                `json:"viewGroup"` // "show" or "movie"
	Metadata  []plexLibraryMetadata `json:"Metadata"`
}

type plexLibraryMetadata struct {
	RatingKey string         `json:"ratingKey"`
	Title     string         `json:"title"`
	Type      string         `json:"type"`
	GUID      string         `json:"guid"`
	GUIDs     []plexGUID     `json:"Guid"` // Note: Plex uses "Guid" not "guids"
	Media     []plexMedia    `json:"Media,omitempty"`
	Location  []plexLocation `json:"Location,omitempty"` // For TV shows - contains folder path
}

type plexGUID struct {
	ID string `json:"id"` // Format: "tmdb://12345" or "tvdb://12345" or "imdb://tt12345"
}

// GetLibraryItemsWithProviderIDs implements TargetFixer for Plex
// Returns all items in a library with their provider IDs
func (s *PlexTarget) GetLibraryItemsWithProviderIDs(ctx context.Context, libraryID string) ([]matcharr.MediaServerItem, error) {
	// Fetch all items from the library section
	// includeGuids=1 is required to get external IDs (TVDB, TMDB, IMDB)
	itemsURL := fmt.Sprintf("%s/library/sections/%s/all?includeGuids=1&includeDetails=1", s.dbTarget.URL, libraryID)

	req, err := http.NewRequestWithContext(ctx, "GET", itemsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	log.Trace().
		Str("target", s.Name()).
		Str("url", itemsURL).
		Str("library_id", libraryID).
		Msg("Fetching Plex library items")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read the full body for trace logging
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	log.Trace().
		Str("target", s.Name()).
		Str("library_id", libraryID).
		Int("status_code", resp.StatusCode).
		Int("body_length", len(body)).
		RawJSON("response_body", body).
		Msg("Plex library items response")

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
	}

	var itemsResp plexLibraryItemsResponse
	if err := json.Unmarshal(body, &itemsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	viewGroup := itemsResp.MediaContainer.ViewGroup
	isShowLibrary := viewGroup == "show"
	isMovieLibrary := viewGroup == "movie"

	// Only process movie and show libraries
	if !isShowLibrary && !isMovieLibrary {
		log.Debug().
			Str("target", s.Name()).
			Str("library_id", libraryID).
			Str("view_group", viewGroup).
			Msg("Skipping unsupported library type for matcharr")
		return nil, nil
	}

	// First pass: collect items and their rating keys
	items := make([]matcharr.MediaServerItem, 0, len(itemsResp.MediaContainer.Metadata))
	ratingKeys := make([]string, 0, len(itemsResp.MediaContainer.Metadata))

	for _, meta := range itemsResp.MediaContainer.Metadata {
		item := matcharr.MediaServerItem{
			ServerType:  "plex",
			ItemID:      meta.RatingKey,
			Title:       meta.Title,
			ProviderIDs: make(map[string]string),
		}

		// Extract path from media (movies)
		if len(meta.Media) > 0 && len(meta.Media[0].Part) > 0 {
			item.Path = meta.Media[0].Part[0].File
		} else if len(meta.Location) > 0 {
			item.Path = meta.Location[0].Path
		}

		// Parse provider IDs from GUIDs array (modern Plex agent)
		for _, guid := range meta.GUIDs {
			parseProviderID(guid.ID, item.ProviderIDs)
		}

		// Also try parsing legacy GUID if present
		if meta.GUID != "" {
			parseProviderID(meta.GUID, item.ProviderIDs)
		}

		items = append(items, item)
		ratingKeys = append(ratingKeys, meta.RatingKey)
	}

	// For show libraries, batch fetch locations since they're not included in the list response
	if isShowLibrary && len(ratingKeys) > 0 {
		locations, err := s.fetchBatchLocations(ctx, ratingKeys)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to fetch batch locations for shows")
		} else {
			for i := range items {
				if path, ok := locations[items[i].ItemID]; ok {
					items[i].Path = path
				}
			}
		}
	}

	// Log parsed items
	for _, item := range items {
		log.Trace().
			Str("target", s.Name()).
			Str("title", item.Title).
			Str("rating_key", item.ItemID).
			Str("path", item.Path).
			Interface("provider_ids", item.ProviderIDs).
			Msg("Parsed Plex item")
	}

	log.Debug().
		Str("target", s.Name()).
		Str("library_id", libraryID).
		Int("items", len(items)).
		Msg("Fetched library items with provider IDs")

	return items, nil
}

// parseProviderID parses a GUID string like "tmdb://12345" into the provider IDs map
func parseProviderID(guid string, providerIDs map[string]string) {
	// Handle formats like "tmdb://12345", "tvdb://12345", "imdb://tt12345"
	// Also handle legacy format like "com.plexapp.agents.themoviedb://12345?lang=en"

	guid = strings.TrimSpace(guid)
	if guid == "" {
		return
	}

	// Modern format: "provider://id"
	if after, ok := strings.CutPrefix(guid, "tmdb://"); ok {
		providerIDs["tmdb"] = after
		return
	}
	if after, ok := strings.CutPrefix(guid, "tvdb://"); ok {
		providerIDs["tvdb"] = after
		return
	}
	if after, ok := strings.CutPrefix(guid, "imdb://"); ok {
		providerIDs["imdb"] = after
		return
	}

	// Legacy format: "com.plexapp.agents.themoviedb://12345?lang=en"
	if strings.Contains(guid, "themoviedb://") {
		parts := strings.Split(guid, "themoviedb://")
		if len(parts) == 2 {
			id := strings.Split(parts[1], "?")[0]
			providerIDs["tmdb"] = id
		}
		return
	}
	if strings.Contains(guid, "thetvdb://") {
		parts := strings.Split(guid, "thetvdb://")
		if len(parts) == 2 {
			id := strings.Split(parts[1], "?")[0]
			providerIDs["tvdb"] = id
		}
		return
	}
	if strings.Contains(guid, "imdb://") {
		parts := strings.Split(guid, "imdb://")
		if len(parts) == 2 {
			id := strings.Split(parts[1], "?")[0]
			providerIDs["imdb"] = id
		}
		return
	}
}

// MatchItem updates a Plex item's metadata to match a specific provider ID
// This calls the Plex match API to reassign metadata
func (s *PlexTarget) MatchItem(ctx context.Context, itemID string, idType string, idValue string, title string) error {
	// Build the match URL
	matchURL := fmt.Sprintf("%s/library/metadata/%s/match", s.dbTarget.URL, itemID)

	// Build the GUID parameter based on ID type
	var guid string
	switch idType {
	case "tmdb":
		guid = fmt.Sprintf("tmdb://%s?lang=en", idValue)
	case "tvdb":
		guid = fmt.Sprintf("tvdb://%s?lang=en", idValue)
	case "imdb":
		guid = fmt.Sprintf("imdb://%s?lang=en", idValue)
	default:
		return fmt.Errorf("unsupported ID type: %s", idType)
	}

	params := url.Values{}
	params.Set("guid", guid)
	params.Set("name", title)

	req, err := http.NewRequestWithContext(ctx, "PUT", matchURL+"?"+params.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Plex-Token", s.dbTarget.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("plex match returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Info().
		Str("target", s.Name()).
		Str("item_id", itemID).
		Str("id_type", idType).
		Str("id_value", idValue).
		Str("title", title).
		Msg("Matched Plex item")

	return nil
}

// fetchBatchLocations fetches Location data for multiple items in batches
// Returns a map of ratingKey -> path
func (s *PlexTarget) fetchBatchLocations(ctx context.Context, ratingKeys []string) (map[string]string, error) {
	const batchSize = 100
	locations := make(map[string]string)

	for i := 0; i < len(ratingKeys); i += batchSize {
		end := min(i+batchSize, len(ratingKeys))
		batch := ratingKeys[i:end]

		// Fetch batch: /library/metadata/1,2,3,...
		metadataURL := fmt.Sprintf("%s/library/metadata/%s", s.dbTarget.URL, strings.Join(batch, ","))

		req, err := http.NewRequestWithContext(ctx, "GET", metadataURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("X-Plex-Token", s.dbTarget.Token)
		req.Header.Set("Accept", "application/json")

		log.Trace().
			Str("target", s.Name()).
			Int("batch_start", i).
			Int("batch_size", len(batch)).
			Msg("Fetching batch locations for shows")

		resp, err := s.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("plex returned status %d: %s", resp.StatusCode, string(body))
		}

		var metaResp plexLibraryItemsResponse
		if err := json.Unmarshal(body, &metaResp); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		for _, meta := range metaResp.MediaContainer.Metadata {
			if len(meta.Location) > 0 {
				locations[meta.RatingKey] = meta.Location[0].Path
			}
		}
	}

	log.Debug().
		Str("target", s.Name()).
		Int("total_items", len(ratingKeys)).
		Int("locations_found", len(locations)).
		Msg("Fetched batch locations for shows")

	return locations, nil
}

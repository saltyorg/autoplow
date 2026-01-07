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

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/matcharr"
)

// mediaBrowserItemWithProviders represents an item with provider IDs from Emby/Jellyfin
type mediaBrowserItemWithProviders struct {
	ID          string            `json:"Id"`
	Name        string            `json:"Name"`
	Path        string            `json:"Path"`
	Type        string            `json:"Type"`
	ProviderIDs map[string]string `json:"ProviderIds"`
}

// mediaBrowserItemsWithProvidersResponse represents the response from /Items endpoint
type mediaBrowserItemsWithProvidersResponse struct {
	Items            []mediaBrowserItemWithProviders `json:"Items"`
	TotalRecordCount int                             `json:"TotalRecordCount"`
}

// GetLibraryItemsWithProviderIDs implements TargetFixer for MediaBrowser (Emby/Jellyfin)
// Returns all items in a library with their provider IDs
func (s *MediaBrowserTarget) GetLibraryItemsWithProviderIDs(ctx context.Context, libraryID string) ([]matcharr.MediaServerItem, error) {
	baseURL := strings.TrimRight(s.dbTarget.URL, "/")
	limit := 500
	startIndex := 0
	var allItems []matcharr.MediaServerItem

	for {
		itemsURL, err := url.Parse(fmt.Sprintf("%s/Items", baseURL))
		if err != nil {
			return nil, fmt.Errorf("failed to parse URL: %w", err)
		}

		q := itemsURL.Query()
		q.Set("ParentId", libraryID)
		q.Set("Recursive", "true")
		q.Set("Fields", "Path,ProviderIds")
		q.Set("EnableImages", "false")
		q.Set("EnableTotalRecordCount", "true")
		q.Set("Limit", fmt.Sprintf("%d", limit))
		q.Set("StartIndex", fmt.Sprintf("%d", startIndex))
		// Filter to get movies and series (not episodes - we need series-level IDs for TV)
		q.Set("IncludeItemTypes", "Movie,Series")
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

		var itemsResp mediaBrowserItemsWithProvidersResponse
		if err := json.NewDecoder(resp.Body).Decode(&itemsResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		// Convert to matcharr items
		for _, item := range itemsResp.Items {
			msItem := matcharr.MediaServerItem{
				ServerType:  string(s.config.TargetType),
				ItemID:      item.ID,
				Title:       item.Name,
				Path:        item.Path,
				IsFile:      strings.EqualFold(item.Type, "movie"),
				ProviderIDs: make(map[string][]string),
			}

			// Copy and normalize provider IDs (Emby/Jellyfin use "Tmdb", "Tvdb", "Imdb")
			for key, value := range item.ProviderIDs {
				normalizedKey := strings.ToLower(key)
				msItem.ProviderIDs[normalizedKey] = append(msItem.ProviderIDs[normalizedKey], strings.TrimSpace(value))
			}

			allItems = append(allItems, msItem)
		}

		// Check if we've retrieved all items
		if len(itemsResp.Items) < limit {
			break
		}

		startIndex += limit

		// Safety check to avoid infinite loops
		if startIndex >= itemsResp.TotalRecordCount {
			break
		}
	}

	log.Debug().
		Str("target", s.Name()).
		Str("library_id", libraryID).
		Int("items", len(allItems)).
		Msg("Fetched library items with provider IDs")

	return allItems, nil
}

// mediaBrowserItemUpdate represents the update payload for changing provider IDs
// Jellyfin requires additional fields (Genres, Tags, LockData, LockedFields) to be present
type mediaBrowserItemUpdate struct {
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	Genres       []string          `json:"Genres"`
	Tags         []string          `json:"Tags"`
	LockData     bool              `json:"LockData"`
	LockedFields []string          `json:"LockedFields"`
	ProviderIDs  map[string]string `json:"ProviderIds"`
}

// MatchItem updates an Emby/Jellyfin item's provider IDs and refreshes metadata
func (s *MediaBrowserTarget) MatchItem(ctx context.Context, itemID string, idType string, idValue string, title string) error {
	baseURL := strings.TrimRight(s.dbTarget.URL, "/")

	// Build the update payload
	// Note: Emby/Jellyfin expect capitalized provider keys (Tmdb, Tvdb, Imdb)
	providerKey := capitalizeProviderKey(idType)

	update := mediaBrowserItemUpdate{
		ID:           itemID,
		Name:         title,
		Genres:       []string{},
		Tags:         []string{},
		LockData:     false,
		LockedFields: []string{},
		ProviderIDs: map[string]string{
			providerKey: idValue,
		},
	}

	// Clear conflicting provider IDs
	switch idType {
	case "tmdb":
		update.ProviderIDs["Tvdb"] = ""
	case "tvdb":
		update.ProviderIDs["Tmdb"] = ""
	}

	bodyJSON, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal update: %w", err)
	}

	// Update the item
	updateURL := fmt.Sprintf("%s/Items/%s", baseURL, itemID)
	req, err := http.NewRequestWithContext(ctx, "POST", updateURL, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("failed to create update request: %w", err)
	}

	s.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("update request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s update returned status %d: %s", s.config.ServerName, resp.StatusCode, string(body))
	}

	// Refresh the item's metadata
	if err := s.refreshItemForMatcharr(ctx, itemID); err != nil {
		log.Warn().
			Err(err).
			Str("target", s.Name()).
			Str("item_id", itemID).
			Msg("Failed to refresh item after match")
		// Don't return error - the match itself succeeded
	}

	log.Info().
		Str("target", s.Name()).
		Str("item_id", itemID).
		Str("id_type", idType).
		Str("id_value", idValue).
		Str("title", title).
		Msgf("Matched %s item", s.config.ServerName)

	return nil
}

// refreshItemForMatcharr refreshes an item's metadata after updating provider IDs
func (s *MediaBrowserTarget) refreshItemForMatcharr(ctx context.Context, itemID string) error {
	baseURL := strings.TrimRight(s.dbTarget.URL, "/")

	refreshURL, err := url.Parse(fmt.Sprintf("%s/Items/%s/Refresh", baseURL, itemID))
	if err != nil {
		return fmt.Errorf("failed to parse URL: %w", err)
	}

	q := refreshURL.Query()
	q.Set("MetadataRefreshMode", "FullRefresh")
	q.Set("ImageRefreshMode", "FullRefresh")
	q.Set("ReplaceAllMetadata", "true")
	q.Set("ReplaceAllImages", "true")
	refreshURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", refreshURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create refresh request: %w", err)
	}

	s.setHeaders(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("refresh request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s refresh returned status %d: %s", s.config.ServerName, resp.StatusCode, string(body))
	}

	return nil
}

// capitalizeProviderKey converts a provider key to the format expected by Emby/Jellyfin
func capitalizeProviderKey(key string) string {
	switch strings.ToLower(key) {
	case "tmdb":
		return "Tmdb"
	case "tvdb":
		return "Tvdb"
	case "imdb":
		return "Imdb"
	default:
		// Capitalize first letter
		if len(key) == 0 {
			return key
		}
		return strings.ToUpper(key[:1]) + key[1:]
	}
}

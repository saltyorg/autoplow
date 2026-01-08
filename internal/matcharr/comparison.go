package matcharr

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// CompareResult holds the result of comparing an Arr instance to a target
type CompareResult struct {
	ArrID      int64
	ArrName    string
	TargetID   int64
	TargetName string
	Compared   int
	Mismatches []Mismatch
	MissingArr []MissingPath // Present in Arr, missing on server
	MissingSrv []MissingPath // Present on server, missing in Arr
	Matches    []MatchedMedia
	Errors     []error
}

// MatchedMedia represents an Arr item matched to a media server item.
type MatchedMedia struct {
	ArrMedia   ArrMedia
	ServerItem MediaServerItem
	IDType     string
	IDValue    string
}

// CompareArrToTarget compares media from an Arr instance to items in a media server target
func CompareArrToTarget(
	ctx context.Context,
	arr *database.MatcharrArr,
	arrMedia []ArrMedia,
	target *database.Target,
	targetItems []MediaServerItem,
) *CompareResult {
	result := &CompareResult{
		ArrID:      arr.ID,
		ArrName:    arr.Name,
		TargetID:   target.ID,
		TargetName: target.Name,
	}

	// Build a map of target items by their mapped paths for faster lookup
	targetByPath := make(map[string]*MediaServerItem)
	for i := range targetItems {
		item := &targetItems[i]
		matchPath := item.MatchPath()
		if matchPath == "" {
			continue
		}
		targetByPath[matchPath] = item
	}
	matchedTargetPaths := make(map[string]struct{})

	log.Debug().
		Str("arr", arr.Name).
		Str("target", target.Name).
		Int("arr_items", len(arrMedia)).
		Int("target_items", len(targetItems)).
		Msg("Starting comparison")

	for _, media := range arrMedia {
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, ctx.Err())
			return result
		default:
		}

		// Map the Arr path to the media server path
		mappedPath := mapPath(media.Path, arr.PathMappings)
		normalizedPath := normalizePath(mappedPath)

		// Try to find matching target item
		targetItem := findMatchingTargetItem(normalizedPath, targetByPath)
		if targetItem == nil {
			if !media.HasFile {
				log.Trace().
					Str("arr", arr.Name).
					Str("title", media.Title).
					Str("path", mappedPath).
					Msg("Skipping missing check for Arr item without files")
				continue
			}

			log.Trace().
				Str("arr", arr.Name).
				Str("title", media.Title).
				Str("path", mappedPath).
				Str("normalized_path", normalizedPath).
				Msg("No matching target item found")

			result.MissingArr = append(result.MissingArr, MissingPath{
				ArrInstance: arr,
				Target:      target,
				ArrMedia:    media,
			})
			continue
		}

		matchedTargetPaths[targetItem.MatchPath()] = struct{}{}
		result.Compared++

		// Get expected ID from Arr
		expectedIDType, expectedID := media.GetPrimaryID(arr.Type)
		if expectedID == "" {
			log.Trace().
				Str("arr", arr.Name).
				Str("title", media.Title).
				Msg("No primary ID available from Arr")
			continue
		}

		// Get actual ID from target
		actualIDs := targetItem.GetProviderIDs(expectedIDType)
		hasExpectedID := containsID(actualIDs, expectedID)

		log.Trace().
			Str("arr", arr.Name).
			Str("target", target.Name).
			Str("title", media.Title).
			Str("expected_id_type", expectedIDType).
			Str("expected_id", expectedID).
			Interface("server_provider_ids", targetItem.ProviderIDs).
			Msg("Comparing media IDs")

		matched := hasExpectedID

		// Radarr movies: if TMDB is missing on the server, allow imdb as a fallback
		if arr.Type == database.ArrTypeRadarr && expectedIDType == "tmdb" && len(actualIDs) == 0 {
			imdbID := strings.TrimSpace(media.IMDBID)
			if imdbID != "" {
				fallbackIDs := targetItem.GetProviderIDs("imdb")
				if containsID(fallbackIDs, imdbID) {
					log.Debug().
						Str("arr", arr.Name).
						Str("target", target.Name).
						Str("title", media.Title).
						Str("fallback_id_type", "imdb").
						Str("fallback_expected", imdbID).
						Interface("server_provider_ids", targetItem.ProviderIDs).
						Msg("Using fallback provider match for movie with missing TMDB ID")
					matched = true
				}
			} else {
				log.Trace().
					Str("arr", arr.Name).
					Str("title", media.Title).
					Msg("Skipping fallback match because Arr item has no IMDB ID")
			}
		}

		// Sonarr shows: if TVDB doesn't match, allow imdb first, then tmdb as fallbacks
		if arr.Type == database.ArrTypeSonarr && expectedIDType == "tvdb" && !matched {
			if imdbID := strings.TrimSpace(media.IMDBID); imdbID != "" {
				if fallbackIDs := targetItem.GetProviderIDs("imdb"); containsID(fallbackIDs, imdbID) {
					log.Debug().
						Str("arr", arr.Name).
						Str("target", target.Name).
						Str("title", media.Title).
						Str("fallback_id_type", "imdb").
						Str("fallback_expected", imdbID).
						Interface("server_provider_ids", targetItem.ProviderIDs).
						Msg("Using fallback provider match for show with mismatched TVDB ID")
					matched = true
				}
			}

			if !matched && media.TMDBID > 0 {
				tmdbID := intToString(media.TMDBID)
				if fallbackIDs := targetItem.GetProviderIDs("tmdb"); containsID(fallbackIDs, tmdbID) {
					log.Debug().
						Str("arr", arr.Name).
						Str("target", target.Name).
						Str("title", media.Title).
						Str("fallback_id_type", "tmdb").
						Str("fallback_expected", tmdbID).
						Interface("server_provider_ids", targetItem.ProviderIDs).
						Msg("Using fallback provider match for show with mismatched TVDB ID")
					matched = true
				}
			}
		}

		// Compare IDs
		if !matched {
			actualID := strings.Join(actualIDs, ",")
			log.Debug().
				Str("arr", arr.Name).
				Str("target", target.Name).
				Str("title", media.Title).
				Str("arr_path", media.Path).
				Str("mapped_path", normalizedPath).
				Str("server_path", targetItem.Path).
				Str("id_type", expectedIDType).
				Str("expected", expectedID).
				Str("actual", actualID).
				Interface("server_provider_ids", targetItem.ProviderIDs).
				Msg("Mismatch detected")

			result.Mismatches = append(result.Mismatches, Mismatch{
				ArrInstance:    arr,
				Target:         target,
				ArrMedia:       media,
				ServerItem:     *targetItem,
				ExpectedIDType: expectedIDType,
				ExpectedID:     expectedID,
				ActualID:       actualID,
			})
		} else {
			result.Matches = append(result.Matches, MatchedMedia{
				ArrMedia:   media,
				ServerItem: *targetItem,
				IDType:     expectedIDType,
				IDValue:    expectedID,
			})
		}
	}

	result.MissingSrv = findMissingTargetItems(arr, target, targetByPath, matchedTargetPaths)

	return result
}

// findMissingTargetItems returns items not matched during comparison
func findMissingTargetItems(arr *database.MatcharrArr, target *database.Target, targetByPath map[string]*MediaServerItem, matched map[string]struct{}) []MissingPath {
	var missing []MissingPath
	for path, item := range targetByPath {
		if _, ok := matched[path]; ok {
			continue
		}
		missing = append(missing, MissingPath{
			ArrInstance: arr,
			Target:      target,
			ServerItem:  *item,
		})
	}
	return missing
}

// findMatchingTargetItem finds a target item that matches the given path
func findMatchingTargetItem(path string, targetByPath map[string]*MediaServerItem) *MediaServerItem {
	return targetByPath[path]
}

// mapPath applies path mappings to convert an Arr path to a media server path
func mapPath(path string, mappings []database.MatcharrPathMapping) string {
	if len(mappings) == 0 {
		return normalizePath(path)
	}

	normalizedPath := normalizePath(path)

	for _, mapping := range mappings {
		arrPrefix := normalizePath(mapping.ArrPath)
		serverPrefix := normalizePath(mapping.ServerPath)

		if arrPrefix == "" {
			continue
		}

		if normalizedPath == arrPrefix {
			return serverPrefix
		}

		// Require a path boundary so we don't map /mnt/media to /mnt/mediabackup
		if strings.HasPrefix(normalizedPath, arrPrefix+"/") {
			return serverPrefix + normalizedPath[len(arrPrefix):]
		}
	}

	return normalizedPath
}

// normalizePath normalizes a path for comparison
func normalizePath(path string) string {
	return strings.TrimRight(path, "/")
}

// containsID checks whether the expected ID exists in the list of provider IDs
func containsID(ids []string, expected string) bool {
	expected = strings.TrimSpace(expected)
	for _, id := range ids {
		if strings.TrimSpace(id) == expected {
			return true
		}
	}
	return false
}

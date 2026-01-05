package matcharr

import (
	"context"
	"path/filepath"
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
	Errors     []error
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
		matchPath := itemMatchPath(item.Path)
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

		matchedTargetPaths[itemMatchPath(targetItem.Path)] = struct{}{}
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
		actualID := targetItem.GetProviderID(expectedIDType)

		// Compare IDs
		if actualID != expectedID {
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

// itemMatchPath returns the directory path to compare against Arr paths.
// If the media server path is a file, use its parent directory; otherwise use the path as-is.
func itemMatchPath(path string) string {
	normalized := normalizePath(path)
	if normalized == "" {
		return ""
	}

	base := filepath.Base(normalized)
	// Treat a path with a media file extension as a file path and use its directory.
	// Some series folders contain dots (e.g., "A.P. Bio"), so we only treat it as
	// a file when the extension looks like an actual media file.
	if looksLikeMediaFile(base) {
		return normalizePath(filepath.Dir(normalized))
	}

	return normalized
}

// looksLikeMediaFile returns true if the base name has a common media file extension.
func looksLikeMediaFile(base string) bool {
	ext := filepath.Ext(base)
	if ext == "" {
		return false
	}

	// Ignore "extensions" that contain spaces or are unusually long (likely folder names)
	ext = strings.TrimPrefix(ext, ".")
	if ext == "" || len(ext) > 6 || strings.Contains(ext, " ") {
		return false
	}

	// Only consider simple alphanumeric extensions (e.g., mkv, mp4, iso)
	for _, r := range ext {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
			return false
		}
	}

	return true
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

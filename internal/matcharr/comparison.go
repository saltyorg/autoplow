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
			continue
		}

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

	return result
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
	// Treat a path with an extension as a file path and use its directory
	if strings.Contains(base, ".") {
		return normalizePath(filepath.Dir(normalized))
	}

	return normalized
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

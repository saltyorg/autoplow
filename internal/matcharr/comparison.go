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
		normalizedPath := normalizePath(item.Path)
		targetByPath[normalizedPath] = item
		// Also store by directory path for matching
		dirPath := filepath.Dir(normalizedPath)
		if _, exists := targetByPath[dirPath]; !exists {
			targetByPath[dirPath] = item
		}
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
	// Try exact match first
	if item, ok := targetByPath[path]; ok {
		return item
	}

	// Try directory match (for TV shows where Arr path is the series folder)
	// The target item path might be an episode file within that folder
	for targetPath, item := range targetByPath {
		// Check if the target item's path starts with the Arr path
		if strings.HasPrefix(targetPath, path+"/") {
			return item
		}
		// Check if the Arr path starts with the target item's directory
		targetDir := filepath.Dir(targetPath)
		if path == targetDir || strings.HasPrefix(path, targetDir+"/") {
			return item
		}
	}

	return nil
}

// mapPath applies path mappings to convert an Arr path to a media server path
func mapPath(path string, mappings []database.MatcharrPathMapping) string {
	if len(mappings) == 0 {
		return path
	}

	for _, mapping := range mappings {
		if strings.HasPrefix(path, mapping.ArrPath) {
			return mapping.ServerPath + path[len(mapping.ArrPath):]
		}
	}

	return path
}

// normalizePath normalizes a path for comparison
func normalizePath(path string) string {
	// Trim trailing slashes
	path = strings.TrimRight(path, "/")
	// Convert backslashes to forward slashes for cross-platform compatibility
	path = strings.ReplaceAll(path, "\\", "/")
	return path
}

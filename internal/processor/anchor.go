package processor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// AnchorConfig holds configuration for mount readiness checking
type AnchorConfig struct {
	// Enabled controls whether anchor checking is performed
	Enabled bool `json:"enabled"`

	// AnchorFiles is the list of default anchor files - all must exist for paths to be considered ready
	AnchorFiles []string `json:"anchor_files,omitempty"`

	// PathAnchorFiles maps specific paths to their anchor files (path prefix -> absolute anchor file path)
	// This allows different containers/hosts with different mappings to use different anchor files
	PathAnchorFiles map[string]string `json:"path_anchor_files,omitempty"`
}

// DefaultAnchorConfig returns the default anchor configuration
func DefaultAnchorConfig() AnchorConfig {
	return AnchorConfig{
		Enabled:         true,
		PathAnchorFiles: make(map[string]string),
	}
}

// GetAnchorFilesForPath returns the appropriate anchor files for a given path.
// Default anchor files always apply; path-specific anchor files only apply to matching prefixes.
func (ac *AnchorConfig) GetAnchorFilesForPath(path string) []string {
	anchors := make([]string, 0, len(ac.AnchorFiles)+1)
	for _, anchor := range ac.AnchorFiles {
		anchor = strings.TrimSpace(anchor)
		if anchor != "" {
			anchors = append(anchors, anchor)
		}
	}

	if len(ac.PathAnchorFiles) == 0 {
		return anchors
	}

	// Find the most specific (longest) matching path prefix
	normalizedPath := filepath.Clean(path)
	var bestMatch string
	var bestAnchor string
	sep := string(filepath.Separator)
	for pathPrefix, anchorFile := range ac.PathAnchorFiles {
		pathPrefix = filepath.Clean(strings.TrimSpace(pathPrefix))
		if pathPrefix == "." || pathPrefix == "" {
			continue
		}
		if normalizedPath == pathPrefix || strings.HasPrefix(normalizedPath, pathPrefix+sep) {
			if len(pathPrefix) > len(bestMatch) {
				bestMatch = pathPrefix
				bestAnchor = strings.TrimSpace(anchorFile)
			}
		}
	}

	if bestMatch != "" && bestAnchor != "" {
		anchors = append(anchors, bestAnchor)
	}

	return anchors
}

// AnchorChecker checks if mounts/paths are available for scanning
type AnchorChecker struct {
	config AnchorConfig
}

// NewAnchorChecker creates a new anchor checker
func NewAnchorChecker(config AnchorConfig) *AnchorChecker {
	return &AnchorChecker{
		config: config,
	}
}

// IsReady checks if a path's mount is ready (all required anchor files exist)
func (ac *AnchorChecker) IsReady(path string) (bool, string) {
	if !ac.config.Enabled {
		return true, ""
	}

	// Get the appropriate anchor files for this path
	anchorFiles := ac.config.GetAnchorFilesForPath(path)

	// If no anchor files configured, consider ready
	if len(anchorFiles) == 0 {
		return true, ""
	}

	missing := make([]string, 0, len(anchorFiles))
	invalid := make([]string, 0, len(anchorFiles))
	seen := make(map[string]struct{}, len(anchorFiles))
	for _, anchorFile := range anchorFiles {
		anchorFile = strings.TrimSpace(anchorFile)
		if anchorFile == "" {
			continue
		}
		if _, ok := seen[anchorFile]; ok {
			continue
		}
		seen[anchorFile] = struct{}{}
		if !filepath.IsAbs(anchorFile) {
			invalid = append(invalid, anchorFile)
			continue
		}
		if _, err := os.Stat(anchorFile); err != nil {
			missing = append(missing, anchorFile)
		}
	}

	if len(invalid) > 0 {
		return false, fmt.Sprintf("anchor file paths must be absolute: %s", strings.Join(invalid, ", "))
	}
	if len(missing) > 0 {
		return false, fmt.Sprintf("missing anchor files (required): %s", strings.Join(missing, ", "))
	}

	return true, ""
}

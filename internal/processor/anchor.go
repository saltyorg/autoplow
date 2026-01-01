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

	// AnchorFiles is the list of default anchor files - at least one must exist for paths to be considered ready
	AnchorFiles []string `json:"anchor_files,omitempty"`

	// PathAnchorFiles maps specific paths to their anchor files (path prefix -> anchor filename)
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

// GetAnchorFilesForPath returns the appropriate anchor files for a given path
// It checks PathAnchorFiles for a matching prefix, falling back to the default AnchorFiles
func (ac *AnchorConfig) GetAnchorFilesForPath(path string) []string {
	if len(ac.PathAnchorFiles) == 0 {
		return ac.AnchorFiles
	}

	// Find the most specific (longest) matching path prefix
	var bestMatch string
	var bestAnchor string
	for pathPrefix, anchorFile := range ac.PathAnchorFiles {
		if strings.HasPrefix(path, pathPrefix) && len(pathPrefix) > len(bestMatch) {
			bestMatch = pathPrefix
			bestAnchor = anchorFile
		}
	}

	if bestMatch != "" {
		return []string{bestAnchor}
	}
	return ac.AnchorFiles
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

// IsReady checks if a path's mount is ready (at least one anchor file exists)
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

	// Check if at least one anchor file exists
	baseDir := filepath.Dir(path)
	for _, anchorFile := range anchorFiles {
		if anchorFile == "" {
			continue
		}
		anchorPath := filepath.Join(baseDir, anchorFile)
		if _, err := os.Stat(anchorPath); err == nil {
			return true, ""
		}
	}

	return false, fmt.Sprintf("no anchor file found (checked: %s)", strings.Join(anchorFiles, ", "))
}

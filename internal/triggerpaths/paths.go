package triggerpaths

import (
	"path/filepath"

	"github.com/saltyorg/autoplow/internal/database"
)

// ApplyPathRewrites applies trigger path rewrite rules to a list of paths.
func ApplyPathRewrites(paths []string, rewrites []database.PathRewrite) []string {
	if len(rewrites) == 0 {
		return paths
	}

	result := make([]string, len(paths))
	for i, path := range paths {
		result[i] = rewritePath(path, rewrites)
	}
	return result
}

// FilterPaths applies trigger include/exclude rules to a list of paths.
func FilterPaths(paths []string, config *database.TriggerConfig) []string {
	if config == nil {
		return paths
	}
	if len(config.IncludePaths) == 0 && len(config.ExcludePaths) == 0 && len(config.ExcludeExtensions) == 0 && config.AdvancedFilters == nil {
		return paths
	}

	var result []string
	for _, path := range paths {
		cleaned := filepath.Clean(path)
		if config.MatchesPathFilters(cleaned) {
			result = append(result, cleaned)
		}
	}
	return result
}

// rewritePath applies rewrite rules to a single path.
func rewritePath(path string, rewrites []database.PathRewrite) string {
	result := path
	for i := range rewrites {
		newPath := rewrites[i].Rewrite(result)
		if newPath != result {
			result = newPath
			if !rewrites[i].IsRegex {
				break
			}
		}
	}
	return result
}

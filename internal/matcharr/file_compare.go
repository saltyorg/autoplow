package matcharr

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/saltyorg/autoplow/internal/database"
)

type fileMismatchDetail struct {
	SeasonNumber    int
	EpisodeNumber   int
	ArrFileName     string
	ArrFilePath     string
	TargetFileNames []string
	TargetFilePaths []string
}

type targetFileInfo struct {
	names []string
	paths []string
}

func seasonEpisodeKey(season, episode int) string {
	return fmt.Sprintf("%d:%d", season, episode)
}

func fileBaseName(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	if idx := strings.LastIndexAny(trimmed, `/\`); idx >= 0 && idx+1 < len(trimmed) {
		return trimmed[idx+1:]
	}
	return trimmed
}

func fileNameEqual(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	return strings.EqualFold(left, right)
}

func fileIgnoreKey(arrType database.ArrType, arrMediaID int64, targetID int64, season, episode int, fileName string) string {
	return fmt.Sprintf("%s:%d:%d:%d:%d:%s", arrType, targetID, arrMediaID, season, episode, strings.ToLower(strings.TrimSpace(fileName)))
}

func addUnique(list []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return list
	}
	for _, existing := range list {
		if strings.EqualFold(existing, value) {
			return list
		}
	}
	return append(list, value)
}

func buildTargetEpisodeIndex(files []TargetEpisodeFile) map[string]targetFileInfo {
	index := make(map[string]targetFileInfo)
	for _, file := range files {
		name := fileBaseName(file.FilePath)
		if name == "" {
			continue
		}
		key := seasonEpisodeKey(file.SeasonNumber, file.EpisodeNumber)
		entry := index[key]
		entry.names = addUnique(entry.names, name)
		entry.paths = addUnique(entry.paths, file.FilePath)
		index[key] = entry
	}
	return index
}

func targetItemPathFromItem(item MediaServerItem) string {
	path := strings.TrimSpace(item.Path)
	if path == "" {
		return ""
	}
	if item.IsFile {
		return filepath.Dir(path)
	}
	return path
}

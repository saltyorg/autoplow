package matcharr

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

func compareEpisodeFiles(arrFiles []ArrEpisodeFile, targetFiles []TargetEpisodeFile) []fileMismatchDetail {
	targetIndex := buildTargetEpisodeIndex(targetFiles)
	fileEpisodes := buildArrFileEpisodeIndex(arrFiles)
	mismatches := make([]fileMismatchDetail, 0)

	for _, arrFile := range arrFiles {
		arrName := fileBaseName(arrFile.FilePath)
		if arrName == "" {
			continue
		}
		multiEpisodeLabel := formatMultiEpisodeLabel(fileEpisodes[arrFile.FilePath])
		key := seasonEpisodeKey(arrFile.SeasonNumber, arrFile.EpisodeNumber)
		targetEntry, ok := targetIndex[key]
		if !ok || len(targetEntry.names) == 0 {
			mismatches = append(mismatches, fileMismatchDetail{
				SeasonNumber:    arrFile.SeasonNumber,
				EpisodeNumber:   arrFile.EpisodeNumber,
				ArrFileName:     arrName,
				ArrFilePath:     arrFile.FilePath,
				TargetFileNames: nil,
				TargetFilePaths: nil,
				MultiEpisode:    multiEpisodeLabel,
			})
			continue
		}

		matched := false
		for _, candidate := range targetEntry.names {
			if fileNameEqual(arrName, candidate) {
				matched = true
				break
			}
		}
		if matched {
			continue
		}

		mismatches = append(mismatches, fileMismatchDetail{
			SeasonNumber:    arrFile.SeasonNumber,
			EpisodeNumber:   arrFile.EpisodeNumber,
			ArrFileName:     arrName,
			ArrFilePath:     arrFile.FilePath,
			TargetFileNames: targetEntry.names,
			TargetFilePaths: targetEntry.paths,
			MultiEpisode:    multiEpisodeLabel,
		})
	}

	return mismatches
}

func compareMovieFiles(arrFiles []ArrMovieFile, targetFiles []TargetMovieFile) []fileMismatchDetail {
	targetNames := make([]string, 0, len(targetFiles))
	targetPaths := make([]string, 0, len(targetFiles))
	for _, file := range targetFiles {
		name := fileBaseName(file.FilePath)
		if name == "" {
			continue
		}
		targetNames = addUnique(targetNames, name)
		targetPaths = addUnique(targetPaths, file.FilePath)
	}

	mismatches := make([]fileMismatchDetail, 0)
	for _, arrFile := range arrFiles {
		arrName := fileBaseName(arrFile.FilePath)
		if arrName == "" {
			continue
		}
		matched := false
		for _, candidate := range targetNames {
			if fileNameEqual(arrName, candidate) {
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		mismatches = append(mismatches, fileMismatchDetail{
			ArrFileName:     arrName,
			ArrFilePath:     arrFile.FilePath,
			TargetFileNames: targetNames,
			TargetFilePaths: targetPaths,
		})
	}

	return mismatches
}

func mismatchDetailKey(season, episode int, fileName string) string {
	return fmt.Sprintf("%d:%d:%s", season, episode, strings.ToLower(strings.TrimSpace(fileName)))
}

const defaultFileConcurrency = 4

func normalizeFileConcurrency(value int) int {
	if value < 1 {
		return defaultFileConcurrency
	}
	return value
}

func fileConcurrencyLimit(arr *database.MatcharrArr, target *database.Target) int {
	arrLimit := normalizeFileConcurrency(arr.FileConcurrency)
	targetLimit := normalizeFileConcurrency(target.Config.MatcharrFileConcurrency)
	if arrLimit < targetLimit {
		return arrLimit
	}
	return targetLimit
}

func (m *Manager) compareFileMismatches(
	ctx context.Context,
	runID int64,
	arr *database.MatcharrArr,
	arrClient *ArrClient,
	target *database.Target,
	fileFetcher TargetFileFetcher,
	matches []MatchedMedia,
	targetLimiter chan struct{},
	ignoreSet map[string]struct{},
	runLog *RunLogger,
) {
	if fileFetcher == nil {
		runLog.Debug("Target %s does not support file mismatch checks", target.Name)
		return
	}
	if len(matches) == 0 {
		return
	}

	workerCount := min(fileConcurrencyLimit(arr, target), len(matches))
	if workerCount < 1 {
		workerCount = 1
	}

	var episodeCache sync.Map
	var movieCache sync.Map
	var targetEpisodeCache sync.Map
	var targetMovieCache sync.Map

	workCh := make(chan MatchedMedia)
	var wg sync.WaitGroup

	acquireLimiter := func() bool {
		if targetLimiter == nil {
			return true
		}
		select {
		case targetLimiter <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}

	releaseLimiter := func() {
		if targetLimiter == nil {
			return
		}
		<-targetLimiter
	}

	processMatch := func(match MatchedMedia) {
		if !acquireLimiter() {
			return
		}
		defer releaseLimiter()

		if match.ArrMedia.ID == 0 || match.ServerItem.ItemID == "" {
			return
		}
		if !match.ArrMedia.HasFile {
			return
		}

		switch arr.Type {
		case database.ArrTypeSonarr:
			arrFiles, ok := episodeCache.Load(match.ArrMedia.ID)
			if !ok {
				files, err := arrClient.GetEpisodeFiles(ctx, match.ArrMedia.ID)
				if err != nil {
					runLog.Warn("Failed to fetch episode files for %s: %v", match.ArrMedia.Title, err)
					return
				}
				episodeCache.Store(match.ArrMedia.ID, files)
				arrFiles = files
			}

			targetFiles, ok := targetEpisodeCache.Load(match.ServerItem.ItemID)
			if !ok {
				files, err := fileFetcher.GetEpisodeFiles(ctx, match.ServerItem.ItemID)
				if err != nil {
					runLog.Warn("Failed to fetch episode files from %s for %s: %v", target.Name, match.ArrMedia.Title, err)
					return
				}
				targetEpisodeCache.Store(match.ServerItem.ItemID, files)
				targetFiles = files
			}

			mismatches := compareEpisodeFiles(arrFiles.([]ArrEpisodeFile), targetFiles.([]TargetEpisodeFile))
			for _, mismatch := range mismatches {
				ignoreKey := fileIgnoreKey(arr.Type, match.ArrMedia.ID, target.ID, mismatch.SeasonNumber, mismatch.EpisodeNumber, mismatch.ArrFileName)
				if _, ignored := ignoreSet[ignoreKey]; ignored {
					continue
				}

				targetItemPath := targetItemPathFromItem(match.ServerItem)

				record := &database.MatcharrFileMismatch{
					RunID:            runID,
					ArrID:            arr.ID,
					TargetID:         target.ID,
					ArrType:          arr.Type,
					ArrName:          arr.Name,
					TargetName:       target.Name,
					MediaTitle:       match.ArrMedia.Title,
					ArrMediaID:       match.ArrMedia.ID,
					TargetMetadataID: match.ServerItem.ItemID,
					SeasonNumber:     mismatch.SeasonNumber,
					EpisodeNumber:    mismatch.EpisodeNumber,
					ArrFileName:      mismatch.ArrFileName,
					TargetFileNames:  strings.Join(mismatch.TargetFileNames, ", "),
					ArrFilePath:      mismatch.ArrFilePath,
					TargetItemPath:   targetItemPath,
					TargetFilePaths:  strings.Join(mismatch.TargetFilePaths, ", "),
					MultiEpisode:     mismatch.MultiEpisode,
				}

				if err := m.db.CreateMatcharrFileMismatch(record); err != nil {
					runLog.Warn("Failed to save file mismatch for %s: %v", match.ArrMedia.Title, err)
				}
			}

		case database.ArrTypeRadarr:
			arrFiles, ok := movieCache.Load(match.ArrMedia.ID)
			if !ok {
				var files []ArrMovieFile
				if match.ArrMedia.MovieFilePath != "" {
					files = []ArrMovieFile{{FilePath: match.ArrMedia.MovieFilePath}}
				} else {
					fetched, err := arrClient.GetMovieFiles(ctx, match.ArrMedia.ID)
					if err != nil {
						runLog.Warn("Failed to fetch movie files for %s: %v", match.ArrMedia.Title, err)
						return
					}
					files = fetched
				}
				movieCache.Store(match.ArrMedia.ID, files)
				arrFiles = files
			}

			targetFiles, ok := targetMovieCache.Load(match.ServerItem.ItemID)
			if !ok {
				files, err := fileFetcher.GetMovieFiles(ctx, match.ServerItem.ItemID)
				if err != nil {
					runLog.Warn("Failed to fetch movie files from %s for %s: %v", target.Name, match.ArrMedia.Title, err)
					return
				}
				targetMovieCache.Store(match.ServerItem.ItemID, files)
				targetFiles = files
			}

			mismatches := compareMovieFiles(arrFiles.([]ArrMovieFile), targetFiles.([]TargetMovieFile))
			for _, mismatch := range mismatches {
				ignoreKey := fileIgnoreKey(arr.Type, match.ArrMedia.ID, target.ID, 0, 0, mismatch.ArrFileName)
				if _, ignored := ignoreSet[ignoreKey]; ignored {
					continue
				}

				targetItemPath := targetItemPathFromItem(match.ServerItem)

				record := &database.MatcharrFileMismatch{
					RunID:            runID,
					ArrID:            arr.ID,
					TargetID:         target.ID,
					ArrType:          arr.Type,
					ArrName:          arr.Name,
					TargetName:       target.Name,
					MediaTitle:       match.ArrMedia.Title,
					ArrMediaID:       match.ArrMedia.ID,
					TargetMetadataID: match.ServerItem.ItemID,
					ArrFileName:      mismatch.ArrFileName,
					TargetFileNames:  strings.Join(mismatch.TargetFileNames, ", "),
					ArrFilePath:      mismatch.ArrFilePath,
					TargetItemPath:   targetItemPath,
					TargetFilePaths:  strings.Join(mismatch.TargetFilePaths, ", "),
					MultiEpisode:     mismatch.MultiEpisode,
				}

				if err := m.db.CreateMatcharrFileMismatch(record); err != nil {
					runLog.Warn("Failed to save file mismatch for %s: %v", match.ArrMedia.Title, err)
				}
			}

		default:
			runLog.Debug("Skipping file mismatch check for unsupported Arr type %s", arr.Type)
		}
	}

	worker := func() {
		defer wg.Done()
		for match := range workCh {
			select {
			case <-ctx.Done():
				return
			default:
			}
			processMatch(match)
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	for _, match := range matches {
		select {
		case <-ctx.Done():
			runLog.Warn("File mismatch comparison cancelled: %v", ctx.Err())
			close(workCh)
			wg.Wait()
			return
		case workCh <- match:
		}
	}
	close(workCh)
	wg.Wait()

	log.Debug().
		Str("arr", arr.Name).
		Str("target", target.Name).
		Int("matches", len(matches)).
		Msg("File mismatch comparison completed")
}

func (m *Manager) recheckFileMismatches(
	ctx context.Context,
	runID int64,
	arr *database.MatcharrArr,
	target *database.Target,
	targetObj any,
	arrMediaID int64,
	targetMetadataID string,
	mediaTitle string,
	targetItemPath string,
) (int, error) {
	fileFetcher, ok := targetObj.(TargetFileFetcher)
	if !ok {
		return 0, fmt.Errorf("target does not support file mismatch checks")
	}

	arrClient := NewArrClient(arr.URL, arr.APIKey, arr.Type)

	ignores, err := m.db.ListMatcharrFileIgnores()
	if err != nil {
		return 0, err
	}
	ignoreSet := make(map[string]struct{}, len(ignores))
	for _, ignore := range ignores {
		key := fileIgnoreKey(ignore.ArrType, ignore.ArrMediaID, ignore.TargetID, ignore.SeasonNumber, ignore.EpisodeNumber, ignore.ArrFileName)
		ignoreSet[key] = struct{}{}
	}

	existing, err := m.db.ListMatcharrFileMismatchesForMedia(runID, arr.ID, target.ID, arrMediaID)
	if err != nil {
		return 0, err
	}

	existingMap := make(map[string]*database.MatcharrFileMismatch)
	for _, mismatch := range existing {
		key := mismatchDetailKey(mismatch.SeasonNumber, mismatch.EpisodeNumber, mismatch.ArrFileName)
		existingMap[key] = mismatch
	}

	var mismatchDetails []fileMismatchDetail
	switch arr.Type {
	case database.ArrTypeSonarr:
		arrFiles, err := arrClient.GetEpisodeFiles(ctx, arrMediaID)
		if err != nil {
			return 0, err
		}
		targetFiles, err := fileFetcher.GetEpisodeFiles(ctx, targetMetadataID)
		if err != nil {
			return 0, err
		}
		mismatchDetails = compareEpisodeFiles(arrFiles, targetFiles)
	case database.ArrTypeRadarr:
		arrFiles, err := arrClient.GetMovieFiles(ctx, arrMediaID)
		if err != nil {
			return 0, err
		}
		targetFiles, err := fileFetcher.GetMovieFiles(ctx, targetMetadataID)
		if err != nil {
			return 0, err
		}
		mismatchDetails = compareMovieFiles(arrFiles, targetFiles)
	default:
		return 0, fmt.Errorf("unsupported arr type: %s", arr.Type)
	}

	currentKeys := make(map[string]struct{})
	for _, detail := range mismatchDetails {
		ignoreKey := fileIgnoreKey(arr.Type, arrMediaID, target.ID, detail.SeasonNumber, detail.EpisodeNumber, detail.ArrFileName)
		if _, ignored := ignoreSet[ignoreKey]; ignored {
			continue
		}

		key := mismatchDetailKey(detail.SeasonNumber, detail.EpisodeNumber, detail.ArrFileName)
		currentKeys[key] = struct{}{}

		targetFileNames := strings.Join(detail.TargetFileNames, ", ")
		targetFilePaths := strings.Join(detail.TargetFilePaths, ", ")

		if existingMismatch, ok := existingMap[key]; ok {
			if err := m.db.UpdateMatcharrFileMismatch(existingMismatch.ID, targetFileNames, detail.ArrFilePath, targetItemPath, targetFilePaths, detail.MultiEpisode); err != nil {
				return 0, err
			}
			continue
		}

		record := &database.MatcharrFileMismatch{
			RunID:            runID,
			ArrID:            arr.ID,
			TargetID:         target.ID,
			ArrType:          arr.Type,
			ArrName:          arr.Name,
			TargetName:       target.Name,
			MediaTitle:       mediaTitle,
			ArrMediaID:       arrMediaID,
			TargetMetadataID: targetMetadataID,
			SeasonNumber:     detail.SeasonNumber,
			EpisodeNumber:    detail.EpisodeNumber,
			ArrFileName:      detail.ArrFileName,
			TargetFileNames:  targetFileNames,
			ArrFilePath:      detail.ArrFilePath,
			TargetItemPath:   targetItemPath,
			TargetFilePaths:  targetFilePaths,
			MultiEpisode:     detail.MultiEpisode,
		}
		if err := m.db.CreateMatcharrFileMismatch(record); err != nil {
			return 0, err
		}
	}

	for _, mismatch := range existing {
		key := mismatchDetailKey(mismatch.SeasonNumber, mismatch.EpisodeNumber, mismatch.ArrFileName)
		if _, stillPresent := currentKeys[key]; stillPresent {
			continue
		}
		if err := m.db.DeleteMatcharrFileMismatch(mismatch.ID); err != nil {
			return 0, err
		}
	}

	remaining, err := m.db.ListMatcharrFileMismatchesForMedia(runID, arr.ID, target.ID, arrMediaID)
	if err != nil {
		return 0, err
	}
	return len(remaining), nil
}

// RecheckFileMismatch re-runs filename comparison for a single media item.
func (m *Manager) RecheckFileMismatch(ctx context.Context, mismatchID int64) (int, error) {
	if m == nil {
		return 0, fmt.Errorf("manager not initialized")
	}

	mismatch, err := m.db.GetMatcharrFileMismatch(mismatchID)
	if err != nil {
		return 0, err
	}
	if mismatch == nil {
		return 0, fmt.Errorf("file mismatch not found")
	}

	arr, err := m.db.GetMatcharrArr(mismatch.ArrID)
	if err != nil {
		return 0, err
	}
	if arr == nil {
		return 0, fmt.Errorf("arr not found")
	}

	target, err := m.db.GetTarget(mismatch.TargetID)
	if err != nil {
		return 0, err
	}
	if target == nil {
		return 0, fmt.Errorf("target not found")
	}

	targetObj, err := m.targetGetter.GetTargetAny(target.ID)
	if err != nil {
		return 0, err
	}

	return m.recheckFileMismatches(
		ctx,
		mismatch.RunID,
		arr,
		target,
		targetObj,
		mismatch.ArrMediaID,
		mismatch.TargetMetadataID,
		mismatch.MediaTitle,
		mismatch.TargetItemPath,
	)
}

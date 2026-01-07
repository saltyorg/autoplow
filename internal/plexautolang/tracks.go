package plexautolang

import (
	"strings"
)

// MatchAudioStream finds the best matching audio stream in candidates
// Returns nil if no suitable match found
func MatchAudioStream(reference *AudioStream, candidates []AudioStream) *AudioStream {
	if reference == nil || len(candidates) == 0 {
		return nil
	}

	// Step 1: Filter by language code (required)
	var filtered []AudioStream
	for _, s := range candidates {
		if s.LanguageCode == reference.LanguageCode {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return &filtered[0]
	}

	// Step 2: Filter by visual impaired flag
	filtered = filterByVisualImpaired(filtered, reference.VisualImpaired, reference.Title)
	if len(filtered) == 1 {
		return &filtered[0]
	}

	// Step 3: Score remaining candidates
	var bestScore int
	var best *AudioStream

	for i := range filtered {
		score := scoreAudioStream(reference, &filtered[i], len(candidates))
		if score > bestScore || best == nil {
			bestScore = score
			best = &filtered[i]
		}
	}

	return best
}

// MatchSubtitleStream finds the best matching subtitle stream.
// Returns nil to indicate "no subtitles".
func MatchSubtitleStream(reference *SubtitleStream, audio *AudioStream, candidates []SubtitleStream) *SubtitleStream {
	if len(candidates) == 0 {
		return nil
	}

	var languageCode string
	var matchForcedOnly bool
	var matchHIOnly bool

	if reference == nil {
		return nil
	} else {
		languageCode = reference.LanguageCode
		matchForcedOnly = reference.Forced
		matchHIOnly = reference.HearingImpaired
	}

	// Filter candidates
	var filtered []SubtitleStream
	for _, s := range candidates {
		if s.LanguageCode != languageCode {
			continue
		}
		if matchForcedOnly && !s.Forced {
			continue
		}
		filtered = append(filtered, s)
	}

	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return &filtered[0]
	}

	// Filter by hearing impaired if reference had it
	if matchHIOnly {
		var hiFiltered []SubtitleStream
		for _, s := range filtered {
			if s.HearingImpaired {
				hiFiltered = append(hiFiltered, s)
			}
		}
		if len(hiFiltered) > 0 {
			filtered = hiFiltered
		}
	}

	if len(filtered) == 1 {
		return &filtered[0]
	}

	// Score remaining candidates
	var bestScore int
	var best *SubtitleStream

	for i := range filtered {
		score := scoreSubtitleStream(reference, &filtered[i])
		if score > bestScore || best == nil {
			bestScore = score
			best = &filtered[i]
		}
	}

	return best
}

func scoreAudioStream(ref, candidate *AudioStream, totalCandidates int) int {
	score := 0

	// Codec match (+5)
	if ref.Codec == candidate.Codec {
		score += 5
	}

	// Channel layout match (+3)
	if ref.AudioChannelLayout == candidate.AudioChannelLayout {
		score += 3
	}

	// Channel count handling for ambiguous streams
	if totalCandidates > 2 {
		if ref.Channels < 3 {
			// For stereo/mono reference, prefer more channels
			if candidate.Channels > ref.Channels {
				score += 8
			}
		} else {
			// For surround reference, prefer same or more channels
			if candidate.Channels >= ref.Channels {
				score += 1
			}
		}
	}

	// Title matches (+5 each)
	if ref.ExtendedDisplayTitle != "" && ref.ExtendedDisplayTitle == candidate.ExtendedDisplayTitle {
		score += 5
	}
	if ref.DisplayTitle != "" && ref.DisplayTitle == candidate.DisplayTitle {
		score += 5
	}
	if ref.Title != "" && ref.Title == candidate.Title {
		score += 5
	}

	return score
}

func scoreSubtitleStream(ref, candidate *SubtitleStream) int {
	if ref == nil {
		return 0
	}

	score := 0

	// Forced flag match (+3)
	if ref.Forced == candidate.Forced {
		score += 3
	}

	// Hearing impaired match (+3)
	if ref.HearingImpaired == candidate.HearingImpaired {
		score += 3
	}

	// Codec match (+1)
	if ref.Codec == candidate.Codec {
		score += 1
	}

	// Title matches (+5 each)
	if ref.ExtendedDisplayTitle != "" && ref.ExtendedDisplayTitle == candidate.ExtendedDisplayTitle {
		score += 5
	}
	if ref.DisplayTitle != "" && ref.DisplayTitle == candidate.DisplayTitle {
		score += 5
	}
	if ref.Title != "" && ref.Title == candidate.Title {
		score += 5
	}

	return score
}

func filterByVisualImpaired(streams []AudioStream, visualImpaired bool, refTitle string) []AudioStream {
	// First try to match the exact visual impaired flag
	var filtered []AudioStream
	for _, s := range streams {
		if s.VisualImpaired == visualImpaired {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) > 0 {
		return filtered
	}

	// If no exact match, try matching by descriptive audio terms in title
	if visualImpaired || containsDescriptiveTerms(refTitle) {
		for _, s := range streams {
			if containsDescriptiveTerms(s.Title) || containsDescriptiveTerms(s.DisplayTitle) {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			return filtered
		}
	}

	// Fall back to excluding descriptive audio if reference was not visual impaired
	if !visualImpaired {
		for _, s := range streams {
			if !containsDescriptiveTerms(s.Title) && !containsDescriptiveTerms(s.DisplayTitle) {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			return filtered
		}
	}

	return streams // Fall back to all if no match
}

// containsDescriptiveTerms checks if a title contains terms indicating descriptive audio
func containsDescriptiveTerms(title string) bool {
	lower := strings.ToLower(title)
	descriptiveTerms := []string{
		"descriptive",
		"audio description",
		"visual impaired",
		"visually impaired",
		"ad:",
		"(ad)",
		"[ad]",
	}
	for _, term := range descriptiveTerms {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

// GetSelectedAudioStream returns the currently selected audio stream from a part
func GetSelectedAudioStream(part *MediaPart) *AudioStream {
	for i := range part.AudioStreams {
		if part.AudioStreams[i].Selected {
			return &part.AudioStreams[i]
		}
	}
	return nil
}

// GetSelectedSubtitleStream returns the currently selected subtitle stream from a part
// Returns nil if no subtitle is selected
func GetSelectedSubtitleStream(part *MediaPart) *SubtitleStream {
	for i := range part.SubtitleStreams {
		if part.SubtitleStreams[i].Selected {
			return &part.SubtitleStreams[i]
		}
	}
	return nil
}

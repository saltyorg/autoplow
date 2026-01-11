package targets

import (
	"testing"
)

func TestGetSearchTerm(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "TV show with season directory",
			path:     "/media/TV Shows/Breaking Bad/Season 1/S01E01.mkv",
			expected: "Breaking Bad",
		},
		{
			name:     "Movie with year and quality in brackets",
			path:     "/media/Movies/The Matrix (1999) [1080p]/matrix.mkv",
			expected: "The Matrix",
		},
		{
			name:     "Simple movie path",
			path:     "/media/Movies/Inception/inception.mkv",
			expected: "Inception",
		},
		{
			name:     "TV show season directory path",
			path:     "/media/TV Shows/Game of Thrones/Season 2",
			expected: "Game of Thrones",
		},
		{
			name:     "TV show without season directory",
			path:     "/media/TV Shows/Game of Thrones",
			expected: "Game of Thrones",
		},
		{
			name:     "Multiple season levels",
			path:     "/media/TV Shows/Doctor Who/Season 10/Season 10 Part 2/S10E12.mkv",
			expected: "Doctor Who",
		},
		{
			name:     "Movie with multiple bracket types",
			path:     "/media/Movies/Avatar (2009) [2160p] {HDR}/avatar.mkv",
			expected: "Avatar",
		},
		{
			name:     "Specials directory",
			path:     "/media/TV Shows/The Office/Specials/Special1.mkv",
			expected: "The Office",
		},
		{
			name:     "Nested quality folders",
			path:     "/media/Movies/4K/Dune (2021)/dune.mkv",
			expected: "Dune",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSearchTerm(tt.path)
			if result != tt.expected {
				t.Errorf("getSearchTerm(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}

func TestGetMatchingLibraries(t *testing.T) {
	scanner := &PlexTarget{}

	libraries := []Library{
		{
			ID:    "1",
			Name:  "Movies",
			Path:  "/media/movies",
			Paths: []string{"/media/movies"},
		},
		{
			ID:    "2",
			Name:  "Movies 4K",
			Path:  "/media/movies/4k",
			Paths: []string{"/media/movies/4k"},
		},
		{
			ID:    "3",
			Name:  "TV Shows",
			Path:  "/media/tv",
			Paths: []string{"/media/tv"},
		},
	}

	tests := []struct {
		name           string
		path           string
		expectedFirst  string
		expectedLength int
	}{
		{
			name:           "Match 4K library first (deeper path)",
			path:           "/media/movies/4k/Inception.mkv",
			expectedFirst:  "2",
			expectedLength: 2, // Both Movies and Movies 4K match
		},
		{
			name:           "Match regular movies",
			path:           "/media/movies/The Matrix/matrix.mkv",
			expectedFirst:  "1",
			expectedLength: 1,
		},
		{
			name:           "Match TV shows",
			path:           "/media/tv/Breaking Bad/S01E01.mkv",
			expectedFirst:  "3",
			expectedLength: 1,
		},
		{
			name:           "No match",
			path:           "/other/path/file.mkv",
			expectedFirst:  "",
			expectedLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scanner.getMatchingLibraries(tt.path, libraries)
			if len(result) != tt.expectedLength {
				t.Errorf("getMatchingLibraries(%q) returned %d libraries, want %d", tt.path, len(result), tt.expectedLength)
			}
			if tt.expectedLength > 0 && result[0].ID != tt.expectedFirst {
				t.Errorf("getMatchingLibraries(%q) first library ID = %q, want %q", tt.path, result[0].ID, tt.expectedFirst)
			}
		})
	}
}

func TestMediaMatchesPath(t *testing.T) {
	scanner := &PlexTarget{}

	tests := []struct {
		name     string
		media    []plexMedia
		path     string
		expected bool
	}{
		{
			name: "Exact file match",
			media: []plexMedia{
				{Part: []plexPart{{File: "/media/movies/Inception/inception.mkv"}}},
			},
			path:     "/media/movies/Inception/inception.mkv",
			expected: true,
		},
		{
			name: "Directory contains file",
			media: []plexMedia{
				{Part: []plexPart{{File: "/media/movies/Inception/inception.mkv"}}},
			},
			path:     "/media/movies/Inception",
			expected: true,
		},
		{
			name: "No match",
			media: []plexMedia{
				{Part: []plexPart{{File: "/media/movies/Matrix/matrix.mkv"}}},
			},
			path:     "/media/movies/Inception",
			expected: false,
		},
		{
			name: "Multiple parts, one matches",
			media: []plexMedia{
				{Part: []plexPart{
					{File: "/media/movies/Inception/inception-part1.mkv"},
					{File: "/media/movies/Inception/inception-part2.mkv"},
				}},
			},
			path:     "/media/movies/Inception/inception-part1.mkv",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scanner.mediaMatchesPath(tt.media, tt.path)
			if result != tt.expected {
				t.Errorf("mediaMatchesPath() = %v, want %v", result, tt.expected)
			}
		})
	}
}

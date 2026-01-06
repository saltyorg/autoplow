package matcharr

import (
	"testing"

	"github.com/saltyorg/autoplow/internal/database"
)

func TestDetermineRequiredLibraries_UsesAllLibraryPaths(t *testing.T) {
	m := &Manager{}
	arr := &database.MatcharrArr{
		ID:   1,
		Name: "Radarr",
	}

	arrMetadata := []arrMetadataResult{{
		arr: arr,
		rootFolders: []RootFolder{
			{Path: "/data/movies"},
		},
	}}

	target := &database.Target{ID: 2, Name: "Plex"}
	targetMetadata := []targetMetadataResult{{
		target: target,
		libraries: []Library{{
			ID:    "lib1",
			Name:  "Movies",
			Paths: []string{"/mnt/movies", "/data/movies"},
		}},
	}}

	required, arrMap := m.determineRequiredLibraries(arrMetadata, targetMetadata, NewRunLogger())

	if len(required) != 1 {
		t.Fatalf("expected 1 required library, got %d", len(required))
	}
	if len(arrMap[arr.ID]) != 1 {
		t.Fatalf("expected arr to map to 1 library, got %d", len(arrMap[arr.ID]))
	}
	if required[0].library.ID != "lib1" {
		t.Fatalf("expected library lib1 to be selected, got %s", required[0].library.ID)
	}
}

func TestFilterItemsByIgnoredPaths(t *testing.T) {
	ignored := []string{"/data/manual", "/data/temp"}
	items := []MediaServerItem{
		{Path: "/data/movies/Avatar/Avatar.mkv"},
		{Path: "/data/manual/Keep.mkv"},
		{Path: "/data/temp/Skip/Skip.mkv"},
		{Path: "/data/manual"}, // folder path match
	}

	filtered := filterItemsByIgnoredPaths(items, ignored)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 item after filtering, got %d", len(filtered))
	}
	if filtered[0].Path != "/data/movies/Avatar/Avatar.mkv" {
		t.Fatalf("unexpected remaining path: %s", filtered[0].Path)
	}
}

func TestDetermineRequiredLibraries_RespectsMatcharrIgnorePaths(t *testing.T) {
	m := &Manager{}
	arr := &database.MatcharrArr{
		ID:   1,
		Name: "Radarr",
	}

	arrMetadata := []arrMetadataResult{{
		arr: arr,
		rootFolders: []RootFolder{
			{Path: "/data/movies"},
		},
	}}

	target := &database.Target{
		ID:   2,
		Name: "Plex",
		Config: database.TargetConfig{
			MatcharrExcludePaths: []string{"/data/movies"},
		},
	}

	targetMetadata := []targetMetadataResult{{
		target: target,
		libraries: []Library{{
			ID:    "lib1",
			Name:  "Movies",
			Paths: []string{"/data/movies"},
		}},
	}}

	required, arrMap := m.determineRequiredLibraries(arrMetadata, targetMetadata, NewRunLogger())

	if len(required) != 0 {
		t.Fatalf("expected no required libraries when all paths are ignored, got %d", len(required))
	}
	if len(arrMap[arr.ID]) != 0 {
		t.Fatalf("expected no libraries mapped to arr when all paths are ignored, got %d", len(arrMap[arr.ID]))
	}
}

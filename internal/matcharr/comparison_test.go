package matcharr

import (
	"context"
	"testing"

	"github.com/saltyorg/autoplow/internal/database"
)

func TestCompareArrToTarget_RequiresExactFolderMatch(t *testing.T) {
	arr := &database.MatcharrArr{
		ID:   1,
		Name: "Radarr",
		Type: database.ArrTypeRadarr,
	}
	arrMedia := []ArrMedia{{
		Title:  "Other Movie",
		Path:   "/mnt/media/Movies/Other Movie",
		TMDBID: 1,
	}}
	target := &database.Target{
		ID:   10,
		Name: "Plex",
	}
	targetItems := []MediaServerItem{{
		Title: "Avatar",
		Path:  "/mnt/media/Movies/Avatar/Avatar.mkv",
		ProviderIDs: map[string]string{
			"tmdb": "2",
		},
	}}

	result := CompareArrToTarget(context.Background(), arr, arrMedia, target, targetItems)

	if result.Compared != 0 {
		t.Fatalf("expected 0 items compared, got %d", result.Compared)
	}
	if len(result.Mismatches) != 0 {
		t.Fatalf("expected no mismatches when paths differ, got %d", len(result.Mismatches))
	}
}

func TestCompareArrToTarget_MatchesMappedFolderExactly(t *testing.T) {
	arr := &database.MatcharrArr{
		ID:   2,
		Name: "Radarr",
		Type: database.ArrTypeRadarr,
		PathMappings: []database.MatcharrPathMapping{{
			ArrPath:    "/mnt/media",
			ServerPath: "/srv/movies",
		}},
	}
	arrMedia := []ArrMedia{{
		Title:  "Avatar",
		Path:   "/mnt/media/Avatar",
		TMDBID: 123,
	}}
	target := &database.Target{
		ID:   20,
		Name: "Plex",
	}
	targetItems := []MediaServerItem{{
		Title: "Avatar",
		Path:  "/srv/movies/Avatar/Avatar.mkv",
		ProviderIDs: map[string]string{
			"tmdb": "999", // intentionally different to surface mismatch
		},
	}}

	result := CompareArrToTarget(context.Background(), arr, arrMedia, target, targetItems)

	if result.Compared != 1 {
		t.Fatalf("expected 1 item compared, got %d", result.Compared)
	}
	if len(result.Mismatches) != 1 {
		t.Fatalf("expected 1 mismatch, got %d", len(result.Mismatches))
	}
	if result.Mismatches[0].ExpectedID != "123" || result.Mismatches[0].ActualID != "999" {
		t.Fatalf("unexpected mismatch IDs: expected=%s actual=%s", result.Mismatches[0].ExpectedID, result.Mismatches[0].ActualID)
	}
}

func TestCompareArrToTarget_AllowsDotsInDirectoryName(t *testing.T) {
	arr := &database.MatcharrArr{
		ID:   3,
		Name: "Sonarr",
		Type: database.ArrTypeSonarr,
	}
	arrMedia := []ArrMedia{{
		Title:  "Taylor Swift vs. Scooter Braun: Bad Blood",
		Path:   "/mnt/media/TV/Taylor Swift vs. Scooter Braun - Bad Blood (2024) (tvdb-451309)",
		TVDBID: 451309,
	}}
	target := &database.Target{
		ID:   30,
		Name: "Plex",
	}
	targetItems := []MediaServerItem{{
		Title: "Taylor Swift vs. Scooter Braun: Bad Blood",
		Path:  "/mnt/media/TV/Taylor Swift vs. Scooter Braun - Bad Blood (2024) (tvdb-451309)",
		ProviderIDs: map[string]string{
			"tvdb": "451309",
		},
	}}

	result := CompareArrToTarget(context.Background(), arr, arrMedia, target, targetItems)

	if result.Compared != 1 {
		t.Fatalf("expected 1 item compared, got %d", result.Compared)
	}
	if len(result.MissingArr) != 0 || len(result.MissingSrv) != 0 {
		t.Fatalf("expected no gaps, got missing_arr=%d missing_srv=%d", len(result.MissingArr), len(result.MissingSrv))
	}
	if len(result.Mismatches) != 0 {
		t.Fatalf("expected no mismatches, got %d", len(result.Mismatches))
	}
}

func TestMapPath_RespectsPathBoundaries(t *testing.T) {
	mappings := []database.MatcharrPathMapping{{
		ArrPath:    "/mnt/media",
		ServerPath: "/srv/media",
	}}

	// Should map when prefix matches a full path segment
	mapped := mapPath("/mnt/media/Movie", mappings)
	if mapped != "/srv/media/Movie" {
		t.Fatalf("expected mapped path /srv/media/Movie, got %s", mapped)
	}

	// Should not map when the prefix only matches partially
	unchanged := mapPath("/mnt/mediabackup/Movie", mappings)
	if unchanged != "/mnt/mediabackup/Movie" {
		t.Fatalf("expected path to remain unchanged, got %s", unchanged)
	}
}

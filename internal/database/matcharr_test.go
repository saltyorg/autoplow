package database

import (
	"path/filepath"
	"testing"
)

func TestCreateMatcharrMismatch_InsertsAllColumns(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")

	db, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	// Seed required foreign key rows
	if _, err := db.exec(`
		INSERT INTO matcharr_runs (id, started_at, status, total_compared, mismatches_found, mismatches_fixed, triggered_by)
		VALUES (1, CURRENT_TIMESTAMP, 'running', 0, 0, 0, 'manual')
	`); err != nil {
		t.Fatalf("failed to seed run: %v", err)
	}

	if _, err := db.exec(`
		INSERT INTO matcharr_arrs (id, name, type, url, api_key, enabled, path_mappings, created_at, updated_at)
		VALUES (1, 'Sonarr', 'sonarr', 'http://localhost', 'abc', 1, '[]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("failed to seed arr: %v", err)
	}

	if _, err := db.exec(`
		INSERT INTO targets (id, name, type, url, token, api_key, enabled, config, created_at, updated_at)
		VALUES (1, 'Plex', 'plex', 'http://localhost', '', '', 1, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`); err != nil {
		t.Fatalf("failed to seed target: %v", err)
	}

	mismatch := &MatcharrMismatch{
		RunID:            1,
		ArrID:            1,
		TargetID:         1,
		ArrType:          ArrTypeSonarr,
		ArrName:          "Sonarr",
		TargetName:       "Plex",
		TargetTitle:      "Plex Title",
		MediaTitle:       "Some Show",
		MediaPath:        "/arr/path",
		TargetPath:       "/server/path",
		ArrIDType:        "tvdb",
		ArrIDValue:       "12345",
		TargetIDType:     "tvdb",
		TargetIDValue:    "99999",
		TargetMetadataID: "meta-1",
		Status:           MatcharrMismatchStatusPending,
	}

	if err := db.CreateMatcharrMismatch(mismatch); err != nil {
		t.Fatalf("CreateMatcharrMismatch returned error: %v", err)
	}

	saved, err := db.GetMatcharrMismatch(mismatch.ID)
	if err != nil {
		t.Fatalf("GetMatcharrMismatch returned error: %v", err)
	}
	if saved == nil {
		t.Fatal("expected mismatch to be saved")
	}

	if saved.TargetTitle != mismatch.TargetTitle {
		t.Fatalf("expected target_title %q, got %q", mismatch.TargetTitle, saved.TargetTitle)
	}
	if saved.TargetPath != mismatch.TargetPath {
		t.Fatalf("expected target_path %q, got %q", mismatch.TargetPath, saved.TargetPath)
	}
	if saved.ArrIDValue != mismatch.ArrIDValue || saved.TargetIDValue != mismatch.TargetIDValue {
		t.Fatalf("expected id values %q/%q, got %q/%q", mismatch.ArrIDValue, mismatch.TargetIDValue, saved.ArrIDValue, saved.TargetIDValue)
	}
}

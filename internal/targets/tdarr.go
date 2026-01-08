package targets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/httpclient"
)

// TdarrTarget implements the Target interface for Tdarr
type TdarrTarget struct {
	dbTarget *database.Target
	client   *http.Client
}

// NewTdarrTarget creates a new Tdarr target
func NewTdarrTarget(dbTarget *database.Target) *TdarrTarget {
	return &TdarrTarget{
		dbTarget: dbTarget,
		client:   httpclient.NewTraceClient(fmt.Sprintf("%s:%s", dbTarget.Type, dbTarget.Name), config.GetTimeouts().HTTPClient),
	}
}

// Name returns the scanner name
func (s *TdarrTarget) Name() string {
	return s.dbTarget.Name
}

// Type returns the scanner type
func (s *TdarrTarget) Type() database.TargetType {
	return database.TargetTypeTdarr
}

// SupportsSessionMonitoring returns false as Tdarr doesn't have playback sessions
func (s *TdarrTarget) SupportsSessionMonitoring() bool {
	return false
}

// tdarrScanPayload represents the payload for Tdarr scan API
type tdarrScanPayload struct {
	Data tdarrScanData `json:"data"`
}

type tdarrScanData struct {
	ScanConfig tdarrScanConfig `json:"scanConfig"`
}

type tdarrScanConfig struct {
	DbID        string   `json:"dbID"`
	ArrayOrPath []string `json:"arrayOrPath"`
	Mode        string   `json:"mode"`
}

// Scan triggers a scan for the given path on Tdarr
func (s *TdarrTarget) Scan(ctx context.Context, path string) error {
	if s.dbTarget.Config.TdarrDBID == "" {
		return fmt.Errorf("tdarr: library ID (db_id) not configured")
	}

	payload := tdarrScanPayload{
		Data: tdarrScanData{
			ScanConfig: tdarrScanConfig{
				DbID:        s.dbTarget.Config.TdarrDBID,
				ArrayOrPath: []string{path},
				Mode:        "scanFolderWatcher",
			},
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("tdarr: failed to marshal payload: %w", err)
	}

	scanURL := fmt.Sprintf("%s/api/v2/scan-files", s.dbTarget.URL)
	req, err := http.NewRequestWithContext(ctx, "POST", scanURL, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("tdarr: failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("tdarr: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("tdarr: returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Info().
		Str("scanner", s.Name()).
		Str("path", path).
		Str("db_id", s.dbTarget.Config.TdarrDBID).
		Msg("Tdarr scan triggered")

	return nil
}

// TestConnection verifies the connection to Tdarr
func (s *TdarrTarget) TestConnection(ctx context.Context) error {
	// Tdarr doesn't have a dedicated health endpoint, but we can try to get status
	testURL := fmt.Sprintf("%s/api/v2/status", s.dbTarget.URL)

	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return fmt.Errorf("tdarr: failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("tdarr: connection to %s failed: %w", s.dbTarget.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tdarr: %s returned status %d", testURL, resp.StatusCode)
	}

	return nil
}

// GetLibraries returns available Tdarr libraries
func (s *TdarrTarget) GetLibraries(ctx context.Context) ([]Library, error) {
	// Tdarr libraries can be fetched from the status endpoint or a dedicated endpoint
	// For now, return empty as library discovery isn't critical for basic functionality
	return []Library{}, nil
}

// GetSessions returns active sessions (not applicable for Tdarr)
func (s *TdarrTarget) GetSessions(ctx context.Context) ([]Session, error) {
	// Tdarr doesn't have playback sessions like media servers
	return []Session{}, nil
}

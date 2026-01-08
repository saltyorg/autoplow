package targets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/httpclient"
)

// AutoplowTarget implements the Target interface for forwarding scans to another Autoplow instance
type AutoplowTarget struct {
	dbTarget *database.Target
	client   *http.Client
}

// NewAutoplowTarget creates a new Autoplow target
func NewAutoplowTarget(dbTarget *database.Target) *AutoplowTarget {
	return &AutoplowTarget{
		dbTarget: dbTarget,
		client:   httpclient.NewTraceClient(fmt.Sprintf("%s:%s", dbTarget.Type, dbTarget.Name), config.GetTimeouts().HTTPClient),
	}
}

// Name returns the scanner name
func (s *AutoplowTarget) Name() string {
	return s.dbTarget.Name
}

// Type returns the scanner type
func (s *AutoplowTarget) Type() database.TargetType {
	return database.TargetTypeAutoplow
}

// SupportsSessionMonitoring returns false as Autoplow doesn't have playback sessions
func (s *AutoplowTarget) SupportsSessionMonitoring() bool {
	return false
}

// Scan forwards the scan request to another Autoplow instance
func (s *AutoplowTarget) Scan(ctx context.Context, path string) error {
	// Build the webhook URL using the target's URL and API key
	// The URL should be the base URL of the remote autoplow instance
	// We'll send to /api/triggers/autoplow/{trigger_id}?api_key={api_key}
	// The target.URL contains the full webhook URL including trigger ID and API key
	webhookURL := strings.TrimRight(s.dbTarget.URL, "/")

	// Build request body in the same format as the webhook trigger expects
	reqBody := map[string]any{
		"paths": []string{path},
	}

	bodyJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Add API key header if configured (in addition to query param in URL)
	if s.dbTarget.APIKey != "" {
		req.Header.Set("X-API-Key", s.dbTarget.APIKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("autoplow returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Info().
		Str("scanner", s.Name()).
		Str("path", path).
		Str("url", webhookURL).
		Msg("Scan forwarded to remote Autoplow instance")

	return nil
}

// TestConnection verifies the connection to the remote Autoplow instance
func (s *AutoplowTarget) TestConnection(ctx context.Context) error {
	// Try to hit the health endpoint or just make a request to verify connectivity
	// Most autoplow instances will have a health check at /health or the base URL
	baseURL := s.dbTarget.URL

	// Extract base URL (remove /api/triggers/... path if present)
	if idx := strings.Index(baseURL, "/api/triggers/"); idx > 0 {
		baseURL = baseURL[:idx]
	}

	healthURL := fmt.Sprintf("%s/health", strings.TrimRight(baseURL, "/"))

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer resp.Body.Close()

	// Accept any 2xx status code as success
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return fmt.Errorf("server returned status %d", resp.StatusCode)
}

// GetLibraries returns an empty list since Autoplow doesn't have libraries
// This is required by the Scanner interface but not applicable to Autoplow targets
func (s *AutoplowTarget) GetLibraries(ctx context.Context) ([]Library, error) {
	// Autoplow doesn't have the concept of libraries
	return []Library{}, nil
}

// GetSessions returns an empty list since Autoplow doesn't have playback sessions
// This is required by the Scanner interface but not applicable to Autoplow targets
func (s *AutoplowTarget) GetSessions(ctx context.Context) ([]Session, error) {
	// Autoplow doesn't have the concept of playback sessions
	return []Session{}, nil
}

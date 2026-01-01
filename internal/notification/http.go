package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// sendJSONRequest marshals payload to JSON and sends it to the given URL.
// Returns an error if the request fails or returns a non-2xx status.
func sendJSONRequest(ctx context.Context, client *http.Client, method, url string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	return doRequest(client, req)
}

// doRequest executes an HTTP request and checks for a successful status code.
func doRequest(client *http.Client, req *http.Request) error {
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

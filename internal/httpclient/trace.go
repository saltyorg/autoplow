package httpclient

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type traceTransport struct {
	base http.RoundTripper
	name string
}

// NewTraceTransport returns a RoundTripper that logs requests at trace level.
func NewTraceTransport(name string, base http.RoundTripper) http.RoundTripper {
	return &traceTransport{
		base: base,
		name: name,
	}
}

// NewTraceClient returns an HTTP client that logs requests at trace level.
func NewTraceClient(name string, timeout time.Duration) *http.Client {
	return Wrap(&http.Client{Timeout: timeout}, name)
}

// Wrap applies trace logging to an existing HTTP client.
func Wrap(client *http.Client, name string) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	client.Transport = NewTraceTransport(name, client.Transport)
	return client
}

func (t *traceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}

	urlStr := redactURL(req.URL)
	start := time.Now()

	log.Trace().
		Str("client", t.name).
		Str("method", req.Method).
		Str("url", urlStr).
		Msg("HTTP request")

	resp, err := base.RoundTrip(req)
	duration := time.Since(start)
	if err != nil {
		log.Trace().
			Str("client", t.name).
			Str("method", req.Method).
			Str("url", urlStr).
			Dur("duration", duration).
			Err(err).
			Msg("HTTP request failed")
		return nil, err
	}

	bodyBytes, readErr := readAndRestoreBody(resp)
	logEvent := log.Trace().
		Str("client", t.name).
		Str("method", req.Method).
		Str("url", urlStr).
		Int("status", resp.StatusCode).
		Dur("duration", duration).
		Int("body_length", len(bodyBytes))

	if readErr != nil {
		logEvent.Err(readErr)
	}

	if len(bodyBytes) > 0 {
		if json.Valid(bodyBytes) {
			logEvent.RawJSON("body", bodyBytes)
		} else {
			logEvent.Str("body", string(bodyBytes))
		}
	}

	logEvent.Msg("HTTP response")

	return resp, nil
}

func readAndRestoreBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	return bodyBytes, err
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}

	copyURL := *u
	if copyURL.RawQuery == "" {
		return copyURL.String()
	}

	q := copyURL.Query()
	for key := range q {
		if isSensitiveQueryKey(key) {
			q.Set(key, "redacted")
		}
	}

	copyURL.RawQuery = q.Encode()
	return copyURL.String()
}

func isSensitiveQueryKey(key string) bool {
	switch strings.ToLower(key) {
	case "apikey", "api_key", "api-key", "token", "access_token", "x-plex-token", "authorization", "auth":
		return true
	default:
		return false
	}
}

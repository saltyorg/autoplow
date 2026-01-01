package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"text/template"
	"time"
)

// WebhookConfig holds generic webhook configuration
type WebhookConfig struct {
	URL         string
	Method      string            // HTTP method (POST, PUT, etc.)
	Body        string            // Template for request body
	Headers     map[string]string // Custom headers
	ContentType string            // Content-Type header
	Enabled     bool
}

// WebhookProvider sends notifications via generic HTTP webhooks
type WebhookProvider struct {
	config WebhookConfig
	client *http.Client
}

// NewWebhookProvider creates a new generic webhook notification provider
func NewWebhookProvider(config WebhookConfig) *WebhookProvider {
	// Set defaults
	if config.Method == "" {
		config.Method = "POST"
	}
	if config.ContentType == "" {
		config.ContentType = "application/json"
	}
	if config.Headers == nil {
		config.Headers = make(map[string]string)
	}

	return &WebhookProvider{
		config: config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name returns the provider name
func (w *WebhookProvider) Name() string {
	return "webhook"
}

// webhookTemplateData holds the data available for template rendering
type webhookTemplateData struct {
	Type       string
	Title      string
	Message    string
	Timestamp  string
	Fields     map[string]string
	FieldsJSON string
}

// Send sends a notification via the webhook
func (w *WebhookProvider) Send(ctx context.Context, event Event) error {
	if !w.config.Enabled || w.config.URL == "" {
		return nil
	}

	body, err := w.renderBody(event)
	if err != nil {
		return fmt.Errorf("failed to render body template: %w", err)
	}

	return w.sendRequest(ctx, body)
}

// Test sends a test notification
func (w *WebhookProvider) Test(ctx context.Context) error {
	if w.config.URL == "" {
		return fmt.Errorf("webhook URL not configured")
	}

	event := Event{
		Type:      "test",
		Title:     "Test Notification",
		Message:   "This is a test notification from Autoplow. If you see this, webhook notifications are working!",
		Timestamp: time.Now(),
		Fields: map[string]string{
			"source": "autoplow",
			"test":   "true",
		},
	}

	body, err := w.renderBody(event)
	if err != nil {
		return fmt.Errorf("failed to render body template: %w", err)
	}

	return w.sendRequest(ctx, body)
}

// SetConfig updates the webhook configuration
func (w *WebhookProvider) SetConfig(config WebhookConfig) {
	if config.Method == "" {
		config.Method = "POST"
	}
	if config.ContentType == "" {
		config.ContentType = "application/json"
	}
	if config.Headers == nil {
		config.Headers = make(map[string]string)
	}
	w.config = config
}

// GetConfig returns the current configuration
func (w *WebhookProvider) GetConfig() WebhookConfig {
	return w.config
}

// renderBody renders the body template with event data
func (w *WebhookProvider) renderBody(event Event) (string, error) {
	bodyTemplate := w.config.Body
	if bodyTemplate == "" {
		bodyTemplate = DefaultWebhookBody()
	}

	// Prepare template data
	fieldsJSON, _ := json.Marshal(event.Fields)
	if event.Fields == nil {
		fieldsJSON = []byte("{}")
	}

	data := webhookTemplateData{
		Type:       string(event.Type),
		Title:      event.Title,
		Message:    event.Message,
		Timestamp:  event.Timestamp.Format(time.RFC3339),
		Fields:     event.Fields,
		FieldsJSON: string(fieldsJSON),
	}

	tmpl, err := template.New("webhook").Parse(bodyTemplate)
	if err != nil {
		return "", fmt.Errorf("invalid body template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// sendRequest sends the HTTP request to the webhook URL
func (w *WebhookProvider) sendRequest(ctx context.Context, body string) error {
	req, err := http.NewRequestWithContext(ctx, w.config.Method, w.config.URL, bytes.NewReader([]byte(body)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set Content-Type header
	req.Header.Set("Content-Type", w.config.ContentType)

	// Set custom headers
	for key, value := range w.config.Headers {
		req.Header.Set(key, value)
	}

	return doRequest(w.client, req)
}

// DefaultWebhookBody returns the default webhook body template
func DefaultWebhookBody() string {
	return `{
  "event": "{{.Type}}",
  "title": "{{.Title}}",
  "message": "{{.Message}}",
  "timestamp": "{{.Timestamp}}",
  "fields": {{.FieldsJSON}}
}`
}

// ValidateWebhookBody validates a webhook body template
func ValidateWebhookBody(body string) error {
	if body == "" {
		return nil // Empty body uses default, which is valid
	}

	_, err := template.New("validate").Parse(body)
	if err != nil {
		return fmt.Errorf("invalid template syntax: %w", err)
	}

	return nil
}

// ParseWebhookHeaders parses headers from form data format (key1:value1\nkey2:value2)
func ParseWebhookHeaders(headersStr string) map[string]string {
	headers := make(map[string]string)
	if headersStr == "" {
		return headers
	}

	lines := strings.SplitSeq(headersStr, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key != "" {
				headers[key] = value
			}
		}
	}

	return headers
}

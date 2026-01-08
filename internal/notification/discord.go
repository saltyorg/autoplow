package notification

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/saltyorg/autoplow/internal/httpclient"
)

// DiscordConfig holds Discord webhook configuration
type DiscordConfig struct {
	WebhookURL string
	Username   string // Bot username (optional)
	AvatarURL  string // Bot avatar URL (optional)
	Enabled    bool
}

// DiscordProvider sends notifications via Discord webhooks
type DiscordProvider struct {
	config DiscordConfig
	client *http.Client
}

// NewDiscordProvider creates a new Discord notification provider
func NewDiscordProvider(config DiscordConfig) *DiscordProvider {
	return &DiscordProvider{
		config: config,
		client: httpclient.NewTraceClient("discord", 30*time.Second),
	}
}

// Name returns the provider name
func (d *DiscordProvider) Name() string {
	return "discord"
}

// Send sends a notification to Discord
func (d *DiscordProvider) Send(ctx context.Context, event Event) error {
	if !d.config.Enabled || d.config.WebhookURL == "" {
		return nil
	}

	embed := d.buildEmbed(event)

	payload := discordWebhookPayload{
		Username:  d.config.Username,
		AvatarURL: d.config.AvatarURL,
		Embeds:    []discordEmbed{embed},
	}

	if payload.Username == "" {
		payload.Username = "Autoplow"
	}

	return d.sendWebhook(ctx, payload)
}

// Test sends a test notification
func (d *DiscordProvider) Test(ctx context.Context) error {
	if d.config.WebhookURL == "" {
		return fmt.Errorf("webhook URL not configured")
	}

	event := Event{
		Type:      "test",
		Title:     "Test Notification",
		Message:   "This is a test notification from Autoplow. If you see this, Discord notifications are working!",
		Timestamp: time.Now(),
	}

	embed := d.buildEmbed(event)

	payload := discordWebhookPayload{
		Username:  d.config.Username,
		AvatarURL: d.config.AvatarURL,
		Embeds:    []discordEmbed{embed},
	}

	if payload.Username == "" {
		payload.Username = "Autoplow"
	}

	return d.sendWebhook(ctx, payload)
}

// SetConfig updates the Discord configuration
func (d *DiscordProvider) SetConfig(config DiscordConfig) {
	d.config = config
}

// GetConfig returns the current configuration
func (d *DiscordProvider) GetConfig() DiscordConfig {
	return d.config
}

// buildEmbed creates a Discord embed from an event
func (d *DiscordProvider) buildEmbed(event Event) discordEmbed {
	embed := discordEmbed{
		Title:       event.Title,
		Description: event.Message,
		Color:       d.getColorForEvent(event.Type),
		Timestamp:   event.Timestamp.Format(time.RFC3339),
		Footer: &discordEmbedFooter{
			Text: "Autoplow",
		},
	}

	// Add fields if present
	if len(event.Fields) > 0 {
		for name, value := range event.Fields {
			embed.Fields = append(embed.Fields, discordEmbedField{
				Name:   name,
				Value:  value,
				Inline: true,
			})
		}
	}

	return embed
}

// getColorForEvent returns a color based on event type
func (d *DiscordProvider) getColorForEvent(eventType EventType) int {
	switch eventType {
	case EventScanCompleted, EventUploadSuccess, EventThrottleStop:
		return 0x00FF00 // Green
	case EventScanFailed, EventUploadFailed, EventSystemError:
		return 0xFF0000 // Red
	case EventScanStarted, EventUploadStarted, EventThrottleStart:
		return 0xFFFF00 // Yellow
	case EventScanQueued, EventUploadQueued:
		return 0x0099FF // Blue
	default:
		return 0x808080 // Gray
	}
}

// sendWebhook sends a webhook payload to Discord
func (d *DiscordProvider) sendWebhook(ctx context.Context, payload discordWebhookPayload) error {
	return sendJSONRequest(ctx, d.client, "POST", d.config.WebhookURL, payload)
}

// Discord webhook payload structures
type discordWebhookPayload struct {
	Username  string         `json:"username,omitempty"`
	AvatarURL string         `json:"avatar_url,omitempty"`
	Content   string         `json:"content,omitempty"`
	Embeds    []discordEmbed `json:"embeds,omitempty"`
}

type discordEmbed struct {
	Title       string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	URL         string              `json:"url,omitempty"`
	Color       int                 `json:"color,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
	Footer      *discordEmbedFooter `json:"footer,omitempty"`
	Fields      []discordEmbedField `json:"fields,omitempty"`
}

type discordEmbedFooter struct {
	Text    string `json:"text,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

type discordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

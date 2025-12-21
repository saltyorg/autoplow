package handlers

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/notification"
)

// SettingsNotificationsPage renders the notifications settings page
func (h *Handlers) SettingsNotificationsPage(w http.ResponseWriter, r *http.Request) {
	providers, err := h.db.ListNotificationProviders()
	if err != nil {
		h.flashErr(w, "Failed to load notification providers")
		providers = []*database.NotificationProvider{}
	}

	logs, err := h.db.ListNotificationLogs(50)
	if err != nil {
		logs = []*database.NotificationLog{}
	}

	sent, failed, _ := h.db.GetNotificationStats()

	// Get all available event types
	eventTypes := []struct {
		Type  string
		Label string
	}{
		{string(notification.EventScanQueued), "Scan Queued"},
		{string(notification.EventScanStarted), "Scan Started"},
		{string(notification.EventScanCompleted), "Scan Completed"},
		{string(notification.EventScanFailed), "Scan Failed"},
		{string(notification.EventUploadQueued), "Upload Queued"},
		{string(notification.EventUploadStarted), "Upload Started"},
		{string(notification.EventUploadSuccess), "Upload Completed"},
		{string(notification.EventUploadFailed), "Upload Failed"},
		{string(notification.EventThrottleStart), "Throttle Started"},
		{string(notification.EventThrottleStop), "Throttle Stopped"},
		{string(notification.EventSystemError), "System Error"},
	}

	// Build provider data with subscriptions
	type providerData struct {
		*database.NotificationProvider
		Subscriptions map[string]bool
	}

	providersList := make([]providerData, 0, len(providers))
	for _, p := range providers {
		subs, _ := h.db.GetNotificationSubscriptions(p.ID)
		subMap := make(map[string]bool)
		for _, s := range subs {
			subMap[s.EventType] = s.Enabled
		}
		providersList = append(providersList, providerData{
			NotificationProvider: p,
			Subscriptions:        subMap,
		})
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":        "notifications",
		"Providers":  providersList,
		"EventTypes": eventTypes,
		"Logs":       logs,
		"Stats": map[string]int{
			"Sent":   sent,
			"Failed": failed,
		},
	})
}

// NotificationProviderNew renders the new provider form
func (h *Handlers) NotificationProviderNew(w http.ResponseWriter, r *http.Request) {
	providerType := r.URL.Query().Get("type")
	if providerType == "" {
		providerType = "discord"
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":          "notifications",
		"EditProvider": true,
		"Provider": &database.NotificationProvider{
			Type:    providerType,
			Enabled: true,
			Config:  make(map[string]string),
		},
		"IsNew": true,
	})
}

// NotificationProviderCreate creates a new notification provider
func (h *Handlers) NotificationProviderCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	provider := &database.NotificationProvider{
		Name:    r.FormValue("name"),
		Type:    r.FormValue("type"),
		Enabled: r.FormValue("enabled") == "on",
		Config:  make(map[string]string),
	}

	// Parse type-specific config
	switch provider.Type {
	case "discord":
		provider.Config["webhook_url"] = r.FormValue("webhook_url")
		provider.Config["username"] = r.FormValue("username")
		provider.Config["avatar_url"] = r.FormValue("avatar_url")
	case "webhook":
		provider.Config["url"] = r.FormValue("url")
		provider.Config["method"] = r.FormValue("method")
		provider.Config["content_type"] = r.FormValue("content_type")
		provider.Config["body"] = r.FormValue("body")
		provider.Config["headers"] = r.FormValue("headers")

		// Validate body template
		if err := notification.ValidateWebhookBody(provider.Config["body"]); err != nil {
			h.flashErr(w, "Invalid body template: "+err.Error())
			h.redirect(w, r, "/settings/notifications/new?type=webhook")
			return
		}
	}

	if provider.Name == "" {
		h.flashErr(w, "Name is required")
		h.redirect(w, r, "/settings/notifications/new")
		return
	}

	if err := h.db.CreateNotificationProvider(provider); err != nil {
		h.flashErr(w, "Failed to create provider: "+err.Error())
		h.redirect(w, r, "/settings/notifications/new")
		return
	}

	// Register with notification manager if available
	if h.notificationMgr != nil {
		h.registerProvider(provider)
	}

	h.flash(w, "Notification provider created")
	h.redirect(w, r, "/settings/notifications")
}

// NotificationProviderEdit renders the edit provider form
func (h *Handlers) NotificationProviderEdit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid provider ID")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	provider, err := h.db.GetNotificationProvider(id)
	if err != nil {
		h.flashErr(w, "Provider not found")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	subs, _ := h.db.GetNotificationSubscriptions(id)
	subMap := make(map[string]bool)
	for _, s := range subs {
		subMap[s.EventType] = s.Enabled
	}

	eventTypes := []struct {
		Type  string
		Label string
	}{
		{string(notification.EventScanQueued), "Scan Queued"},
		{string(notification.EventScanStarted), "Scan Started"},
		{string(notification.EventScanCompleted), "Scan Completed"},
		{string(notification.EventScanFailed), "Scan Failed"},
		{string(notification.EventUploadQueued), "Upload Queued"},
		{string(notification.EventUploadStarted), "Upload Started"},
		{string(notification.EventUploadSuccess), "Upload Completed"},
		{string(notification.EventUploadFailed), "Upload Failed"},
		{string(notification.EventThrottleStart), "Throttle Started"},
		{string(notification.EventThrottleStop), "Throttle Stopped"},
		{string(notification.EventSystemError), "System Error"},
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":           "notifications",
		"EditProvider":  true,
		"Provider":      provider,
		"Subscriptions": subMap,
		"EventTypes":    eventTypes,
		"IsNew":         false,
	})
}

// NotificationProviderUpdate updates a notification provider
func (h *Handlers) NotificationProviderUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid provider ID")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	provider, err := h.db.GetNotificationProvider(id)
	if err != nil {
		h.flashErr(w, "Provider not found")
		h.redirect(w, r, "/settings/notifications")
		return
	}

	provider.Name = r.FormValue("name")
	provider.Enabled = r.FormValue("enabled") == "on"

	// Parse type-specific config
	switch provider.Type {
	case "discord":
		provider.Config["webhook_url"] = r.FormValue("webhook_url")
		provider.Config["username"] = r.FormValue("username")
		provider.Config["avatar_url"] = r.FormValue("avatar_url")
	case "webhook":
		provider.Config["url"] = r.FormValue("url")
		provider.Config["method"] = r.FormValue("method")
		provider.Config["content_type"] = r.FormValue("content_type")
		provider.Config["body"] = r.FormValue("body")
		provider.Config["headers"] = r.FormValue("headers")

		// Validate body template
		if err := notification.ValidateWebhookBody(provider.Config["body"]); err != nil {
			h.flashErr(w, "Invalid body template: "+err.Error())
			h.redirect(w, r, "/settings/notifications/"+strconv.FormatInt(id, 10))
			return
		}
	}

	if err := h.db.UpdateNotificationProvider(provider); err != nil {
		h.flashErr(w, "Failed to update provider: "+err.Error())
		h.redirect(w, r, "/settings/notifications/"+strconv.FormatInt(id, 10))
		return
	}

	// Update subscriptions
	eventTypes := []string{
		string(notification.EventScanQueued),
		string(notification.EventScanStarted),
		string(notification.EventScanCompleted),
		string(notification.EventScanFailed),
		string(notification.EventUploadQueued),
		string(notification.EventUploadStarted),
		string(notification.EventUploadSuccess),
		string(notification.EventUploadFailed),
		string(notification.EventThrottleStart),
		string(notification.EventThrottleStop),
		string(notification.EventSystemError),
	}

	var subscriptionErr error
	for _, et := range eventTypes {
		enabled := r.FormValue("event_"+et) == "on"
		if err := h.db.SetNotificationSubscription(id, et, enabled); err != nil {
			subscriptionErr = err
		}
	}
	if subscriptionErr != nil {
		log.Error().Err(subscriptionErr).Msg("Failed to save some notification subscriptions")
	}

	// Re-register with notification manager
	if h.notificationMgr != nil {
		h.notificationMgr.UnregisterProvider(provider.Name)
		if provider.Enabled {
			h.registerProvider(provider)
		}
	}

	h.flash(w, "Notification provider updated")
	h.redirect(w, r, "/settings/notifications")
}

// NotificationProviderDelete deletes a notification provider
func (h *Handlers) NotificationProviderDelete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid provider ID", http.StatusBadRequest)
		return
	}

	provider, err := h.db.GetNotificationProvider(id)
	if err == nil && h.notificationMgr != nil {
		h.notificationMgr.UnregisterProvider(provider.Name)
	}

	if err := h.db.DeleteNotificationProvider(id); err != nil {
		h.jsonError(w, "Failed to delete provider", http.StatusInternalServerError)
		return
	}

	w.Header().Set("HX-Redirect", "/settings/notifications")
	h.jsonSuccess(w, "Provider deleted")
}

// NotificationProviderTest sends a test notification
func (h *Handlers) NotificationProviderTest(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.jsonError(w, "Invalid provider ID", http.StatusBadRequest)
		return
	}

	provider, err := h.db.GetNotificationProvider(id)
	if err != nil {
		h.jsonError(w, "Provider not found", http.StatusNotFound)
		return
	}

	// Create a temporary provider instance for testing
	switch provider.Type {
	case "discord":
		discord := notification.NewDiscordProvider(notification.DiscordConfig{
			WebhookURL: provider.Config["webhook_url"],
			Username:   provider.Config["username"],
			AvatarURL:  provider.Config["avatar_url"],
			Enabled:    true,
		})

		if err := discord.Test(r.Context()); err != nil {
			h.jsonError(w, "Test failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	case "webhook":
		webhook := notification.NewWebhookProvider(notification.WebhookConfig{
			URL:         provider.Config["url"],
			Method:      provider.Config["method"],
			Body:        provider.Config["body"],
			Headers:     notification.ParseWebhookHeaders(provider.Config["headers"]),
			ContentType: provider.Config["content_type"],
			Enabled:     true,
		})

		if err := webhook.Test(r.Context()); err != nil {
			h.jsonError(w, "Test failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		h.jsonError(w, "Unknown provider type", http.StatusBadRequest)
		return
	}

	h.jsonSuccess(w, "Test notification sent")
}

// NotificationLogsClear clears old notification logs
func (h *Handlers) NotificationLogsClear(w http.ResponseWriter, r *http.Request) {
	_, err := h.db.ClearNotificationLogs(0) // Clear all
	if err != nil {
		h.flashErr(w, "Failed to clear logs")
	} else {
		h.flash(w, "Notification logs cleared")
	}
	h.redirect(w, r, "/settings/notifications")
}

// registerProvider creates and registers a provider with the notification manager
func (h *Handlers) registerProvider(p *database.NotificationProvider) {
	if h.notificationMgr == nil {
		return
	}

	switch p.Type {
	case "discord":
		discord := notification.NewDiscordProvider(notification.DiscordConfig{
			WebhookURL: p.Config["webhook_url"],
			Username:   p.Config["username"],
			AvatarURL:  p.Config["avatar_url"],
			Enabled:    p.Enabled,
		})
		h.notificationMgr.RegisterProvider(p.Name, discord)
	case "webhook":
		webhook := notification.NewWebhookProvider(notification.WebhookConfig{
			URL:         p.Config["url"],
			Method:      p.Config["method"],
			Body:        p.Config["body"],
			Headers:     notification.ParseWebhookHeaders(p.Config["headers"]),
			ContentType: p.Config["content_type"],
			Enabled:     p.Enabled,
		})
		h.notificationMgr.RegisterProvider(p.Name, webhook)
	}
}

// InitNotificationProviders loads and registers all enabled providers
func (h *Handlers) InitNotificationProviders() {
	if h.notificationMgr == nil {
		return
	}

	providers, err := h.db.ListEnabledNotificationProviders()
	if err != nil {
		return
	}

	for _, p := range providers {
		h.registerProvider(p)
	}
}

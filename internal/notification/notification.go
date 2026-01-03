package notification

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/database"
)

// EventType represents the type of event that can trigger a notification
type EventType string

const (
	EventScanQueued    EventType = "scan_queued"
	EventScanStarted   EventType = "scan_started"
	EventScanCompleted EventType = "scan_completed"
	EventScanFailed    EventType = "scan_failed"
	EventUploadQueued  EventType = "upload_queued"
	EventUploadStarted EventType = "upload_started"
	EventUploadSuccess EventType = "upload_completed"
	EventUploadFailed  EventType = "upload_failed"
	EventThrottleStart EventType = "throttle_start"
	EventThrottleStop  EventType = "throttle_stop"
	EventSystemError   EventType = "system_error"
)

// Event represents a notification event
type Event struct {
	Type      EventType
	Title     string
	Message   string
	Fields    map[string]string
	Timestamp time.Time
}

// Provider is the interface for notification providers
type Provider interface {
	// Name returns the provider name
	Name() string

	// Send sends a notification
	Send(ctx context.Context, event Event) error

	// Test sends a test notification
	Test(ctx context.Context) error
}

// Manager handles notification dispatch
type Manager struct {
	db        *database.DB
	providers map[string]Provider
	mu        sync.RWMutex
	events    chan Event
	stopChan  chan struct{}
	wg        sync.WaitGroup

	// Running state
	running bool
}

// NewManager creates a new notification manager
func NewManager(db *database.DB) *Manager {
	return &Manager{
		db:        db,
		providers: make(map[string]Provider),
		events:    make(chan Event, 100),
		stopChan:  make(chan struct{}),
	}
}

// RegisterProvider registers a notification provider.
// If the manager is not running and this is the first provider, it will start automatically.
func (m *Manager) RegisterProvider(name string, provider Provider) {
	m.mu.Lock()
	wasEmpty := len(m.providers) == 0
	m.providers[name] = provider
	shouldStart := wasEmpty && !m.running
	m.mu.Unlock()

	log.Info().Str("provider", name).Msg("Registered notification provider")

	// Auto-start if this is the first provider
	if shouldStart {
		m.Start()
	}
}

// UnregisterProvider removes a notification provider.
// If this was the last provider, the manager will stop automatically.
func (m *Manager) UnregisterProvider(name string) {
	m.mu.Lock()
	delete(m.providers, name)
	shouldStop := m.running && len(m.providers) == 0
	m.mu.Unlock()

	log.Info().Str("provider", name).Msg("Unregistered notification provider")

	// Auto-stop if no providers remain
	if shouldStop {
		m.Stop()
	}
}

// GetProvider returns a provider by name
func (m *Manager) GetProvider(name string) (Provider, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.providers[name]
	return p, ok
}

// ListProviders returns all registered provider names
func (m *Manager) ListProviders() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.providers))
	for name := range m.providers {
		names = append(names, name)
	}
	return names
}

// Start starts the notification dispatcher.
// Returns true if the manager was started (providers exist), false otherwise.
func (m *Manager) Start() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return true
	}

	// Only start if we have providers
	if len(m.providers) == 0 {
		return false
	}

	m.running = true
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Notification dispatcher panicked")
			}
		}()
		m.dispatcher()
	})
	log.Info().Msg("Notification manager started")
	return true
}

// Stop stops the notification dispatcher
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	m.mu.Unlock()

	close(m.stopChan)
	m.wg.Wait()

	// Recreate stopChan for potential restart
	m.stopChan = make(chan struct{})

	log.Info().Msg("Notification manager stopped")
}

// IsRunning returns whether the manager is currently running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// Notify queues an event for notification
func (m *Manager) Notify(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	select {
	case m.events <- event:
	default:
		log.Warn().Str("type", string(event.Type)).Msg("Notification queue full, dropping event")
	}
}

// NotifySimple is a convenience method to send a simple notification
func (m *Manager) NotifySimple(eventType EventType, title, message string) {
	m.Notify(Event{
		Type:    eventType,
		Title:   title,
		Message: message,
	})
}

// dispatcher processes events and sends notifications
func (m *Manager) dispatcher() {
	for {
		select {
		case <-m.stopChan:
			return
		case event := <-m.events:
			m.dispatch(event)
		}
	}
}

// dispatch sends an event to all registered providers
func (m *Manager) dispatch(event Event) {
	m.mu.RLock()
	providers := make([]Provider, 0, len(m.providers))
	for _, p := range m.providers {
		providers = append(providers, p)
	}
	m.mu.RUnlock()

	if len(providers) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, provider := range providers {
		if err := provider.Send(ctx, event); err != nil {
			log.Error().
				Err(err).
				Str("provider", provider.Name()).
				Str("event", string(event.Type)).
				Msg("Failed to send notification")

			// Log to database
			m.logNotification(event, provider.Name(), err)
		} else {
			log.Debug().
				Str("provider", provider.Name()).
				Str("event", string(event.Type)).
				Msg("Notification sent")

			// Log success to database
			m.logNotification(event, provider.Name(), nil)
		}
	}
}

// logNotification logs a notification attempt to the database
func (m *Manager) logNotification(event Event, provider string, sendErr error) {
	status := "sent"
	errMsg := ""
	if sendErr != nil {
		status = "failed"
		errMsg = sendErr.Error()
	}

	err := m.db.LogNotification(&database.NotificationLog{
		Provider:  provider,
		EventType: string(event.Type),
		Title:     event.Title,
		Message:   event.Message,
		Status:    status,
		Error:     errMsg,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to log notification")
	}
}

// TestProvider sends a test notification to a specific provider
func (m *Manager) TestProvider(providerName string) error {
	m.mu.RLock()
	provider, ok := m.providers[providerName]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("provider not found: %s", providerName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return provider.Test(ctx)
}

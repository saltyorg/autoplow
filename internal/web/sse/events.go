package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// EventType represents the type of SSE event
type EventType string

const (
	EventScanQueued    EventType = "scan_queued"
	EventScanStarted   EventType = "scan_started"
	EventScanCompleted EventType = "scan_completed"
	EventScanFailed    EventType = "scan_failed"

	EventUploadQueued    EventType = "upload_queued"
	EventUploadStarted   EventType = "upload_started"
	EventUploadProgress  EventType = "upload_progress"
	EventUploadCompleted EventType = "upload_completed"
	EventUploadFailed    EventType = "upload_failed"

	EventUploadManagerPaused  EventType = "upload_manager_paused"
	EventUploadManagerResumed EventType = "upload_manager_resumed"

	EventSessionStarted EventType = "session_started"
	EventSessionEnded   EventType = "session_ended"

	EventThrottleChanged EventType = "throttle_changed"
	EventHeartbeat       EventType = "heartbeat"

	EventMatcharrRunStarted   EventType = "matcharr_run_started"
	EventMatcharrRunCompleted EventType = "matcharr_run_completed"
	EventMatcharrRunFailed    EventType = "matcharr_run_failed"
)

// Event represents an SSE event to be sent to clients
type Event struct {
	Type EventType `json:"type"`
	Data any       `json:"data"`
}

// Client represents a connected SSE client
type Client struct {
	ID       string
	Messages chan []byte
	Done     chan struct{}
}

// Broker manages SSE client connections and event broadcasting
type Broker struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	broadcast  chan Event
	done       chan struct{}
	mu         sync.RWMutex
}

// NewBroker creates a new SSE broker
func NewBroker() *Broker {
	b := &Broker{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Event, 100),
		done:       make(chan struct{}),
	}
	go b.run()
	return b
}

// run handles client registration and event broadcasting
func (b *Broker) run() {
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-b.done:
			// Graceful shutdown - close all client channels
			b.mu.Lock()
			for _, client := range b.clients {
				close(client.Messages)
			}
			b.clients = make(map[string]*Client)
			b.mu.Unlock()
			log.Debug().Msg("SSE broker stopped")
			return

		case client := <-b.register:
			b.mu.Lock()
			b.clients[client.ID] = client
			b.mu.Unlock()
			log.Debug().Str("client_id", client.ID).Int("total_clients", len(b.clients)).Msg("SSE client connected")

		case client := <-b.unregister:
			b.mu.Lock()
			if _, ok := b.clients[client.ID]; ok {
				delete(b.clients, client.ID)
				close(client.Messages)
			}
			b.mu.Unlock()
			log.Debug().Str("client_id", client.ID).Int("total_clients", len(b.clients)).Msg("SSE client disconnected")

		case event := <-b.broadcast:
			data, err := json.Marshal(event)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal SSE event")
				continue
			}

			message := formatSSEMessage(string(event.Type), data)

			b.mu.RLock()
			for _, client := range b.clients {
				select {
				case client.Messages <- message:
				default:
					// Client buffer full, skip this message
					log.Warn().Str("client_id", client.ID).Msg("SSE client buffer full, dropping message")
				}
			}
			b.mu.RUnlock()

		case <-heartbeatTicker.C:
			// Send heartbeat to all clients
			b.Broadcast(Event{Type: EventHeartbeat, Data: map[string]any{"time": time.Now().Unix()}})
		}
	}
}

// Broadcast sends an event to all connected clients
func (b *Broker) Broadcast(event Event) {
	select {
	case b.broadcast <- event:
	default:
		log.Warn().Str("event_type", string(event.Type)).Msg("SSE broadcast channel full, dropping event")
	}
}

// Stop gracefully shuts down the broker
func (b *Broker) Stop() {
	close(b.done)
}

// ServeHTTP handles SSE connections
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Check if flushing is supported
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Create client
	client := &Client{
		ID:       fmt.Sprintf("%p-%d", r, time.Now().UnixNano()),
		Messages: make(chan []byte, 32),
		Done:     make(chan struct{}),
	}

	// Register client
	b.register <- client

	// Ensure cleanup on exit (non-blocking to avoid deadlock during shutdown)
	defer func() {
		select {
		case b.unregister <- client:
		case <-b.done:
			// Broker is shutting down, skip unregister
		}
	}()

	// Send initial connection event
	initialEvent := Event{
		Type: "connected",
		Data: map[string]any{
			"client_id": client.ID,
			"time":      time.Now().Unix(),
		},
	}
	data, _ := json.Marshal(initialEvent)
	fmt.Fprintf(w, "event: connected\ndata: %s\n\n", data)
	flusher.Flush()

	// Stream events to client
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-client.Messages:
			if !ok {
				return
			}
			_, _ = w.Write(msg)
			flusher.Flush()
		}
	}
}

// ClientCount returns the number of connected clients
func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// formatSSEMessage formats an SSE message with event type and data
func formatSSEMessage(eventType string, data []byte) []byte {
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", eventType, data)
}

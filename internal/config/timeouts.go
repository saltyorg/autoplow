package config

import "time"

// TimeoutConfig holds timeout settings for various operations.
// These can be configured via CLI flags to tune performance for different environments.
type TimeoutConfig struct {
	// HTTPClient is the timeout for HTTP client requests to external services
	// (media servers, webhooks, etc). Default: 30s
	HTTPClient time.Duration

	// WebSocketPing is the interval between WebSocket keepalive pings.
	// Default: 30s
	WebSocketPing time.Duration

	// ScanOperation is the timeout for individual scan operations.
	// Default: 60s
	ScanOperation time.Duration
}

// DefaultTimeoutConfig returns the default timeout configuration
func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		HTTPClient:    30 * time.Second,
		WebSocketPing: 30 * time.Second,
		ScanOperation: 60 * time.Second,
	}
}

// global instance that can be set at startup
var globalTimeouts = DefaultTimeoutConfig()

// SetGlobalTimeouts sets the global timeout configuration
func SetGlobalTimeouts(cfg *TimeoutConfig) {
	globalTimeouts = cfg
}

// GetTimeouts returns the global timeout configuration
func GetTimeouts() *TimeoutConfig {
	return globalTimeouts
}

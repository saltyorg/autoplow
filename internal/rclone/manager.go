package rclone

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

// ManagerConfig holds process manager configuration
type ManagerConfig struct {
	// Mode determines whether Autoplow manages the rclone process or connects to an external one
	// "managed" = Autoplow starts/stops rclone RCD process
	// "unmanaged" = Connect to an existing rclone RCD instance
	Managed bool `json:"managed"`

	// Managed mode fields
	BinaryPath    string        `json:"binary_path"` // Path to rclone binary
	ConfigPath    string        `json:"config_path"` // Path to rclone.conf (optional)
	AutoStart     bool          `json:"auto_start"`  // Start on manager creation
	RestartOnFail bool          `json:"restart_on_fail"`
	RestartDelay  time.Duration `json:"restart_delay"`
	MaxRestarts   int           `json:"max_restarts"` // Max restarts before giving up (0 = unlimited)
	HealthCheck   time.Duration `json:"health_check"` // Health check interval

	// Shared fields (both modes)
	Address  string `json:"address"`  // 127.0.0.1:5572 (unmanaged) or just port for managed
	Username string `json:"username"` // RCD authentication
	Password string `json:"password"`

	// Managed mode logging (ignored for unmanaged mode)
	LogLevel          string `json:"log_level"`
	LogFilePath       string `json:"log_file_path"`
	LogFileMaxAge     string `json:"log_file_max_age"`
	LogFileMaxBackups int    `json:"log_file_max_backups"`
	LogFileMaxSize    string `json:"log_file_max_size"`
	LogFileCompress   bool   `json:"log_file_compress"`
}

// DefaultManagerConfig returns default manager configuration
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		Managed:           true, // Default to managed mode
		BinaryPath:        "rclone",
		ConfigPath:        "~/.config/rclone/rclone.conf",
		Address:           "127.0.0.1:5572",
		AutoStart:         true,
		RestartOnFail:     true,
		RestartDelay:      5 * time.Second,
		MaxRestarts:       10,
		HealthCheck:       30 * time.Second,
		LogLevel:          "NOTICE",
		LogFileMaxAge:     "0s",
		LogFileMaxBackups: 0,
		LogFileMaxSize:    "0",
		LogFileCompress:   false,
	}
}

// Manager manages the rclone RCD subprocess
type Manager struct {
	config       ManagerConfig
	client       *Client
	cmd          *exec.Cmd
	running      bool
	startedAt    time.Time
	restartCount int
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	// Cached rclone config data (populated after RCD starts)
	providers     []Provider
	configRemotes []string
	providersMu   sync.RWMutex

	// Callback invoked when RCD becomes ready (after start or restart)
	onReady   func()
	onReadyMu sync.RWMutex
}

// Status represents the current rclone RCD status
type Status struct {
	Running      bool          `json:"running"`
	PID          int           `json:"pid,omitempty"`
	Address      string        `json:"address"`
	RestartCount int           `json:"restart_count"`
	Uptime       time.Duration `json:"uptime,omitempty"`
	Version      string        `json:"version,omitempty"`
}

// NewManager creates a new rclone process manager
func NewManager(config ManagerConfig) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	// Create client
	m.client = NewClient(ClientConfig{
		Address:  config.Address,
		Username: config.Username,
		Password: config.Password,
		Timeout:  30 * time.Second,
	})

	return m
}

// Start starts the rclone RCD process (managed mode) or connects to an existing one (unmanaged mode)
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil // Already running
	}

	// Validate credentials for both modes
	if m.config.Username == "" || m.config.Password == "" {
		return fmt.Errorf("rclone RCD credentials not configured")
	}

	// In unmanaged mode, just mark as running and wait for ready
	if !m.config.Managed {
		log.Info().
			Str("address", m.config.Address).
			Msg("Connecting to external rclone RCD instance (unmanaged mode)")

		m.running = true
		m.startedAt = time.Now()

		// Wait for RCD to be ready
		if err := m.waitForReady(); err != nil {
			m.running = false
			return fmt.Errorf("failed to connect to rclone RCD at %s: %w", m.config.Address, err)
		}

		return nil
	}

	// Managed mode - start the rclone process
	// Find an available port, starting from the configured port
	host := "127.0.0.1"
	startPort := 5572 // default
	parts := strings.Split(m.config.Address, ":")
	if len(parts) == 2 {
		host = parts[0]
		if p, err := strconv.Atoi(parts[1]); err == nil {
			startPort = p
		}
	}

	availablePort, err := FindAvailablePort(startPort)
	if err != nil {
		return fmt.Errorf("cannot start rclone RCD: %w", err)
	}
	m.config.Address = fmt.Sprintf("%s:%d", host, availablePort)

	// Update client with new address
	m.client = NewClient(ClientConfig{
		Address:  m.config.Address,
		Username: m.config.Username,
		Password: m.config.Password,
		Timeout:  30 * time.Second,
	})

	if availablePort != startPort {
		log.Info().Int("configured_port", startPort).Int("available_port", availablePort).Msg("Configured port in use, using next available port")
	}

	// Validate binary path exists
	binaryPath := strings.Trim(m.config.BinaryPath, "\"")
	if binaryPath == "" || binaryPath == "rclone" {
		// Try to find rclone binary
		binaryPath = FindRcloneBinary()
		if binaryPath == "rclone" {
			// FindRcloneBinary returns "rclone" if not found, verify it exists in PATH
			if _, err := exec.LookPath("rclone"); err != nil {
				return fmt.Errorf("rclone binary not found: checked /usr/bin/rclone, /usr/local/bin/rclone, and PATH")
			}
		}
		log.Debug().Str("path", binaryPath).Msg("Auto-detected rclone binary")
	} else {
		// Explicit path provided, verify it exists
		if _, err := os.Stat(binaryPath); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("rclone binary not found at configured path: %s", binaryPath)
			}
			return fmt.Errorf("failed to access rclone binary at %s: %w", binaryPath, err)
		}
		log.Debug().Str("path", binaryPath).Msg("Using configured rclone binary")
	}
	m.config.BinaryPath = binaryPath

	// Validate config path if specified
	configPath := strings.Trim(m.config.ConfigPath, "\"")
	// Expand ~ to home directory
	if strings.HasPrefix(configPath, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			configPath = home + configPath[1:]
		}
	}
	m.config.ConfigPath = configPath
	if configPath != "" {
		if _, err := os.Stat(configPath); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("rclone config file not found at: %s", m.config.ConfigPath)
			}
			return fmt.Errorf("failed to access rclone config file at %s: %w", m.config.ConfigPath, err)
		}
		log.Debug().Str("path", configPath).Msg("Using rclone config file")
	} else {
		log.Debug().Msg("No rclone config file specified, using rclone defaults")
	}

	// Build command arguments
	args := []string{
		"rcd",
		"--rc-addr", m.config.Address,
		"--rc-web-gui-no-open-browser",
	}

	if m.config.ConfigPath != "" {
		args = append(args, "--config", m.config.ConfigPath)
	}

	args = append(args, "--rc-user", m.config.Username)
	args = append(args, "--rc-pass", m.config.Password)

	if m.config.LogLevel != "" {
		args = append(args, "--log-level", m.config.LogLevel)
	}
	if m.config.LogFilePath != "" {
		if err := ensureLogDir(m.config.LogFilePath); err != nil {
			log.Warn().Err(err).Str("path", m.config.LogFilePath).Msg("Failed to prepare rclone log directory")
		}
		args = append(args, "--log-file", m.config.LogFilePath)
		if m.config.LogFileMaxAge != "" {
			args = append(args, "--log-file-max-age", m.config.LogFileMaxAge)
		}
		if m.config.LogFileMaxBackups >= 0 {
			args = append(args, "--log-file-max-backups", strconv.Itoa(m.config.LogFileMaxBackups))
		}
		if m.config.LogFileMaxSize != "" {
			args = append(args, "--log-file-max-size", m.config.LogFileMaxSize)
		}
		if m.config.LogFileCompress {
			args = append(args, "--log-file-compress")
		}
	}

	m.cmd = exec.Command(m.config.BinaryPath, args...)
	// Keep rclone in its own process group so SIGINT doesn't kill it before we stop jobs.
	setProcessGroup(m.cmd)

	// Redirect stdout/stderr to logs
	m.cmd.Stdout = &rcloneLogWriter{level: "info"}
	m.cmd.Stderr = &rcloneLogWriter{level: "error"}

	// Start the process
	if err := m.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start rclone: %w", err)
	}

	m.running = true
	m.startedAt = time.Now()

	log.Info().
		Str("binary", m.config.BinaryPath).
		Str("address", m.config.Address).
		Int("pid", m.cmd.Process.Pid).
		Msg("Started rclone RCD process (managed mode)")

	// Start process monitor goroutine
	m.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Rclone process monitor panicked")
			}
		}()
		m.monitorProcess()
	})

	// Wait for RCD to be ready
	if err := m.waitForReady(); err != nil {
		log.Warn().Err(err).Msg("Rclone RCD not responding yet, will retry")
	}

	return nil
}

// PrepareShutdown disables automatic restarts without stopping the process.
// This lets dependents shut down cleanly while preventing restart loops.
func (m *Manager) PrepareShutdown() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
}

// Stop stops the rclone RCD process gracefully (managed mode) or disconnects (unmanaged mode)
func (m *Manager) Stop() error {
	m.mu.Lock()

	if !m.running {
		m.mu.Unlock()
		return nil
	}

	// In unmanaged mode, just mark as not running (don't stop the external process)
	if !m.config.Managed {
		log.Info().Str("address", m.config.Address).Msg("Disconnecting from external rclone RCD instance (unmanaged mode)")
		m.running = false
		m.mu.Unlock()
		return nil
	}

	// Managed mode - actually stop the process
	if m.cmd == nil || m.cmd.Process == nil {
		m.running = false
		m.mu.Unlock()
		return nil
	}

	log.Info().Int("pid", m.cmd.Process.Pid).Msg("Stopping rclone RCD process")

	// Cancel context to stop monitor goroutine
	m.cancel()

	// Send SIGTERM for graceful shutdown
	if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		log.Warn().Err(err).Msg("Failed to send SIGTERM to rclone")
	}

	m.running = false
	m.mu.Unlock()

	// Wait for monitor goroutine to finish (it will handle process exit)
	// Use a timeout to prevent hanging forever
	waitDone := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		log.Info().Msg("Rclone RCD process stopped")
	case <-time.After(15 * time.Second):
		// Force kill if still running
		m.mu.Lock()
		if m.cmd != nil && m.cmd.Process != nil {
			log.Warn().Msg("Rclone RCD did not stop gracefully, sending SIGKILL")
			if err := m.cmd.Process.Kill(); err != nil {
				if !strings.Contains(err.Error(), "process already finished") {
					log.Error().Err(err).Msg("Failed to kill rclone process")
				}
			}
		}
		m.mu.Unlock()
		// Wait again for monitor to finish
		<-waitDone
	}

	m.mu.Lock()
	m.cmd = nil
	m.mu.Unlock()

	return nil
}

// Restart restarts the rclone RCD process
func (m *Manager) Restart() error {
	if err := m.Stop(); err != nil {
		return fmt.Errorf("failed to stop rclone: %w", err)
	}

	// Reset context
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.restartCount = 0

	time.Sleep(time.Second) // Brief pause before restart

	return m.Start()
}

// IsRunning returns whether rclone RCD is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// Managed reports whether Autoplow owns the rclone process.
func (m *Manager) Managed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Managed
}

// Status returns the current status
func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := Status{
		Running:      m.running,
		Address:      m.config.Address,
		RestartCount: m.restartCount,
	}

	if m.running && m.cmd != nil && m.cmd.Process != nil {
		status.PID = m.cmd.Process.Pid
		status.Uptime = time.Since(m.startedAt)

		// Try to get version
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if version, err := m.client.Version(ctx); err == nil {
			status.Version = version.Version
		}
	}

	return status
}

// Client returns the RCD client
func (m *Manager) Client() *Client {
	return m.client
}

// UpdateConfig updates the manager configuration
// This should be called before Start/Restart if settings have changed
func (m *Manager) UpdateConfig(config ManagerConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.config = config

	// Recreate client with new credentials/address
	m.client = NewClient(ClientConfig{
		Address:  config.Address,
		Username: config.Username,
		Password: config.Password,
		Timeout:  30 * time.Second,
	})
}

// Healthy checks if rclone RCD is responding
func (m *Manager) Healthy(ctx context.Context) bool {
	if !m.IsRunning() {
		return false
	}
	return m.client.Ping(ctx) == nil
}

// monitorProcess monitors the subprocess and restarts if needed
func (m *Manager) monitorProcess() {
	// Wait for process to exit
	processExit := make(chan error, 1)
	go func() {
		if m.cmd != nil {
			processExit <- m.cmd.Wait()
		}
	}()

	// Health check ticker
	var healthTicker *time.Ticker
	var healthChan <-chan time.Time
	if m.config.HealthCheck > 0 {
		healthTicker = time.NewTicker(m.config.HealthCheck)
		healthChan = healthTicker.C
		defer healthTicker.Stop()
	}

	for {
		select {
		case <-m.ctx.Done():
			return

		case err := <-processExit:
			m.mu.Lock()
			m.running = false
			uptime := time.Since(m.startedAt)
			m.mu.Unlock()

			// Check if shutdown was requested - don't log error or restart
			select {
			case <-m.ctx.Done():
				log.Debug().Msg("Rclone RCD process stopped during shutdown")
				return
			default:
			}

			if err != nil {
				log.Error().Err(err).Msg("Rclone RCD process exited with error")
			} else {
				log.Info().Msg("Rclone RCD process exited")
			}

			// Don't restart if process exited very quickly (likely a startup failure like port in use)
			if uptime < 5*time.Second {
				log.Error().Dur("uptime", uptime).Msg("Rclone RCD exited too quickly, not restarting (check if another instance is running)")
				return
			}

			// Check if we should restart
			if m.config.RestartOnFail && (m.config.MaxRestarts == 0 || m.restartCount < m.config.MaxRestarts) {
				m.restartCount++
				log.Info().
					Int("restart_count", m.restartCount).
					Dur("delay", m.config.RestartDelay).
					Msg("Restarting rclone RCD")

				time.Sleep(m.config.RestartDelay)

				if err := m.Start(); err != nil {
					log.Error().Err(err).Msg("Failed to restart rclone RCD")
				}
			}
			return

		case <-healthChan:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.client.Ping(ctx); err != nil {
				log.Warn().Err(err).Msg("Rclone RCD health check failed")
			}
			cancel()
		}
	}
}

// waitForReady polls until RCD responds to ping
func (m *Manager) waitForReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for rclone RCD to be ready")
		case <-ticker.C:
			if err := m.client.Ping(ctx); err == nil {
				log.Info().Msg("Rclone RCD is ready")
				// Populate cache after RCD is ready
				go m.RefreshCache()
				// Invoke callback for post-start setup (e.g., applying saved settings)
				m.invokeOnReady()
				return nil
			}
		}
	}
}

// RefreshCache refreshes the cached providers and remotes from rclone RCD
func (m *Manager) RefreshCache() {
	if !m.IsRunning() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch providers
	providers, err := m.client.GetProviders(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to fetch rclone providers for cache")
	} else {
		m.providersMu.Lock()
		m.providers = providers
		m.providersMu.Unlock()
		log.Info().Int("count", len(providers)).Msg("Cached rclone providers")
	}

	// Fetch configured remotes
	remotes, err := m.client.ListRemotes(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to fetch rclone remotes for cache")
	} else {
		m.providersMu.Lock()
		m.configRemotes = remotes
		m.providersMu.Unlock()
		log.Info().Int("count", len(remotes)).Msg("Cached rclone remotes")
	}
}

// Providers returns the cached list of backend providers
func (m *Manager) Providers() []Provider {
	m.providersMu.RLock()
	defer m.providersMu.RUnlock()
	return m.providers
}

// ConfiguredRemotes returns the cached list of configured remotes
func (m *Manager) ConfiguredRemotes() []string {
	m.providersMu.RLock()
	defer m.providersMu.RUnlock()
	return m.configRemotes
}

// rcloneLogWriter implements io.Writer for rclone output logging
type rcloneLogWriter struct {
	level string
}

func (w *rcloneLogWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	if len(msg) == 0 {
		return len(p), nil
	}

	// Remove trailing newline
	if msg[len(msg)-1] == '\n' {
		msg = msg[:len(msg)-1]
	}

	// Parse rclone log level from message (format: "2025/01/01 00:00:00 LEVEL : message")
	// Rclone levels: DEBUG, INFO, NOTICE, ERROR
	logLevel := "debug"
	if strings.Contains(msg, " ERROR ") {
		logLevel = "error"
	} else if strings.Contains(msg, " NOTICE ") {
		logLevel = "info"
	} else if strings.Contains(msg, " INFO ") {
		logLevel = "info"
	}

	switch logLevel {
	case "error":
		log.Error().Str("source", "rclone").Msg(msg)
	case "info":
		log.Info().Str("source", "rclone").Msg(msg)
	default:
		log.Debug().Str("source", "rclone").Msg(msg)
	}

	return len(p), nil
}

func ensureLogDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

// FindRcloneBinary attempts to find the rclone binary
func FindRcloneBinary() string {
	// Check common locations
	paths := []string{
		"/usr/bin/rclone",
		"/usr/local/bin/rclone",
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Try PATH
	if path, err := exec.LookPath("rclone"); err == nil {
		return path
	}

	return "rclone" // Default, let exec fail if not found
}

// IsManaged returns whether the manager is in managed mode
func (m *Manager) IsManaged() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Managed
}

// SetOnReady sets a callback that will be invoked when RCD becomes ready
// (after initial start or after a restart). The callback is called asynchronously.
func (m *Manager) SetOnReady(fn func()) {
	m.onReadyMu.Lock()
	defer m.onReadyMu.Unlock()
	m.onReady = fn
}

// invokeOnReady calls the onReady callback if set
func (m *Manager) invokeOnReady() {
	m.onReadyMu.RLock()
	fn := m.onReady
	m.onReadyMu.RUnlock()

	if fn != nil {
		go fn()
	}
}

// CheckPortAvailable checks if a port is available for binding
func CheckPortAvailable(port int) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %d is not available: %w", port, err)
	}
	listener.Close()
	return nil
}

// FindAvailablePort finds an available port starting from the given port
func FindAvailablePort(startPort int) (int, error) {
	for port := startPort; port < startPort+100; port++ {
		if err := CheckPortAvailable(port); err == nil {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no available port found in range %d-%d", startPort, startPort+99)
}

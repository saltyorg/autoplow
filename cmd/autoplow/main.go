package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/inotify"
	"github.com/saltyorg/autoplow/internal/notification"
	"github.com/saltyorg/autoplow/internal/polling"
	"github.com/saltyorg/autoplow/internal/rclone"
	"github.com/saltyorg/autoplow/internal/throttle"
	"github.com/saltyorg/autoplow/internal/uploader"
	"github.com/saltyorg/autoplow/internal/web"
	"github.com/saltyorg/autoplow/internal/web/handlers"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// CLI flags
var (
	port        int
	bind        string
	allowSubnet string
	dbPath      string
	verbosity   int

	// Timeout flags (advanced)
	httpTimeout   time.Duration
	websocketPing time.Duration
	scanTimeout   time.Duration
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "autoplow",
		Short: "Autoplow - Media automation server",
		Long:  `Autoplow is a media automation server for managing scans, uploads, and media library synchronization.`,
		RunE:  run,
	}

	// Flags
	rootCmd.Flags().IntVarP(&port, "port", "p", 0, "HTTP server port (required, or set PORT env var)")
	rootCmd.Flags().StringVarP(&bind, "bind", "b", "", "IP address to bind to (e.g., 127.0.0.1, 0.0.0.0)")
	rootCmd.Flags().StringVarP(&allowSubnet, "allow-subnet", "a", "", "CIDR subnet allowed to connect (e.g., 192.168.1.0/24)")
	rootCmd.Flags().StringVarP(&dbPath, "db", "d", "./autoplow.db", "SQLite database path (or set DB_PATH env var)")
	rootCmd.Flags().CountVarP(&verbosity, "verbose", "v", "Increase verbosity (-v debug, -vv trace)")

	// Advanced timeout flags
	rootCmd.Flags().DurationVar(&httpTimeout, "http-timeout", 30*time.Second, "Timeout for HTTP client requests to external services")
	rootCmd.Flags().DurationVar(&websocketPing, "websocket-ping", 30*time.Second, "Interval between WebSocket keepalive pings")
	rootCmd.Flags().DurationVar(&scanTimeout, "scan-timeout", 60*time.Second, "Timeout for individual scan operations")

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("autoplow %s (commit: %s, built: %s)\n", version, commit, date)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Check for PORT env var if flag not set
	if port == 0 {
		if envPort := os.Getenv("PORT"); envPort != "" {
			if _, err := fmt.Sscanf(envPort, "%d", &port); err != nil {
				return fmt.Errorf("invalid PORT environment variable %q: %w", envPort, err)
			}
		}
	}

	// Check for DB_PATH env var if using default
	if dbPath == "./autoplow.db" {
		if envDB := os.Getenv("DB_PATH"); envDB != "" {
			dbPath = envDB
		}
	}

	// Validate port
	if port == 0 {
		return fmt.Errorf("--port flag or PORT environment variable is required")
	}

	// Validate bind address if provided
	if bind != "" {
		if ip := net.ParseIP(bind); ip == nil {
			return fmt.Errorf("invalid bind address: %s", bind)
		}
	}

	// Validate and parse allow-subnet if provided
	var allowedNet *net.IPNet
	if allowSubnet != "" {
		_, parsedNet, err := net.ParseCIDR(allowSubnet)
		if err != nil {
			return fmt.Errorf("invalid allow-subnet CIDR: %s", allowSubnet)
		}
		allowedNet = parsedNet
	}

	// Setup logging
	setupLogging(verbosity)

	// Configure global timeouts
	config.SetGlobalTimeouts(&config.TimeoutConfig{
		HTTPClient:    httpTimeout,
		WebSocketPing: websocketPing,
		ScanOperation: scanTimeout,
	})

	// Warn if binding to all interfaces without an allow list
	if (bind == "" || bind == "0.0.0.0" || bind == "::") && allowSubnet == "" {
		log.Warn().Msg("Server is accessible from all interfaces without subnet restrictions. Consider using --bind or --allow-subnet for security.")
	}

	log.Info().
		Str("version", version).
		Int("port", port).
		Str("bind", bind).
		Str("allow_subnet", allowSubnet).
		Str("database", dbPath).
		Msg("Starting Autoplow")

	// Initialize database
	db, err := database.New(dbPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer db.Close()

	// Run migrations
	if err := db.Migrate(); err != nil {
		log.Fatal().Err(err).Msg("Failed to run database migrations")
	}

	// Create web server with bind address and allowed subnet
	server := web.NewServer(db, port, bind, allowedNet)

	// Initialize rclone manager with config from database
	// This also ensures credentials exist (generates random ones if not set)
	rcloneConfig := handlers.LoadRcloneConfigFromDB(db)
	// Override binary path with auto-detected if not explicitly set in DB
	if binaryPath, _ := db.GetSetting("rclone.binary_path"); binaryPath == "" || binaryPath == "/usr/bin/rclone" {
		rcloneConfig.BinaryPath = rclone.FindRcloneBinary()
	}

	// Check if uploads are enabled - this controls auto-start behavior
	uploadsEnabled := true // default
	if val, _ := db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}
	// Only auto-start rclone if uploads are enabled AND auto_start setting is true
	if !uploadsEnabled {
		rcloneConfig.AutoStart = false
	}

	rcloneMgr := rclone.NewManager(rcloneConfig)
	server.SetRcloneManager(rcloneMgr)

	// Get processor and SSE broker
	processor := server.Processor()
	sseBroker := server.SSEBroker()
	processor.SetSSEBroker(sseBroker)

	// Start processor (always needed for scanning)
	processor.Start()
	defer processor.Stop()

	// Initialize upload subsystem only if uploads are enabled
	var uploadMgr *uploader.Manager
	var throttleMgr *throttle.Manager

	if uploadsEnabled {
		// Initialize upload manager
		uploadConfig := uploader.DefaultConfig()
		uploadMgr = uploader.New(db, rcloneMgr, uploadConfig)
		server.SetUploadManager(uploadMgr)
		processor.SetUploadQueuer(uploadMgr)
		uploadMgr.SetSSEBroker(sseBroker)
		uploadMgr.Start()

		// Start rclone manager if auto-start is enabled
		if rcloneConfig.AutoStart {
			if err := rcloneMgr.Start(); err != nil {
				log.Warn().Err(err).Msg("Failed to auto-start rclone RCD")
			}
		}

		// Initialize throttle manager with config loaded from database
		throttleConfig := throttle.LoadConfigFromDB(db)
		throttleMgr = throttle.New(db, server.TargetsManager(), rcloneMgr, throttleConfig)
		server.SetThrottleManager(throttleMgr)
		uploadMgr.SetSkipChecker(throttleMgr)
		throttleMgr.SetSSEBroker(sseBroker)
		throttleMgr.Start()

		log.Info().Msg("Upload subsystem started")
	} else {
		log.Info().Msg("Upload subsystem disabled")
	}

	// Defer cleanup for upload subsystem
	defer func() {
		if throttleMgr != nil {
			throttleMgr.Stop()
		}
		if uploadMgr != nil {
			uploadMgr.Stop()
		}
		if rcloneMgr.IsRunning() {
			if err := rcloneMgr.Stop(); err != nil {
				log.Error().Err(err).Msg("Failed to stop rclone RCD")
			}
		}
	}()

	// Initialize notification manager
	notificationMgr := notification.NewManager(db)
	defer notificationMgr.Stop()
	server.SetNotificationManager(notificationMgr)

	// Start notification manager (only starts if providers are configured)
	if started := notificationMgr.Start(); !started {
		log.Debug().Msg("Notification manager not started (no providers configured)")
	}

	// Initialize inotify watcher
	inotifyWatcher, err := inotify.New(db, processor)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to initialize inotify watcher")
	} else {
		defer inotifyWatcher.Stop()
		server.SetInotifyManager(inotifyWatcher)
		// Wire upload manager to inotify for direct upload queuing (if uploads enabled)
		if uploadMgr != nil {
			inotifyWatcher.SetUploadManager(uploadMgr)
		}
		if started, err := inotifyWatcher.Start(); err != nil {
			log.Warn().Err(err).Msg("Failed to start inotify watcher")
		} else if !started {
			log.Debug().Msg("Inotify watcher not started (no triggers configured)")
		} else {
			// Scan existing files in watched paths for upload
			go inotifyWatcher.ScanExistingFiles()
		}
	}

	// Initialize polling watcher
	pollingWatcher := polling.New(db, processor)
	defer pollingWatcher.Stop()
	server.SetPollingManager(pollingWatcher)
	// Wire upload manager to polling for direct upload queuing (if uploads enabled)
	if uploadMgr != nil {
		pollingWatcher.SetUploadManager(uploadMgr)
	}
	if started, err := pollingWatcher.Start(); err != nil {
		log.Warn().Err(err).Msg("Failed to start polling watcher")
	} else if !started {
		log.Debug().Msg("Polling watcher not started (no triggers configured)")
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		// Stop rclone first to prevent restart attempts when child process receives SIGINT
		if rcloneMgr.IsRunning() {
			if err := rcloneMgr.Stop(); err != nil {
				log.Debug().Err(err).Msg("Error stopping rclone during shutdown (ignored)")
			}
		}
		cancel()
	}()

	// Start server
	if err := server.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Server error")
	}

	log.Info().Msg("Autoplow stopped")
	return nil
}

func setupLogging(verbosity int) {
	// Pretty console output
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}

	switch verbosity {
	case 0:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case 1:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default: // 2+
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	}

	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

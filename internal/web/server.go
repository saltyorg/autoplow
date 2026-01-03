package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/inotify"
	"github.com/saltyorg/autoplow/internal/matcharr"
	"github.com/saltyorg/autoplow/internal/notification"
	"github.com/saltyorg/autoplow/internal/plexautolang"
	"github.com/saltyorg/autoplow/internal/polling"
	"github.com/saltyorg/autoplow/internal/processor"
	"github.com/saltyorg/autoplow/internal/rclone"
	"github.com/saltyorg/autoplow/internal/targets"
	"github.com/saltyorg/autoplow/internal/throttle"
	"github.com/saltyorg/autoplow/internal/uploader"
	"github.com/saltyorg/autoplow/internal/web/handlers"
	"github.com/saltyorg/autoplow/internal/web/middleware"
	"github.com/saltyorg/autoplow/internal/web/sse"
)

//go:embed templates/*
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

// Server represents the web server
type Server struct {
	db              *database.DB
	port            int
	bind            string
	allowedNet      *net.IPNet
	router          *chi.Mux
	templates       map[string]*template.Template
	authService     *auth.AuthService
	apiKeyService   *auth.APIKeyService
	sseBroker       *sse.Broker
	processor       *processor.Processor
	targetsMgr      *targets.Manager
	rcloneMgr       *rclone.Manager
	uploadMgr       *uploader.Manager
	throttleMgr     *throttle.Manager
	notificationMgr *notification.Manager
	inotifyMgr      *inotify.Watcher
	pollingMgr      *polling.Poller
	matcharrMgr     *matcharr.Manager
	plexAutoLangMgr *plexautolang.Manager
	handlers        *handlers.Handlers
}

// NewServer creates a new web server
func NewServer(db *database.DB, port int, bind string, allowedNet *net.IPNet) *Server {
	targetsMgr := targets.NewManager(db)
	s := &Server{
		db:            db,
		port:          port,
		bind:          bind,
		allowedNet:    allowedNet,
		router:        chi.NewRouter(),
		authService:   auth.NewAuthService(db),
		apiKeyService: auth.NewAPIKeyService(db),
		sseBroker:     sse.NewBroker(),
		processor:     processor.New(db, processor.DefaultConfig()),
		targetsMgr:    targetsMgr,
	}

	s.loadTemplates()
	s.setupRoutes()

	// Refresh library cache on startup in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := targetsMgr.RefreshAllLibraryCache(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to refresh library cache on startup")
		}
	}()

	return s
}

// Processor returns the scan processor
func (s *Server) Processor() *processor.Processor {
	return s.processor
}

// SSEBroker returns the SSE broker for broadcasting events
func (s *Server) SSEBroker() *sse.Broker {
	return s.sseBroker
}

// SetRcloneManager sets the rclone manager and updates handlers
func (s *Server) SetRcloneManager(mgr *rclone.Manager) {
	s.rcloneMgr = mgr
	if s.handlers != nil {
		s.handlers.SetRcloneManager(mgr)
	}

	// Set up callback to apply saved settings when rclone becomes ready
	if mgr != nil {
		mgr.SetOnReady(func() {
			s.applyRcloneSettings()
		})
	}
}

// SetUploadManager sets the upload manager and updates handlers
func (s *Server) SetUploadManager(mgr *uploader.Manager) {
	s.uploadMgr = mgr
	if s.handlers != nil {
		s.handlers.SetUploadManager(mgr)
	}
}

// SetVersionInfo sets the application version information
func (s *Server) SetVersionInfo(version, commit, date string) {
	if s.handlers != nil {
		s.handlers.SetVersionInfo(version, commit, date)
	}
}

// StartUpdateChecker starts the background update checker
func (s *Server) StartUpdateChecker() {
	if s.handlers != nil {
		s.handlers.StartUpdateChecker()
	}
}

// RcloneManager returns the rclone manager
func (s *Server) RcloneManager() *rclone.Manager {
	return s.rcloneMgr
}

// applyRcloneSettings applies saved rclone settings from the database to the running rclone RCD
func (s *Server) applyRcloneSettings() {
	if s.rcloneMgr == nil || !s.rcloneMgr.IsRunning() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mainOpts := make(map[string]any)

	// Load transfers setting
	if v, err := s.db.GetSetting("rclone.transfers"); err == nil && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Transfers"] = i
		}
	}

	// Load checkers setting
	if v, err := s.db.GetSetting("rclone.checkers"); err == nil && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Checkers"] = i
		}
	}

	// Load buffer size setting
	if v, err := s.db.GetSetting("rclone.buffer_size"); err == nil && v != "" {
		mainOpts["BufferSize"] = handlers.ParseSizeString(v)
	}

	if len(mainOpts) > 0 {
		options := map[string]any{
			"main": mainOpts,
		}

		if err := s.rcloneMgr.Client().SetOptions(ctx, options); err != nil {
			log.Error().Err(err).Msg("Failed to apply saved rclone settings")
		} else {
			log.Info().Interface("options", mainOpts).Msg("Applied saved rclone settings")
		}
	}
}

// UploadManager returns the upload manager
func (s *Server) UploadManager() *uploader.Manager {
	return s.uploadMgr
}

// TargetsManager returns the targets manager
func (s *Server) TargetsManager() *targets.Manager {
	return s.targetsMgr
}

// SetThrottleManager sets the throttle manager and updates handlers
func (s *Server) SetThrottleManager(mgr *throttle.Manager) {
	s.throttleMgr = mgr
	if s.handlers != nil {
		s.handlers.SetThrottleManager(mgr)
	}
}

// ThrottleManager returns the throttle manager
func (s *Server) ThrottleManager() *throttle.Manager {
	return s.throttleMgr
}

// StartUploadSubsystem starts the upload subsystem (upload manager, throttle manager, rclone)
// This is called when uploads are enabled at runtime
func (s *Server) StartUploadSubsystem() error {
	// Check if already running
	if s.uploadMgr != nil && s.uploadMgr.IsRunning() {
		return nil
	}

	log.Info().Msg("Starting upload subsystem")

	// Load rclone config
	rcloneConfig := handlers.LoadRcloneConfigFromDB(s.db)
	if binaryPath, _ := s.db.GetSetting("rclone.binary_path"); binaryPath == "" || binaryPath == "/usr/bin/rclone" {
		rcloneConfig.BinaryPath = rclone.FindRcloneBinary()
	}

	// Create rclone manager if not exists
	if s.rcloneMgr == nil {
		s.rcloneMgr = rclone.NewManager(rcloneConfig)
		if s.handlers != nil {
			s.handlers.SetRcloneManager(s.rcloneMgr)
		}
		s.rcloneMgr.SetOnReady(func() {
			s.applyRcloneSettings()
		})
	}

	// Start rclone if auto-start is enabled
	if rcloneConfig.AutoStart {
		if err := s.rcloneMgr.Start(); err != nil {
			log.Warn().Err(err).Msg("Failed to auto-start rclone RCD")
		}
	}

	// Create upload manager if not exists
	if s.uploadMgr == nil {
		uploadConfig := uploader.DefaultConfig()
		s.uploadMgr = uploader.New(s.db, s.rcloneMgr, uploadConfig)
		if s.handlers != nil {
			s.handlers.SetUploadManager(s.uploadMgr)
		}
		s.processor.SetUploadQueuer(s.uploadMgr)
		s.uploadMgr.SetSSEBroker(s.sseBroker)
	}
	s.uploadMgr.Start()

	// Create throttle manager if not exists
	if s.throttleMgr == nil {
		throttleConfig := throttle.LoadConfigFromDB(s.db)
		s.throttleMgr = throttle.New(s.db, s.targetsMgr, s.rcloneMgr, throttleConfig)
		if s.handlers != nil {
			s.handlers.SetThrottleManager(s.throttleMgr)
		}
		s.uploadMgr.SetSkipChecker(s.throttleMgr)
		s.throttleMgr.SetSSEBroker(s.sseBroker)
	}
	s.throttleMgr.Start()

	// Wire upload manager to inotify and polling watchers
	if s.inotifyMgr != nil {
		s.inotifyMgr.SetUploadManager(s.uploadMgr)
	}
	if s.pollingMgr != nil {
		s.pollingMgr.SetUploadManager(s.uploadMgr)
	}

	log.Info().Msg("Upload subsystem started")
	return nil
}

// StopUploadSubsystem stops the upload subsystem
// This is called when uploads are disabled at runtime
func (s *Server) StopUploadSubsystem() error {
	log.Info().Msg("Stopping upload subsystem")

	// Stop in reverse order: throttle -> upload -> rclone

	if s.throttleMgr != nil {
		s.throttleMgr.Stop()
	}

	if s.uploadMgr != nil {
		s.uploadMgr.Stop()
	}

	if s.rcloneMgr != nil && s.rcloneMgr.IsRunning() {
		if err := s.rcloneMgr.Stop(); err != nil {
			log.Warn().Err(err).Msg("Failed to stop rclone RCD")
		}
	}

	// Disconnect upload manager from watchers
	if s.inotifyMgr != nil {
		s.inotifyMgr.SetUploadManager(nil)
	}
	if s.pollingMgr != nil {
		s.pollingMgr.SetUploadManager(nil)
	}

	// Clear upload queuer from processor
	s.processor.SetUploadQueuer(nil)

	log.Info().Msg("Upload subsystem stopped")
	return nil
}

// SetNotificationManager sets the notification manager and updates handlers
func (s *Server) SetNotificationManager(mgr *notification.Manager) {
	s.notificationMgr = mgr
	if s.handlers != nil {
		s.handlers.SetNotificationManager(mgr)
		s.handlers.InitNotificationProviders()
	}
}

// NotificationManager returns the notification manager
func (s *Server) NotificationManager() *notification.Manager {
	return s.notificationMgr
}

// SetInotifyManager sets the inotify manager and updates handlers
func (s *Server) SetInotifyManager(mgr *inotify.Watcher) {
	s.inotifyMgr = mgr
	if s.handlers != nil {
		s.handlers.SetInotifyManager(mgr)
	}
}

// SetPollingManager sets the polling manager and updates handlers
func (s *Server) SetPollingManager(mgr *polling.Poller) {
	s.pollingMgr = mgr
	if s.handlers != nil {
		s.handlers.SetPollingManager(mgr)
	}
}

// SetMatcharrManager sets the matcharr manager and updates handlers
func (s *Server) SetMatcharrManager(mgr *matcharr.Manager) {
	s.matcharrMgr = mgr
	if s.handlers != nil {
		s.handlers.SetMatcharrManager(mgr)
	}
}

// MatcharrManager returns the matcharr manager
func (s *Server) MatcharrManager() *matcharr.Manager {
	return s.matcharrMgr
}

// SetPlexAutoLangManager sets the Plex Auto Languages manager and updates handlers
func (s *Server) SetPlexAutoLangManager(mgr *plexautolang.Manager) {
	s.plexAutoLangMgr = mgr
	if s.handlers != nil {
		s.handlers.SetPlexAutoLangManager(mgr)
	}
}

// PlexAutoLangManager returns the Plex Auto Languages manager
func (s *Server) PlexAutoLangManager() *plexautolang.Manager {
	return s.plexAutoLangMgr
}

// templateFuncMap returns the common template functions
func templateFuncMap() template.FuncMap {
	return template.FuncMap{
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"formatTimePtr": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.Format("2006-01-02 15:04:05")
		},
		"formatBytes": formatBytes,
		"formatSpeed": formatSpeed,
		"formatDuration": func(d time.Duration) string {
			return d.Round(time.Second).String()
		},
		"runDuration": func(start time.Time, end *time.Time) string {
			if end == nil {
				return "-"
			}
			d := end.Sub(start)
			if d < time.Second {
				return d.Round(time.Millisecond).String()
			}
			return d.Round(time.Second).String()
		},
		"formatETA": func(seconds *int64) string {
			if seconds == nil || *seconds <= 0 {
				return "-"
			}
			s := *seconds
			h := s / 3600
			m := (s % 3600) / 60
			sec := s % 60
			if h > 0 {
				return fmt.Sprintf("%dh %dm %ds", h, m, sec)
			}
			if m > 0 {
				return fmt.Sprintf("%dm %ds", m, sec)
			}
			return fmt.Sprintf("%ds", sec)
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"subtract": func(a, b int) int {
			return a - b
		},
		"mul": func(a, b int) int {
			return a * b
		},
		"min": func(a, b int) int {
			if a < b {
				return a
			}
			return b
		},
		"json": func(v any) string {
			b, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return err.Error()
			}
			return string(b)
		},
		"truncate": func(s string, maxLen int) string {
			if len(s) <= maxLen {
				return s
			}
			return s[:maxLen] + "..."
		},
		"jsEscape": func(s string) string {
			// Escape for use in JavaScript strings
			s = strings.ReplaceAll(s, "\\", "\\\\")
			s = strings.ReplaceAll(s, "'", "\\'")
			s = strings.ReplaceAll(s, "\"", "\\\"")
			s = strings.ReplaceAll(s, "\n", "\\n")
			s = strings.ReplaceAll(s, "\r", "\\r")
			return s
		},
		"typeChoices": func(optType string) string {
			choices := rclone.GetTypeChoices(optType)
			if choices == nil {
				return ""
			}
			b, _ := json.Marshal(choices)
			return string(b)
		},
	}
}

// loadTemplates loads all HTML templates
// Each page template is parsed with the base template and partials
func (s *Server) loadTemplates() {
	s.templates = make(map[string]*template.Template)
	funcMap := templateFuncMap()

	// List of page templates to load
	pageTemplates := []string{
		"login.html",
		"dashboard.html",
		"triggers.html",
		"targets.html",
		"uploads.html",
		"settings.html",
		"history_scans.html",
		"history_uploads.html",
		"logs.html",
		"wizard/setup.html",
		"rclone_options.html",
		"matcharr.html",
		"matcharr_run.html",
		"plexautolang.html",
	}

	for _, page := range pageTemplates {
		// Parse base template first, then partials, then the page template
		tmpl, err := template.New("").Funcs(funcMap).ParseFS(templatesFS,
			"templates/base.html",
			"templates/partials/*.html",
			"templates/"+page,
		)
		if err != nil {
			log.Fatal().Err(err).Str("template", page).Msg("Failed to parse template")
		}
		s.templates[page] = tmpl
	}
}

// setupRoutes configures all routes
func (s *Server) setupRoutes() {
	r := s.router

	// Global middleware (applied to all routes, except timeout which is per-group)
	r.Use(chimiddleware.RequestID)
	// AllowSubnet must come BEFORE RealIP so we check the actual connection source
	r.Use(middleware.AllowSubnet(s.allowedNet))
	r.Use(chimiddleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(chimiddleware.Recoverer)
	// Note: Timeout middleware is applied per-group, not globally, to allow SSE long-lived connections

	// SSE endpoint - no timeout (long-lived connections)
	r.Group(func(r chi.Router) {
		r.Use(middleware.SessionAuth(s.authService))
		r.Get("/api/events", s.sseBroker.ServeHTTP)
	})

	// Static files with caching
	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to setup static files")
	}
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.FS(staticContent)))
	r.Handle("/static/*", middleware.StaticCache(staticHandler))

	// Create handlers
	h := handlers.New(s.db, s.templates, s.authService, s.apiKeyService, s.processor)
	s.handlers = h

	// Set the server as the upload subsystem toggler
	h.SetUploadSubsystemToggler(s)

	// Set managers if already available
	if s.rcloneMgr != nil {
		h.SetRcloneManager(s.rcloneMgr)
	}
	if s.uploadMgr != nil {
		h.SetUploadManager(s.uploadMgr)
	}

	// Public routes (no auth required)
	r.Group(func(r chi.Router) {
		r.Use(chimiddleware.Timeout(60 * time.Second))
		r.Get("/login", h.LoginPage)
		r.Post("/login", h.LoginSubmit)
		r.Get("/logout", h.Logout)

		// Setup wizard (only works if no users exist)
		r.Get("/setup", h.SetupWizard)
		r.Post("/setup", h.SetupSubmit)
	})

	// API routes (API key auth)
	r.Route("/api", func(r chi.Router) {
		r.Use(chimiddleware.Timeout(60 * time.Second))
		// Trigger webhooks - authenticated by API key
		r.Route("/triggers", func(r chi.Router) {
			r.Post("/sonarr/{id}", h.APITriggerSonarr)
			r.Post("/radarr/{id}", h.APITriggerRadarr)
			r.Post("/lidarr/{id}", h.APITriggerLidarr)
			r.Post("/webhook/{id}", h.APITriggerWebhook)
			r.Post("/autoplow/{id}", h.APITriggerAutoplow)
			r.Post("/a_train/{id}", h.APITriggerATrain)
			// Autoscan compatibility - supports both GET and POST
			r.Get("/autoscan/{id}", h.APITriggerAutoscan)
			r.Post("/autoscan/{id}", h.APITriggerAutoscan)
			// Bazarr - supports both GET and POST
			r.Get("/bazarr/{id}", h.APITriggerBazarr)
			r.Post("/bazarr/{id}", h.APITriggerBazarr)
		})

	})

	// Protected routes (session auth required)
	r.Group(func(r chi.Router) {
		r.Use(chimiddleware.Timeout(60 * time.Second))
		r.Use(middleware.SessionAuth(s.authService))
		r.Use(middleware.RequireSetup(s.db))

		// Profile management
		r.Post("/profile/username", h.ProfileUpdateUsername)
		r.Post("/profile/password", h.ProfileUpdatePassword)

		// Dashboard
		r.Get("/", h.Dashboard)
		r.Get("/dashboard", h.Dashboard)
		r.Get("/dashboard/stats", h.DashboardStatsPartial)
		r.Get("/dashboard/upload-stats", h.DashboardUploadStatsPartial)
		r.Get("/dashboard/scans", h.DashboardScansPartial)
		r.Get("/dashboard/uploads", h.DashboardUploadsPartial)
		r.Get("/dashboard/sessions", h.DashboardSessionsPartial)
		r.Get("/dashboard/throttle", h.DashboardThrottleStatusPartial)

		// Triggers management
		r.Route("/triggers", func(r chi.Router) {
			r.Get("/", h.TriggersPage)
			r.Get("/new", h.TriggerNew)
			r.Post("/", h.TriggerCreate)
			r.Post("/scan", h.ManualScan)
			r.Get("/{id}", h.TriggerEdit)
			r.Post("/{id}", h.TriggerUpdate)
			r.Delete("/{id}", h.TriggerDelete)
			r.Post("/{id}/regenerate-key", h.TriggerRegenerateKey)
			r.Post("/{id}/update-password", h.TriggerUpdatePassword)
		})

		// Media servers (targets)
		r.Route("/targets", func(r chi.Router) {
			r.Get("/", h.TargetsPage)
			r.Get("/new", h.TargetNew)
			r.Post("/", h.TargetCreate)
			r.Post("/test", h.TargetTestNew)
			r.Get("/{id}", h.TargetEdit)
			r.Post("/{id}", h.TargetUpdate)
			r.Delete("/{id}", h.TargetDelete)
			r.Post("/{id}/test", h.TargetTest)
			r.Get("/{id}/libraries", h.TargetLibraries)
		})

		// Upload configuration
		r.Route("/uploads", func(r chi.Router) {
			r.Get("/", h.UploadsPage)
			r.Get("/queue", h.UploadsQueuePartial)
			r.Get("/queue-stats", h.UploadQueueStatsPartial)
			r.Get("/queue-pagination", h.UploadQueuePaginationPartial)
			r.Get("/active", h.UploadsActivePartial)
			r.Post("/{id}/cancel", h.UploadCancel)
			r.Post("/{id}/retry", h.UploadRetry)
			r.Post("/clear-completed", h.UploadClearCompleted)
			r.Post("/pause", h.UploadPause)
			r.Post("/resume", h.UploadResume)
			r.Get("/pause-btn", h.UploadPauseBtn)
			r.Get("/status-banner", h.UploadStatusBanner)
			r.Get("/progress", h.UploadProgress)
			r.Get("/stats", h.UploadStats)
			r.Get("/history", h.UploadHistoryPage)

			// Remotes management
			r.Route("/remotes", func(r chi.Router) {
				r.Get("/", h.RemotesPage)
				r.Get("/new", h.RemoteNew)
				r.Post("/", h.RemoteCreate)
				r.Get("/available", h.RemoteListAvailable)
				r.Get("/backend/{backend}/options", h.RemoteBackendOptionsPartial)
				r.Get("/backend/{backend}/option/{option}", h.RemoteAddOptionPartial)
				r.Get("/global/{category}", h.RemoteGlobalOptionsPartial)
				r.Get("/{id}", h.RemoteEdit)
				r.Post("/{id}", h.RemoteUpdate)
				r.Delete("/{id}", h.RemoteDelete)
				r.Post("/{id}/test", h.RemoteTest)
			})

			// Destinations management
			r.Route("/destinations", func(r chi.Router) {
				r.Get("/", h.DestinationsPage)
				r.Get("/new", h.DestinationNew)
				r.Post("/", h.DestinationCreate)
				r.Get("/{id}", h.DestinationEdit)
				r.Post("/{id}", h.DestinationUpdate)
				r.Delete("/{id}", h.DestinationDelete)
				r.Post("/{id}/remotes", h.DestinationAddRemote)
				r.Put("/{id}/remotes/reorder", h.DestinationReorderRemotes)
				r.Delete("/{id}/remotes/{remote_id}", h.DestinationRemoveRemote)
			})
		})

		// Settings
		r.Route("/settings", func(r chi.Router) {
			r.Get("/", h.SettingsPage)
			r.Post("/", h.SettingsUpdate)
			r.Post("/clear-upload-history", h.ClearUploadHistory)
			r.Get("/about", h.SettingsAboutPage)
			r.Get("/processor", h.SettingsProcessorPage)
			r.Post("/processor", h.SettingsProcessorUpdate)

			// Notification settings
			r.Get("/notifications", h.SettingsNotificationsPage)
			r.Get("/notifications/new", h.NotificationProviderNew)
			r.Post("/notifications", h.NotificationProviderCreate)
			r.Get("/notifications/{id}", h.NotificationProviderEdit)
			r.Post("/notifications/{id}", h.NotificationProviderUpdate)
			r.Delete("/notifications/{id}", h.NotificationProviderDelete)
			r.Post("/notifications/{id}/test", h.NotificationProviderTest)
			r.Post("/notifications/logs/clear", h.NotificationLogsClear)

			// Throttle settings
			r.Get("/throttle", h.SettingsThrottlePage)
			r.Post("/throttle", h.SettingsThrottleUpdate)
			r.Get("/throttle/status", h.SettingsThrottleStatus)

			// Rclone settings
			r.Get("/rclone", h.SettingsRclonePage)
			r.Post("/rclone", h.SettingsRcloneUpdate)
			r.Post("/rclone/options", h.SettingsRcloneOptionsUpdate)
			r.Post("/rclone/test", h.SettingsRcloneTest)
			r.Post("/rclone/start", h.SettingsRcloneStart)
			r.Post("/rclone/stop", h.SettingsRcloneStop)
			r.Post("/rclone/restart", h.SettingsRcloneRestart)
			r.Get("/rclone/status", h.SettingsRcloneStatus)
			r.Get("/rclone/startstop-btn", h.SettingsRcloneStartStopBtn)
		})

		// History
		r.Route("/history", func(r chi.Router) {
			r.Get("/scans", h.HistoryScans)
			r.Post("/scans/{id}/retry", h.RetryScan)
			r.Get("/uploads", h.HistoryUploads)
		})

		// Logs
		r.Get("/logs", h.LogsPage)

		// Rclone Options Explorer
		r.Route("/rclone", func(r chi.Router) {
			r.Get("/", h.RcloneOptionsPage)
			r.Post("/", h.RcloneOptionsUpdate)
			r.Get("/api/options", h.RcloneOptionsAPI)
			r.Get("/api/providers", h.RcloneProvidersAPI)
			r.Get("/providers/{provider}", h.RcloneProviderOptionsPartial)
			r.Get("/options/{category}", h.RcloneGlobalOptionsPartial)
		})

		// Matcharr - Media Server ID Matching
		r.Route("/matcharr", func(r chi.Router) {
			r.Get("/", h.MatcharrPage)
			r.Get("/arrs/new", h.MatcharrArrNew)
			r.Post("/arrs", h.MatcharrArrCreate)
			r.Post("/arrs/test", h.MatcharrArrTestRaw)
			r.Get("/arrs/{id}", h.MatcharrArrEdit)
			r.Post("/arrs/{id}", h.MatcharrArrUpdate)
			r.Delete("/arrs/{id}", h.MatcharrArrDelete)
			r.Post("/arrs/{id}/test", h.MatcharrArrTest)
			r.Post("/run", h.MatcharrRunNow)
			r.Get("/run/status", h.MatcharrRunStatus)
			r.Get("/mismatches", h.MatcharrMismatchesPartial)
			r.Post("/mismatches/{id}/fix", h.MatcharrFixOne)
			r.Post("/mismatches/{id}/skip", h.MatcharrSkipMismatch)
			r.Post("/mismatches/fix-all", h.MatcharrFixAll)
			r.Post("/settings", h.MatcharrSettingsUpdate)
			r.Get("/runs", h.MatcharrRunsPartial)
			r.Get("/runs/{id}", h.MatcharrRunDetails)
			r.Get("/status", h.MatcharrStatusPartial)
			r.Get("/last-run", h.MatcharrLastRunPartial)
			r.Get("/quick-actions", h.MatcharrQuickActionsPartial)
			r.Post("/targets/{id}/toggle", h.MatcharrToggleTarget)
			r.Post("/history/clear", h.MatcharrClearHistory)
		})

		// Plex Auto Languages - Automatic audio/subtitle track selection
		r.Route("/plex-auto-languages", func(r chi.Router) {
			r.Get("/", h.PlexAutoLangPage)
			r.Get("/status", h.PlexAutoLangStatus)
			r.Get("/status/partial", h.PlexAutoLangStatusPartial)
			r.Get("/recent-activity/partial", h.PlexAutoLangRecentActivityPartial)
			r.Post("/targets/{id}/toggle", h.PlexAutoLangToggleTarget)
			r.Get("/targets/{id}/config", h.PlexAutoLangConfigGet)
			r.Post("/targets/{id}/config", h.PlexAutoLangConfigUpdate)
			r.Get("/targets/{id}/config/partial", h.PlexAutoLangTargetConfigPartial)
			r.Get("/targets/{id}/preferences", h.PlexAutoLangPreferences)
			r.Get("/targets/{id}/preferences/partial", h.PlexAutoLangPreferencesPartial)
			r.Delete("/preferences/{id}", h.PlexAutoLangDeletePreference)
			r.Get("/history", h.PlexAutoLangHistory)
			r.Get("/history/partial", h.PlexAutoLangHistoryPartial)
			r.Post("/history/clear", h.PlexAutoLangClearHistory)
		})
	})
}

// Start starts the web server
func (s *Server) Start(ctx context.Context) error {
	var addr string
	if s.bind != "" {
		addr = fmt.Sprintf("%s:%d", s.bind, s.port)
	} else {
		addr = fmt.Sprintf(":%d", s.port)
	}

	server := &http.Server{
		Addr:    addr,
		Handler: s.router,
		// ReadTimeout is for reading request body
		ReadTimeout: 15 * time.Second,
		// WriteTimeout disabled (0) to allow SSE long-lived connections
		// Chi middleware timeout (60s) protects regular requests
		WriteTimeout: 0,
		// IdleTimeout for keep-alive connections between requests
		IdleTimeout: 120 * time.Second,
	}

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Info().Str("addr", addr).Msg("Starting HTTP server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-ctx.Done():
		log.Info().Msg("Shutting down HTTP server")
		// Stop SSE broker first to close all client connections gracefully
		s.sseBroker.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// formatBytes formats bytes as human readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatSpeed formats speed (bytes/sec as float64) as human readable string
func formatSpeed(bytesPerSec float64) string {
	const unit = 1024.0
	if bytesPerSec < unit {
		return fmt.Sprintf("%.0f B", bytesPerSec)
	}
	div, exp := unit, 0
	for n := bytesPerSec / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", bytesPerSec/div, "KMGTPE"[exp])
}

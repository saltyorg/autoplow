package handlers

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

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
	"github.com/saltyorg/autoplow/internal/throttle"
	"github.com/saltyorg/autoplow/internal/uploader"
	"github.com/saltyorg/autoplow/internal/web/middleware"
)

// UploadSubsystemToggler is an interface for starting/stopping the upload subsystem
type UploadSubsystemToggler interface {
	StartUploadSubsystem() error
	StopUploadSubsystem() error
}

// VersionInfo holds application version information
type VersionInfo struct {
	Version         string
	Commit          string
	StaticVersion   string // Cache-busting version for static files (empty in dev mode)
	Date            string // Formatted date for display (fallback)
	RawDate         string // RFC3339 date for JS locale formatting
	LatestVersion   string // Latest version from GitHub
	UpdateAvailable bool   // True if a newer version is available
}

// Handlers contains all HTTP handlers
type Handlers struct {
	db                     *database.DB
	templates              map[string]*template.Template
	authService            *auth.AuthService
	apiKeyService          *auth.APIKeyService
	triggerAuthService     *auth.TriggerAuthService
	processor              *processor.Processor
	rcloneMgr              *rclone.Manager
	uploadMgr              *uploader.Manager
	throttleMgr            *throttle.Manager
	notificationMgr        *notification.Manager
	inotifyMgr             *inotify.Watcher
	pollingMgr             *polling.Poller
	matcharrMgr            *matcharr.Manager
	plexAutoLangMgr        *plexautolang.Manager
	uploadSubsystemToggler UploadSubsystemToggler
	versionInfo            VersionInfo
	versionMu              sync.RWMutex
	isDev                  bool
}

// New creates a new Handlers instance
func New(db *database.DB, templates map[string]*template.Template, authService *auth.AuthService, apiKeyService *auth.APIKeyService, proc *processor.Processor, isDev bool) *Handlers {
	return &Handlers{
		db:                 db,
		templates:          templates,
		authService:        authService,
		apiKeyService:      apiKeyService,
		triggerAuthService: auth.NewTriggerAuthService(db),
		processor:          proc,
		isDev:              isDev,
	}
}

// SetRcloneManager sets the rclone manager
func (h *Handlers) SetRcloneManager(mgr *rclone.Manager) {
	h.rcloneMgr = mgr
}

// SetUploadManager sets the upload manager
func (h *Handlers) SetUploadManager(mgr *uploader.Manager) {
	h.uploadMgr = mgr
}

// SetThrottleManager sets the throttle manager
func (h *Handlers) SetThrottleManager(mgr *throttle.Manager) {
	h.throttleMgr = mgr
}

// SetNotificationManager sets the notification manager
func (h *Handlers) SetNotificationManager(mgr *notification.Manager) {
	h.notificationMgr = mgr
}

// SetInotifyManager sets the inotify manager
func (h *Handlers) SetInotifyManager(mgr *inotify.Watcher) {
	h.inotifyMgr = mgr
}

// SetPollingManager sets the polling manager
func (h *Handlers) SetPollingManager(mgr *polling.Poller) {
	h.pollingMgr = mgr
}

// SetUploadSubsystemToggler sets the upload subsystem toggler
func (h *Handlers) SetUploadSubsystemToggler(toggler UploadSubsystemToggler) {
	h.uploadSubsystemToggler = toggler
}

// SetMatcharrManager sets the matcharr manager
func (h *Handlers) SetMatcharrManager(mgr *matcharr.Manager) {
	h.matcharrMgr = mgr
}

// SetPlexAutoLangManager sets the Plex Auto Languages manager
func (h *Handlers) SetPlexAutoLangManager(mgr *plexautolang.Manager) {
	h.plexAutoLangMgr = mgr
}

// SetVersionInfo sets the application version information
func (h *Handlers) SetVersionInfo(version, commit, date string) {
	// Parse and format the date to be human-readable
	formattedDate := date
	if t, err := time.Parse(time.RFC3339, date); err == nil {
		formattedDate = t.Format("January 2, 2006 at 3:04 PM MST")
	}

	// Only set StaticVersion for non-dev builds (enables browser caching)
	staticVersion := ""
	if version != "0.0.0-dev" && commit != "none" && commit != "" {
		staticVersion = commit
	}

	h.versionMu.Lock()
	h.versionInfo = VersionInfo{
		Version:       version,
		Commit:        commit,
		StaticVersion: staticVersion,
		Date:          formattedDate,
		RawDate:       date,
	}
	h.versionMu.Unlock()
}

// getVersionInfo returns a copy of the version info (thread-safe)
func (h *Handlers) getVersionInfo() VersionInfo {
	h.versionMu.RLock()
	defer h.versionMu.RUnlock()
	return h.versionInfo
}

// StartUpdateChecker starts a background goroutine that periodically checks for updates
func (h *Handlers) StartUpdateChecker() {
	// Skip update checking for dev versions
	h.versionMu.RLock()
	version := h.versionInfo.Version
	h.versionMu.RUnlock()

	if version == "0.0.0-dev" || version == "dev" {
		log.Debug().Msg("Skipping update checker for dev version")
		return
	}

	// Check immediately on startup
	go h.checkForUpdates()

	// Then check every 6 hours
	go func() {
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			h.checkForUpdates()
		}
	}()
}

// githubRelease represents the response from GitHub releases API
type githubRelease struct {
	TagName string `json:"tag_name"`
}

// checkForUpdates fetches the latest release from GitHub and updates version info
func (h *Handlers) checkForUpdates() {
	const apiURL = "https://svm.saltbox.dev/version?url=https://api.github.com/repos/saltyorg/autoplow/releases/latest"

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to check for updates")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Debug().Int("status", resp.StatusCode).Msg("Update check returned non-OK status")
		return
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		log.Debug().Err(err).Msg("Failed to parse update check response")
		return
	}

	latestVersion := strings.TrimPrefix(release.TagName, "v")
	if latestVersion == "" {
		return
	}

	h.versionMu.Lock()
	defer h.versionMu.Unlock()

	h.versionInfo.LatestVersion = latestVersion
	h.versionInfo.UpdateAvailable = compareVersions(h.versionInfo.Version, latestVersion) < 0

	if h.versionInfo.UpdateAvailable {
		log.Info().
			Str("current", h.versionInfo.Version).
			Str("latest", latestVersion).
			Msg("Update available")
	}
}

// compareVersions compares two semantic versions
// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func compareVersions(v1, v2 string) int {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	maxLen := max(len(parts2), len(parts1))

	for i := range maxLen {
		var n1, n2 int
		if i < len(parts1) {
			n1, _ = strconv.Atoi(parts1[i])
		}
		if i < len(parts2) {
			n2, _ = strconv.Atoi(parts2[i])
		}

		if n1 < n2 {
			return -1
		}
		if n1 > n2 {
			return 1
		}
	}
	return 0
}

// PageData contains common data for all pages
type PageData struct {
	Title           string
	User            *auth.User
	Flash           string
	FlashErr        string
	Content         any
	ScanningEnabled bool // Global setting to show/hide scanning functionality
	UploadsEnabled  bool // Global setting to show/hide upload functionality
	Version         VersionInfo
}

// render renders a template with common data
func (h *Handlers) render(w http.ResponseWriter, r *http.Request, name string, data any) {
	user := middleware.GetUser(r.Context())

	// Check if scanning is enabled (default to true)
	scanningEnabled := true
	if val, _ := h.db.GetSetting("scanning.enabled"); val == "false" {
		scanningEnabled = false
	}

	// Check if uploads are enabled (default to true)
	uploadsEnabled := true
	if val, _ := h.db.GetSetting("uploads.enabled"); val == "false" {
		uploadsEnabled = false
	}

	pageData := PageData{
		Title:           "Autoplow",
		User:            user,
		Content:         data,
		ScanningEnabled: scanningEnabled,
		UploadsEnabled:  uploadsEnabled,
		Version:         h.getVersionInfo(),
	}

	// Check for flash messages in cookies
	if cookie, err := r.Cookie("flash"); err == nil {
		pageData.Flash = cookie.Value
		clear := &http.Cookie{Name: "flash", MaxAge: -1, Path: "/"}
		h.applyCookieSecurity(clear)
		http.SetCookie(w, clear)
	}
	if cookie, err := r.Cookie("flash_err"); err == nil {
		pageData.FlashErr = cookie.Value
		clear := &http.Cookie{Name: "flash_err", MaxAge: -1, Path: "/"}
		h.applyCookieSecurity(clear)
		http.SetCookie(w, clear)
	}

	tmpl, ok := h.templates[name]
	if !ok {
		log.Error().Str("template", name).Msg("Template not found")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "base", pageData); err != nil {
		log.Error().Err(err).Str("template", name).Msg("Failed to render template")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// renderPartial renders a partial template (for HTMX)
// The pageTemplate should be any loaded template, partialName is the defined partial within it
func (h *Handlers) renderPartial(w http.ResponseWriter, pageTemplate string, partialName string, data any) {
	tmpl, ok := h.templates[pageTemplate]
	if !ok {
		log.Error().Str("template", pageTemplate).Msg("Template not found for partial")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, partialName, data); err != nil {
		log.Error().Err(err).Str("partial", partialName).Msg("Failed to render partial")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// flash sets a flash message
func (h *Handlers) flash(w http.ResponseWriter, message string) {
	c := &http.Cookie{
		Name:     "flash",
		Value:    message,
		Path:     "/",
		MaxAge:   60,
		HttpOnly: true,
	}
	h.applyCookieSecurity(c)
	http.SetCookie(w, c)
}

// flashErr sets an error flash message
func (h *Handlers) flashErr(w http.ResponseWriter, message string) {
	c := &http.Cookie{
		Name:     "flash_err",
		Value:    message,
		Path:     "/",
		MaxAge:   60,
		HttpOnly: true,
	}
	h.applyCookieSecurity(c)
	http.SetCookie(w, c)
}

// redirect redirects to a URL
func (h *Handlers) redirect(w http.ResponseWriter, r *http.Request, url string) {
	http.Redirect(w, r, url, http.StatusSeeOther)
}

// jsonError sends a JSON error response
func (h *Handlers) jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"` + message + `"}`))
}

// applyCookieSecurity sets Secure/SameSite defaults based on environment.
func (h *Handlers) applyCookieSecurity(c *http.Cookie) {
	if h.isDev {
		if c.SameSite == 0 {
			c.SameSite = http.SameSiteLaxMode
		}
		return
	}
	c.Secure = true
	if c.SameSite == 0 {
		c.SameSite = http.SameSiteStrictMode
	}
}

// jsonSuccess sends a JSON success response
func (h *Handlers) jsonSuccess(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"success":true,"message":"` + message + `"}`))
}

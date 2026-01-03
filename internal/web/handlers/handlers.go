package handlers

import (
	"html/template"
	"net/http"
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
	Version string
	Commit  string
	Date    string
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
}

// New creates a new Handlers instance
func New(db *database.DB, templates map[string]*template.Template, authService *auth.AuthService, apiKeyService *auth.APIKeyService, proc *processor.Processor) *Handlers {
	return &Handlers{
		db:                 db,
		templates:          templates,
		authService:        authService,
		apiKeyService:      apiKeyService,
		triggerAuthService: auth.NewTriggerAuthService(db),
		processor:          proc,
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

	h.versionInfo = VersionInfo{
		Version: version,
		Commit:  commit,
		Date:    formattedDate,
	}
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
		Version:         h.versionInfo,
	}

	// Check for flash messages in cookies
	if cookie, err := r.Cookie("flash"); err == nil {
		pageData.Flash = cookie.Value
		http.SetCookie(w, &http.Cookie{Name: "flash", MaxAge: -1, Path: "/"})
	}
	if cookie, err := r.Cookie("flash_err"); err == nil {
		pageData.FlashErr = cookie.Value
		http.SetCookie(w, &http.Cookie{Name: "flash_err", MaxAge: -1, Path: "/"})
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
	http.SetCookie(w, &http.Cookie{
		Name:     "flash",
		Value:    message,
		Path:     "/",
		MaxAge:   60,
		HttpOnly: true,
	})
}

// flashErr sets an error flash message
func (h *Handlers) flashErr(w http.ResponseWriter, message string) {
	http.SetCookie(w, &http.Cookie{
		Name:     "flash_err",
		Value:    message,
		Path:     "/",
		MaxAge:   60,
		HttpOnly: true,
	})
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

// jsonSuccess sends a JSON success response
func (h *Handlers) jsonSuccess(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"success":true,"message":"` + message + `"}`))
}

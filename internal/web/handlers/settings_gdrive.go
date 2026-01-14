package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/saltyorg/autoplow/internal/database"
	"github.com/saltyorg/autoplow/internal/gdrive"
)

const gdriveStateTTL = 10 * time.Minute
const gdriveFolderMimeType = "application/vnd.google-apps.folder"

type gdriveOAuthState struct {
	expiresAt time.Time
}

type gdriveSettings struct {
	ClientID      string
	ClientSecret  string
	OAuthAccounts []*database.GDriveAccount
	RedirectURL   string
}

// SettingsGDrivePage renders the Google Drive settings page.
func (h *Handlers) SettingsGDrivePage(w http.ResponseWriter, r *http.Request) {
	clientID, _ := h.db.GetSetting("gdrive.client_id")
	clientSecret, _ := h.db.GetSetting("gdrive.client_secret")

	accounts, err := h.db.ListGDriveAccounts()
	if err != nil {
		log.Error().Err(err).Msg("Failed to load gdrive accounts")
		h.flashErr(w, "Failed to load Google Drive accounts")
		h.redirect(w, r, "/settings")
		return
	}

	oauthAccounts := make([]*database.GDriveAccount, 0, len(accounts))
	for _, account := range accounts {
		if account.AuthType != "" && account.AuthType != database.GDriveAuthTypeOAuth {
			continue
		}
		oauthAccounts = append(oauthAccounts, account)
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab": "gdrive",
		"Settings": gdriveSettings{
			ClientID:      clientID,
			ClientSecret:  clientSecret,
			OAuthAccounts: oauthAccounts,
			RedirectURL:   getBaseURL(r) + "/settings/gdrive/callback",
		},
	})
}

// SettingsGDriveUpdate updates the Google Drive OAuth client settings.
func (h *Handlers) SettingsGDriveUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	clientID := r.FormValue("gdrive_client_id")
	clientSecret := r.FormValue("gdrive_client_secret")

	if err := h.db.SetSetting("gdrive.client_id", clientID); err != nil {
		h.flashErr(w, "Failed to save Google Drive client ID")
		h.redirect(w, r, "/settings/gdrive")
		return
	}
	if err := h.db.SetSetting("gdrive.client_secret", clientSecret); err != nil {
		h.flashErr(w, "Failed to save Google Drive client secret")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	h.flash(w, "Google Drive settings saved")
	h.redirect(w, r, "/settings/gdrive")
}

// SettingsGDriveConnect starts the OAuth flow for a new account.
func (h *Handlers) SettingsGDriveConnect(w http.ResponseWriter, r *http.Request) {
	svc := gdrive.NewService(h.db)
	redirectURL := getBaseURL(r) + "/settings/gdrive/callback"
	cfg, err := svc.OAuthConfig(redirectURL)
	if err != nil {
		h.flashErr(w, "Google Drive client ID/secret not configured")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	state := h.createGDriveState()
	authURL := cfg.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.SetAuthURLParam("prompt", "consent"))
	http.Redirect(w, r, authURL, http.StatusSeeOther)
}

// SettingsGDriveCallback handles the OAuth callback from Google.
func (h *Handlers) SettingsGDriveCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	if state == "" || !h.consumeGDriveState(state) {
		h.flashErr(w, "Invalid or expired OAuth state")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		h.flashErr(w, "Missing OAuth code")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	svc := gdrive.NewService(h.db)
	redirectURL := getBaseURL(r) + "/settings/gdrive/callback"
	cfg, err := svc.OAuthConfig(redirectURL)
	if err != nil {
		h.flashErr(w, "Google Drive client ID/secret not configured")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		log.Error().Err(err).Msg("Failed to exchange gdrive oauth code")
		h.flashErr(w, "Failed to connect Google Drive account")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	driveSvc, err := drive.NewService(ctx, option.WithTokenSource(cfg.TokenSource(ctx, token)))
	if err != nil {
		log.Error().Err(err).Msg("Failed to create gdrive client")
		h.flashErr(w, "Failed to connect Google Drive account")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	about, err := driveSvc.About.Get().Fields("user(emailAddress,displayName,permissionId)").Context(ctx).Do()
	if err != nil {
		log.Error().Err(err).Msg("Failed to fetch gdrive account info")
		h.flashErr(w, "Failed to fetch Google Drive account info")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	subject := about.User.PermissionId
	if subject == "" {
		subject = about.User.EmailAddress
	}
	if subject == "" {
		h.flashErr(w, "Failed to identify Google Drive account")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	encryptedToken := ""
	if token.RefreshToken != "" {
		encryptedToken, err = svc.EncryptRefreshToken(token.RefreshToken)
		if err != nil {
			log.Error().Err(err).Msg("Failed to encrypt gdrive refresh token")
			h.flashErr(w, "Failed to store Google Drive credentials")
			h.redirect(w, r, "/settings/gdrive")
			return
		}
	} else {
		existing, err := h.db.GetGDriveAccountBySubject(subject)
		if err != nil {
			log.Error().Err(err).Msg("Failed to load existing gdrive account")
		}
		if existing == nil || existing.RefreshToken == "" {
			h.flashErr(w, "Google did not return a refresh token. Revoke access and try again.")
			h.redirect(w, r, "/settings/gdrive")
			return
		}
		encryptedToken = existing.RefreshToken
	}

	account := &database.GDriveAccount{
		Subject:            subject,
		Email:              about.User.EmailAddress,
		DisplayName:        about.User.DisplayName,
		RefreshToken:       encryptedToken,
		ServiceAccountJSON: "",
		AuthType:           database.GDriveAuthTypeOAuth,
	}

	if err := h.db.UpsertGDriveAccount(account); err != nil {
		log.Error().Err(err).Msg("Failed to store gdrive account")
		h.flashErr(w, "Failed to store Google Drive account")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	h.flash(w, "Google Drive account connected")
	h.redirect(w, r, "/settings/gdrive")
}

// SettingsGDriveDisconnect removes a connected account.
func (h *Handlers) SettingsGDriveDisconnect(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.flashErr(w, "Invalid account ID")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	if err := h.db.DeleteGDriveAccount(id); err != nil {
		log.Error().Err(err).Msg("Failed to delete gdrive account")
		h.flashErr(w, "Failed to delete Google Drive account")
		h.redirect(w, r, "/settings/gdrive")
		return
	}

	h.flash(w, "Google Drive account disconnected")
	h.redirect(w, r, "/settings/gdrive")
}

// SettingsGDriveAccountDrives lists shared drives for an account.
func (h *Handlers) SettingsGDriveAccountDrives(w http.ResponseWriter, r *http.Request) {
	account, err := h.gdriveAccountFromRequest(r)
	if err != nil {
		h.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	svc, err := gdrive.NewService(h.db).DriveServiceForAccount(ctx, account)
	if err != nil {
		h.jsonError(w, "Failed to connect to Google Drive", http.StatusInternalServerError)
		return
	}

	var drives []*drive.Drive
	pageToken := ""
	for {
		req := svc.Drives.List().Fields("nextPageToken,drives(id,name)").PageSize(100)
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		resp, err := req.Context(ctx).Do()
		if err != nil {
			h.jsonError(w, "Failed to list drives", http.StatusInternalServerError)
			return
		}
		drives = append(drives, resp.Drives...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"drives": drives,
	})
}

// SettingsGDriveAccountFolders lists folders under a parent folder.
func (h *Handlers) SettingsGDriveAccountFolders(w http.ResponseWriter, r *http.Request) {
	account, err := h.gdriveAccountFromRequest(r)
	if err != nil {
		h.jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	svc, err := gdrive.NewService(h.db).DriveServiceForAccount(ctx, account)
	if err != nil {
		h.jsonError(w, "Failed to connect to Google Drive", http.StatusInternalServerError)
		return
	}

	driveID := r.URL.Query().Get("drive_id")
	parentID := r.URL.Query().Get("parent_id")
	if parentID == "" {
		parentID = "root"
	}

	driveName := "My Drive"
	if driveID != "" {
		resp, err := svc.Drives.Get(driveID).Fields("id,name").Context(ctx).Do()
		if err != nil {
			h.jsonError(w, "Failed to resolve drive name", http.StatusInternalServerError)
			return
		}
		driveName = strings.TrimSpace(resp.Name)
		if driveName == "" {
			driveName = "Shared Drive"
		}
	}

	if driveID != "" && parentID == "root" {
		parentID = driveID
	}

	rootPrefix := gdrive.RootPrefix(driveID, driveName)
	parentPath := rootPrefix
	if parentID != "root" && parentID != driveID {
		resolver := gdrive.NewPathResolver(ctx, svc, driveID, driveName, "")
		resolved, _, err := resolver.ResolvePath(parentID)
		if err != nil {
			h.jsonError(w, "Failed to resolve parent folder path", http.StatusInternalServerError)
			return
		}
		parentPath = resolved
	}
	parentDisplayPath := gdrive.SimplifiedPath(parentPath)

	query := fmt.Sprintf("mimeType='%s' and trashed=false and '%s' in parents", gdriveFolderMimeType, parentID)

	var folders []*drive.File
	pageToken := ""
	for {
		req := svc.Files.List().
			Q(query).
			OrderBy("name").
			Fields("nextPageToken,files(id,name)").
			PageSize(200).
			SupportsAllDrives(true).
			IncludeItemsFromAllDrives(true)
		if driveID != "" {
			req = req.DriveId(driveID).Corpora("drive")
		}
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		resp, err := req.Context(ctx).Do()
		if err != nil {
			h.jsonError(w, "Failed to list folders", http.StatusInternalServerError)
			return
		}
		folders = append(folders, resp.Files...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	type folderResponse struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Path        string `json:"path"`
		DisplayPath string `json:"display_path"`
	}

	respFolders := make([]folderResponse, 0, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		name := strings.TrimSpace(folder.Name)
		childPath := path.Join(parentPath, name)
		respFolders = append(respFolders, folderResponse{
			ID:          folder.Id,
			Name:        name,
			Path:        childPath,
			DisplayPath: gdrive.SimplifiedPath(childPath),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"folders":             respFolders,
		"parent_path":         parentPath,
		"parent_display_path": parentDisplayPath,
	})
}

func (h *Handlers) gdriveAccountFromRequest(r *http.Request) (*database.GDriveAccount, error) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid account id")
	}
	account, err := h.db.GetGDriveAccount(id)
	if err != nil {
		return nil, fmt.Errorf("failed to load account")
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}
	if account.AuthType != "" && account.AuthType != database.GDriveAuthTypeOAuth {
		return nil, fmt.Errorf("service accounts are not supported")
	}
	return account, nil
}

func (h *Handlers) createGDriveState() string {
	state := randomState()
	h.gdriveStateMu.Lock()
	defer h.gdriveStateMu.Unlock()
	h.cleanupGDriveStatesLocked()
	h.gdriveStates[state] = gdriveOAuthState{expiresAt: time.Now().Add(gdriveStateTTL)}
	return state
}

func (h *Handlers) consumeGDriveState(state string) bool {
	h.gdriveStateMu.Lock()
	defer h.gdriveStateMu.Unlock()
	h.cleanupGDriveStatesLocked()
	if entry, ok := h.gdriveStates[state]; ok && time.Now().Before(entry.expiresAt) {
		delete(h.gdriveStates, state)
		return true
	}
	delete(h.gdriveStates, state)
	return false
}

func (h *Handlers) cleanupGDriveStatesLocked() {
	now := time.Now()
	for key, entry := range h.gdriveStates {
		if now.After(entry.expiresAt) {
			delete(h.gdriveStates, key)
		}
	}
}

func randomState() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

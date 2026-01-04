package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/web/middleware"
)

// LoginPage renders the login page
func (h *Handlers) LoginPage(w http.ResponseWriter, r *http.Request) {
	// Check if already logged in
	if cookie, err := r.Cookie("session"); err == nil {
		if session, err := h.authService.GetSession(cookie.Value); err == nil && session != nil {
			h.redirect(w, r, "/")
			return
		}
	}

	// Check if setup is needed
	firstRun, err := h.db.IsFirstRun()
	if err != nil {
		log.Error().Err(err).Msg("Failed to check first run")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if firstRun {
		h.redirect(w, r, "/setup")
		return
	}

	h.render(w, r, "login.html", nil)
}

// LoginSubmit handles login form submission
func (h *Handlers) LoginSubmit(w http.ResponseWriter, r *http.Request) {
	username := r.FormValue("username")
	password := r.FormValue("password")

	if username == "" || password == "" {
		h.flashErr(w, "Username and password are required")
		h.redirect(w, r, "/login")
		return
	}

	user, err := h.authService.Authenticate(username, password)
	if err != nil {
		log.Error().Err(err).Msg("Authentication error")
		h.flashErr(w, "An error occurred during login")
		h.redirect(w, r, "/login")
		return
	}
	if user == nil {
		h.flashErr(w, "Invalid username or password")
		h.redirect(w, r, "/login")
		return
	}

	// Create session
	session, err := h.authService.CreateSession(user.ID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create session")
		h.flashErr(w, "An error occurred during login")
		h.redirect(w, r, "/login")
		return
	}

	// Set session cookie
	cookie := &http.Cookie{
		Name:     "session",
		Value:    session.ID,
		Path:     "/",
		Expires:  session.ExpiresAt,
		HttpOnly: true,
	}
	h.applyCookieSecurity(cookie)
	http.SetCookie(w, cookie)

	log.Info().Str("username", username).Msg("User logged in")
	h.redirect(w, r, "/")
}

// Logout handles user logout
func (h *Handlers) Logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("session"); err == nil {
		if err := h.authService.DeleteSession(cookie.Value); err != nil {
			log.Debug().Err(err).Msg("Failed to delete session during logout")
		}
	}

	cookie := &http.Cookie{
		Name:     "session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
	}
	h.applyCookieSecurity(cookie)
	http.SetCookie(w, cookie)

	h.redirect(w, r, "/login")
}

// SetupWizard renders the setup wizard
func (h *Handlers) SetupWizard(w http.ResponseWriter, r *http.Request) {
	// Check if setup is already complete
	firstRun, err := h.db.IsFirstRun()
	if err != nil {
		log.Error().Err(err).Msg("Failed to check first run")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if !firstRun {
		h.redirect(w, r, "/")
		return
	}

	h.render(w, r, "wizard/setup.html", nil)
}

// SetupSubmit handles the setup wizard submission
func (h *Handlers) SetupSubmit(w http.ResponseWriter, r *http.Request) {
	// Verify still in first run
	firstRun, err := h.db.IsFirstRun()
	if err != nil {
		log.Error().Err(err).Msg("Failed to check first run")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if !firstRun {
		h.redirect(w, r, "/")
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")
	confirmPassword := r.FormValue("confirm_password")
	featureScanning := r.FormValue("feature_scanning") == "on"
	featureUploads := r.FormValue("feature_uploads") == "on"

	// Validate
	if username == "" || password == "" {
		h.flashErr(w, "Username and password are required")
		h.redirect(w, r, "/setup")
		return
	}
	if len(username) < 3 {
		h.flashErr(w, "Username must be at least 3 characters")
		h.redirect(w, r, "/setup")
		return
	}
	if len(password) < 8 {
		h.flashErr(w, "Password must be at least 8 characters")
		h.redirect(w, r, "/setup")
		return
	}
	if password != confirmPassword {
		h.flashErr(w, "Passwords do not match")
		h.redirect(w, r, "/setup")
		return
	}
	if !featureScanning && !featureUploads {
		h.flashErr(w, "Please select at least one feature to enable")
		h.redirect(w, r, "/setup")
		return
	}

	// Create user
	user, err := h.authService.CreateUser(username, password)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create user")
		h.flashErr(w, "Failed to create user account")
		h.redirect(w, r, "/setup")
		return
	}

	// Initialize default settings
	if err := h.db.InitializeDefaults(); err != nil {
		log.Error().Err(err).Msg("Failed to initialize default settings")
	}

	// Apply feature selections
	if err := h.db.SetSettingJSON("scanning.enabled", featureScanning); err != nil {
		log.Error().Err(err).Msg("Failed to set scanning.enabled")
	}
	if err := h.db.SetSettingJSON("uploads.enabled", featureUploads); err != nil {
		log.Error().Err(err).Msg("Failed to set uploads.enabled")
	}

	// Create session and log in
	session, err := h.authService.CreateSession(user.ID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create session")
		h.redirect(w, r, "/login")
		return
	}

	cookie := &http.Cookie{
		Name:     "session",
		Value:    session.ID,
		Path:     "/",
		Expires:  time.Now().Add(auth.SessionDuration),
		HttpOnly: true,
	}
	h.applyCookieSecurity(cookie)
	http.SetCookie(w, cookie)

	log.Info().Str("username", username).Msg("Setup completed, user created")
	h.flash(w, "Welcome to Autoplow! Setup complete.")
	h.redirect(w, r, "/")
}

// ProfileUpdateUsername handles username update via AJAX
func (h *Handlers) ProfileUpdateUsername(w http.ResponseWriter, r *http.Request) {
	user := middleware.GetUser(r.Context())
	if user == nil {
		h.jsonError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	newUsername := r.FormValue("username")
	if newUsername == "" {
		h.jsonError(w, "Username is required", http.StatusBadRequest)
		return
	}
	if len(newUsername) < 3 {
		h.jsonError(w, "Username must be at least 3 characters", http.StatusBadRequest)
		return
	}

	// Check if username is already taken (by another user)
	existing, err := h.authService.GetUserByUsername(newUsername)
	if err != nil {
		log.Error().Err(err).Msg("Failed to check username")
		h.jsonError(w, "An error occurred", http.StatusInternalServerError)
		return
	}
	if existing != nil && existing.ID != user.ID {
		h.jsonError(w, "Username is already taken", http.StatusBadRequest)
		return
	}

	if err := h.authService.UpdateUsername(user.ID, newUsername); err != nil {
		log.Error().Err(err).Msg("Failed to update username")
		h.jsonError(w, "Failed to update username", http.StatusInternalServerError)
		return
	}

	log.Info().Str("old_username", user.Username).Str("new_username", newUsername).Msg("Username updated")

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":  true,
		"message":  "Username updated successfully",
		"username": newUsername,
	})
}

// ProfileUpdatePassword handles password update via AJAX
func (h *Handlers) ProfileUpdatePassword(w http.ResponseWriter, r *http.Request) {
	user := middleware.GetUser(r.Context())
	if user == nil {
		h.jsonError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	currentPassword := r.FormValue("current_password")
	newPassword := r.FormValue("new_password")
	confirmPassword := r.FormValue("confirm_password")

	if currentPassword == "" || newPassword == "" || confirmPassword == "" {
		h.jsonError(w, "All password fields are required", http.StatusBadRequest)
		return
	}

	// Verify current password
	if !auth.CheckPassword(currentPassword, user.PasswordHash) {
		h.jsonError(w, "Current password is incorrect", http.StatusBadRequest)
		return
	}

	if len(newPassword) < 8 {
		h.jsonError(w, "New password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	if newPassword != confirmPassword {
		h.jsonError(w, "New passwords do not match", http.StatusBadRequest)
		return
	}

	if err := h.authService.UpdatePassword(user.ID, newPassword); err != nil {
		log.Error().Err(err).Msg("Failed to update password")
		h.jsonError(w, "Failed to update password", http.StatusInternalServerError)
		return
	}

	log.Info().Str("username", user.Username).Msg("Password updated")
	h.jsonSuccess(w, "Password updated successfully")
}

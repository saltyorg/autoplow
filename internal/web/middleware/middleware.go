package middleware

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/database"
)

type contextKey string

const (
	// UserContextKey is the context key for the authenticated user
	UserContextKey contextKey = "user"
	// SessionContextKey is the context key for the session
	SessionContextKey contextKey = "session"
)

// Logger is a middleware that logs requests
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		defer func() {
			log.Debug().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Int("status", ww.Status()).
				Dur("duration", time.Since(start)).
				Str("remote", r.RemoteAddr).
				Str("request_id", middleware.GetReqID(r.Context())).
				Msg("Request")
		}()

		next.ServeHTTP(ww, r)
	})
}

// SessionAuth is a middleware that checks for valid session cookie
func SessionAuth(authService *auth.AuthService) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie("session")
			if err != nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}

			session, err := authService.GetSession(cookie.Value)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get session")
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			if session == nil {
				// Clear invalid cookie
				http.SetCookie(w, &http.Cookie{
					Name:     "session",
					Value:    "",
					Path:     "/",
					MaxAge:   -1,
					HttpOnly: true,
				})
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}

			// Get user
			user, err := authService.GetUserByID(session.UserID)
			if err != nil || user == nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}

			// Extend session on activity
			authService.ExtendSession(session.ID)

			// Add to context
			ctx := context.WithValue(r.Context(), UserContextKey, user)
			ctx = context.WithValue(ctx, SessionContextKey, session)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireSetup ensures the setup wizard has been completed
func RequireSetup(db *database.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			firstRun, err := db.IsFirstRun()
			if err != nil {
				log.Error().Err(err).Msg("Failed to check first run")
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			if firstRun {
				http.Redirect(w, r, "/setup", http.StatusSeeOther)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// GetUser retrieves the user from context
func GetUser(ctx context.Context) *auth.User {
	user, ok := ctx.Value(UserContextKey).(*auth.User)
	if !ok {
		return nil
	}
	return user
}

// AllowSubnet is a middleware that restricts access to connections from within the allowed subnet.
// This checks the actual connection source (RemoteAddr), useful for whitelisting reverse proxies.
func AllowSubnet(allowedNet *net.IPNet) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If no subnet restriction, allow all
			if allowedNet == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Get the direct connection IP from RemoteAddr
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				// Maybe it's just an IP without port
				host = r.RemoteAddr
			}

			ip := net.ParseIP(host)
			if ip == nil {
				log.Warn().Str("remote_addr", r.RemoteAddr).Msg("Could not parse remote address")
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			// Check if connection source is within allowed subnet
			if !allowedNet.Contains(ip) {
				log.Warn().
					Str("remote_addr", r.RemoteAddr).
					Str("allowed_subnet", allowedNet.String()).
					Msg("Connection rejected: source IP not in allowed subnet")
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

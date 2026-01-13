package gdrive

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"

	"github.com/saltyorg/autoplow/internal/auth"
	"github.com/saltyorg/autoplow/internal/database"
)

// Service builds Google Drive API clients using stored credentials.
type Service struct {
	db *database.Manager
}

// NewService creates a new Google Drive service helper.
func NewService(db *database.Manager) *Service {
	return &Service{db: db}
}

func (s *Service) oauthConfig() (*oauth2.Config, error) {
	clientID, _ := s.db.GetSetting("gdrive.client_id")
	clientSecret, _ := s.db.GetSetting("gdrive.client_secret")
	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("gdrive oauth client id/secret not configured")
	}

	return &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{drive.DriveReadonlyScope},
	}, nil
}

// OAuthConfig returns an OAuth config with the provided redirect URL.
func (s *Service) OAuthConfig(redirectURL string) (*oauth2.Config, error) {
	cfg, err := s.oauthConfig()
	if err != nil {
		return nil, err
	}
	cfg.RedirectURL = redirectURL
	return cfg, nil
}

// DriveService creates a Drive API client from a refresh token.
func (s *Service) DriveService(ctx context.Context, refreshToken string) (*drive.Service, error) {
	cfg, err := s.oauthConfig()
	if err != nil {
		return nil, err
	}

	token := &oauth2.Token{RefreshToken: refreshToken}
	ts := cfg.TokenSource(ctx, token)
	return drive.NewService(ctx, option.WithTokenSource(ts))
}

// DriveServiceForAccount creates a Drive API client for an account (OAuth or service account).
func (s *Service) DriveServiceForAccount(ctx context.Context, account *database.GDriveAccount) (*drive.Service, error) {
	if account == nil {
		return nil, fmt.Errorf("missing gdrive account")
	}

	switch account.AuthType {
	case database.GDriveAuthTypeServiceAccount:
		if account.ServiceAccountJSON == "" {
			return nil, fmt.Errorf("missing service account credentials")
		}
		raw, err := s.DecryptServiceAccountJSON(account.ServiceAccountJSON)
		if err != nil {
			return nil, err
		}
		return s.DriveServiceFromServiceAccountJSON(ctx, raw)
	default:
		if account.RefreshToken == "" {
			return nil, fmt.Errorf("missing refresh token")
		}
		refreshToken, err := s.DecryptRefreshToken(account.RefreshToken)
		if err != nil {
			return nil, err
		}
		return s.DriveService(ctx, refreshToken)
	}
}

// EncryptRefreshToken encrypts a refresh token for storage.
func (s *Service) EncryptRefreshToken(refreshToken string) (string, error) {
	return auth.EncryptTriggerPassword(refreshToken)
}

// DecryptRefreshToken decrypts a stored refresh token.
func (s *Service) DecryptRefreshToken(encrypted string) (string, error) {
	return auth.DecryptTriggerPassword(encrypted)
}

// EncryptServiceAccountJSON encrypts a service account JSON blob for storage.
func (s *Service) EncryptServiceAccountJSON(serviceAccountJSON string) (string, error) {
	return auth.EncryptTriggerPassword(serviceAccountJSON)
}

// DecryptServiceAccountJSON decrypts a stored service account JSON blob.
func (s *Service) DecryptServiceAccountJSON(encrypted string) (string, error) {
	return auth.DecryptTriggerPassword(encrypted)
}

// DriveServiceFromServiceAccountJSON creates a Drive API client from raw service account JSON.
func (s *Service) DriveServiceFromServiceAccountJSON(ctx context.Context, serviceAccountJSON string) (*drive.Service, error) {
	cfg, err := google.JWTConfigFromJSON([]byte(serviceAccountJSON), drive.DriveReadonlyScope)
	if err != nil {
		return nil, err
	}
	ts := cfg.TokenSource(ctx)
	return drive.NewService(ctx, option.WithTokenSource(ts))
}

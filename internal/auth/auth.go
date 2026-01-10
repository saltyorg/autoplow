package auth

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/saltyorg/autoplow/internal/database"
)

// triggerPasswordKey is loaded at startup from a per-install key file.
// legacyTriggerPasswordKey preserves compatibility with previously encrypted values.
var triggerPasswordKey []byte
var legacyTriggerPasswordKey = []byte("autoplow-trigger-password-key32!")

const (
	// SessionDuration is how long sessions last
	SessionDuration = 7 * 24 * time.Hour // 7 days
	// BcryptCost is the bcrypt cost factor
	BcryptCost = 12
)

// User represents a user account
type User struct {
	ID           int64
	Username     string
	PasswordHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Session represents a user session
type Session struct {
	ID        string
	UserID    int64
	ExpiresAt time.Time
	CreatedAt time.Time
}

// AuthService handles authentication
type AuthService struct {
	db *database.Manager
}

// NewAuthService creates a new auth service
func NewAuthService(db *database.Manager) *AuthService {
	return &AuthService{db: db}
}

// HashPassword hashes a password using bcrypt
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
	if err != nil {
		return "", fmt.Errorf("failed to hash password: %w", err)
	}
	return string(hash), nil
}

// CheckPassword verifies a password against a hash
func CheckPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func currentTriggerPasswordKey() []byte {
	if len(triggerPasswordKey) > 0 {
		return triggerPasswordKey
	}
	return legacyTriggerPasswordKey
}

// EncryptTriggerPassword encrypts a trigger password using AES-GCM.
// This is reversible encryption for webhook auth passwords (not user login passwords).
func EncryptTriggerPassword(password string) (string, error) {
	key := currentTriggerPasswordKey()

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(password), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptTriggerPassword decrypts a trigger password encrypted with EncryptTriggerPassword.
func DecryptTriggerPassword(encrypted string) (string, error) {
	key := currentTriggerPasswordKey()

	plaintext, err := decryptTriggerPasswordWithKey(key, encrypted)
	if err == nil {
		return plaintext, nil
	}

	// Fallback to legacy key for already-stored passwords if a new key is configured
	if !bytes.Equal(key, legacyTriggerPasswordKey) {
		if legacyPlaintext, legacyErr := decryptTriggerPasswordWithKey(legacyTriggerPasswordKey, encrypted); legacyErr == nil {
			return legacyPlaintext, nil
		}
	}

	return "", err
}

func decryptTriggerPasswordWithKey(key []byte, encrypted string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("failed to decode: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

func encryptTriggerPasswordWithKey(key []byte, password string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(password), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// CreateUser creates a new user account
func (s *AuthService) CreateUser(username, password string) (*User, error) {
	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}
	record, err := s.db.CreateUser(username, hash)
	if err != nil {
		return nil, err
	}

	return &User{
		ID:           record.ID,
		Username:     record.Username,
		PasswordHash: record.PasswordHash,
		CreatedAt:    record.CreatedAt,
		UpdatedAt:    record.UpdatedAt,
	}, nil
}

// GetUserByUsername retrieves a user by username
func (s *AuthService) GetUserByUsername(username string) (*User, error) {
	record, err := s.db.GetUserByUsername(username)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return &User{
		ID:           record.ID,
		Username:     record.Username,
		PasswordHash: record.PasswordHash,
		CreatedAt:    record.CreatedAt,
		UpdatedAt:    record.UpdatedAt,
	}, nil
}

// GetUserByID retrieves a user by ID
func (s *AuthService) GetUserByID(id int64) (*User, error) {
	record, err := s.db.GetUserByID(id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return &User{
		ID:           record.ID,
		Username:     record.Username,
		PasswordHash: record.PasswordHash,
		CreatedAt:    record.CreatedAt,
		UpdatedAt:    record.UpdatedAt,
	}, nil
}

// Authenticate verifies credentials and returns the user
func (s *AuthService) Authenticate(username, password string) (*User, error) {
	user, err := s.GetUserByUsername(username)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, nil
	}
	if !CheckPassword(password, user.PasswordHash) {
		return nil, nil
	}
	return user, nil
}

// UpdatePassword changes a user's password
func (s *AuthService) UpdatePassword(userID int64, newPassword string) error {
	hash, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	return s.db.UpdateUserPassword(userID, hash)
}

// UpdateUsername changes a user's username
func (s *AuthService) UpdateUsername(userID int64, newUsername string) error {
	return s.db.UpdateUsername(userID, newUsername)
}

// CreateSession creates a new session for a user
func (s *AuthService) CreateSession(userID int64) (*Session, error) {
	sessionID, err := generateSessionID()
	if err != nil {
		return nil, err
	}

	expiresAt := time.Now().Add(SessionDuration)
	record, err := s.db.CreateSession(sessionID, userID, expiresAt)
	if err != nil {
		return nil, err
	}

	return &Session{
		ID:        record.ID,
		UserID:    record.UserID,
		ExpiresAt: record.ExpiresAt,
		CreatedAt: record.CreatedAt,
	}, nil
}

// GetSession retrieves a session by ID
func (s *AuthService) GetSession(sessionID string) (*Session, error) {
	record, err := s.db.GetSession(sessionID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}

	session := &Session{
		ID:        record.ID,
		UserID:    record.UserID,
		ExpiresAt: record.ExpiresAt,
		CreatedAt: record.CreatedAt,
	}

	// Check if expired
	if time.Now().After(session.ExpiresAt) {
		if err := s.DeleteSession(sessionID); err != nil {
			// Log but don't fail - session is expired and won't be used anyway
			// Caller will handle the nil return appropriately
			return nil, fmt.Errorf("failed to delete expired session: %w", err)
		}
		return nil, nil
	}

	return session, nil
}

// DeleteSession removes a session
func (s *AuthService) DeleteSession(sessionID string) error {
	return s.db.DeleteSession(sessionID)
}

// ExtendSession extends a session's expiration
func (s *AuthService) ExtendSession(sessionID string) error {
	expiresAt := time.Now().Add(SessionDuration)
	return s.db.ExtendSession(sessionID, expiresAt)
}

// generateSessionID creates a cryptographically secure session ID
func generateSessionID() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate session id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

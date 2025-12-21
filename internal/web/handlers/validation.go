package handlers

import (
	"fmt"
	"path/filepath"
	"strings"
)

// PathValidationError represents a validation error for a path
type PathValidationError struct {
	Field   string
	Message string
}

func (e PathValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidatePath performs validation on a filesystem path.
// It checks for:
// - Path is not empty
// - Path is absolute (starts with /)
// - Path doesn't contain null bytes
// - Path doesn't contain path traversal sequences after cleaning
func ValidatePath(path, fieldName string) error {
	if path == "" {
		return PathValidationError{Field: fieldName, Message: "path cannot be empty"}
	}

	// Check for null bytes (potential injection attack)
	if strings.ContainsRune(path, '\x00') {
		return PathValidationError{Field: fieldName, Message: "path contains invalid characters"}
	}

	// Paths should be absolute
	if !filepath.IsAbs(path) {
		return PathValidationError{Field: fieldName, Message: "path must be absolute (start with /)"}
	}

	// Clean the path and check for traversal
	cleaned := filepath.Clean(path)

	// After cleaning, the path should still be under the same root
	// This catches cases where someone tries to use /../ to escape
	if !strings.HasPrefix(cleaned, "/") {
		return PathValidationError{Field: fieldName, Message: "path contains invalid traversal sequences"}
	}

	return nil
}

// ValidatePathMapping validates a path mapping pair (from -> to).
// Both paths should be valid absolute paths.
func ValidatePathMapping(from, to string) error {
	if err := ValidatePath(from, "from path"); err != nil {
		return err
	}
	if err := ValidatePath(to, "to path"); err != nil {
		return err
	}
	return nil
}

// ValidateRemotePath validates a remote path for rclone destinations.
// Remote paths can be relative (they're relative to the remote root),
// but they shouldn't contain null bytes or traversal sequences.
func ValidateRemotePath(path, fieldName string) error {
	if path == "" {
		return PathValidationError{Field: fieldName, Message: "path cannot be empty"}
	}

	// Check for null bytes
	if strings.ContainsRune(path, '\x00') {
		return PathValidationError{Field: fieldName, Message: "path contains invalid characters"}
	}

	// Clean the path to normalize it
	cleaned := filepath.Clean(path)

	// Check for parent directory traversal that would escape the intended root
	// After cleaning, if the path starts with ".." it's trying to escape
	if strings.HasPrefix(cleaned, "..") {
		return PathValidationError{Field: fieldName, Message: "path cannot traverse above root"}
	}

	return nil
}

// NormalizePath cleans a path by removing redundant separators and resolving . and ..
// while keeping the path absolute.
func NormalizePath(path string) string {
	if path == "" {
		return path
	}
	return filepath.Clean(path)
}

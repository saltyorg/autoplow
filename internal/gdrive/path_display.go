package gdrive

import (
	"path"
	"strings"
)

// RootPrefix returns the drive-relative root path for a drive.
func RootPrefix(driveID, driveName string) string {
	return "/"
}

// SimplifiedPath returns a UI-friendly drive-relative path without the leading slash.
func SimplifiedPath(input string) string {
	cleaned := path.Clean(strings.TrimSpace(input))
	if cleaned == "." || cleaned == "" {
		return ""
	}
	if cleaned == "/" {
		return "/"
	}
	return strings.TrimPrefix(cleaned, "/")
}

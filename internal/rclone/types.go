package rclone

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// ParseOptionValue parses a string value according to the option's Type.
// It returns the parsed value suitable for sending to rclone's API.
func ParseOptionValue(optionType, value string) (any, error) {
	if value == "" {
		return value, nil
	}

	switch optionType {
	case "bool":
		return strconv.ParseBool(value)

	case "int":
		return strconv.Atoi(value)

	case "int64":
		return strconv.ParseInt(value, 10, 64)

	case "uint32":
		v, err := strconv.ParseUint(value, 10, 32)
		return uint32(v), err

	case "float64":
		return strconv.ParseFloat(value, 64)

	case "string":
		return value, nil

	case "Duration":
		var d fs.Duration
		if err := d.Set(value); err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", value, err)
		}
		return time.Duration(d), nil

	case "SizeSuffix":
		var s fs.SizeSuffix
		if err := s.Set(value); err != nil {
			return nil, fmt.Errorf("invalid size %q: %w", value, err)
		}
		return int64(s), nil

	case "Bits":
		// Bits is a generic type in rclone, accept comma-separated values as string
		return value, nil

	case "Tristate":
		var t fs.Tristate
		if err := t.Set(value); err != nil {
			return nil, fmt.Errorf("invalid tristate %q: %w", value, err)
		}
		// Return string for API
		return t.String(), nil

	case "Encoding":
		var e encoder.MultiEncoder
		if err := e.Set(value); err != nil {
			return nil, fmt.Errorf("invalid encoding %q: %w", value, err)
		}
		return uint(e), nil

	case "CacheMode":
		var c vfscommon.CacheMode
		if err := c.Set(value); err != nil {
			return nil, fmt.Errorf("invalid cache mode %q: %w", value, err)
		}
		return c.String(), nil

	case "LogLevel":
		var l fs.LogLevel
		if err := l.Set(value); err != nil {
			return nil, fmt.Errorf("invalid log level %q: %w", value, err)
		}
		return l.String(), nil

	case "stringArray", "CommaSepList":
		// Split by comma
		if value == "" {
			return []string{}, nil
		}
		return strings.Split(value, ","), nil

	case "SpaceSepList":
		// Split by space
		if value == "" {
			return []string{}, nil
		}
		return strings.Fields(value), nil

	default:
		// Check for pipe-separated enum types (e.g., "AUTO|NEVER|ALWAYS")
		if strings.Contains(optionType, "|") {
			choices := strings.SplitSeq(optionType, "|")
			for choice := range choices {
				if strings.EqualFold(value, choice) {
					return choice, nil
				}
			}
			return nil, fmt.Errorf("invalid value %q: must be one of %s", value, optionType)
		}

		// Unknown type - return as string
		return value, nil
	}
}

// ValidateOptionValue validates a string value against the option's Type.
// Returns nil if valid, or an error describing the validation failure.
func ValidateOptionValue(optionType, value string) error {
	_, err := ParseOptionValue(optionType, value)
	return err
}

// GetTypeChoices returns valid choices for a given option type.
// Returns nil if the type doesn't have a fixed set of choices.
func GetTypeChoices(optionType string) []string {
	switch optionType {
	case "Encoding":
		return GetEncodingFlags()
	case "CacheMode":
		return GetCacheModeChoices()
	case "LogLevel":
		return GetLogLevelChoices()
	case "Tristate":
		return GetTristateChoices()
	case "bool":
		return []string{"true", "false"}
	default:
		// Check for pipe-separated enum types (e.g., "AUTO|NEVER|ALWAYS")
		if strings.Contains(optionType, "|") {
			return strings.Split(optionType, "|")
		}
		return nil
	}
}

// GetEncodingFlags returns all valid encoding flag names.
// These can be combined with commas (e.g., "Slash,Colon,InvalidUtf8").
func GetEncodingFlags() []string {
	// Parse from encoder.ValidStrings() which returns comma-separated list
	validStr := encoder.ValidStrings()
	flags := strings.Split(validStr, ", ")
	return flags
}

// GetCacheModeChoices returns valid CacheMode values.
func GetCacheModeChoices() []string {
	var c vfscommon.CacheMode
	return c.Choices()
}

// GetLogLevelChoices returns valid LogLevel values.
func GetLogLevelChoices() []string {
	var l fs.LogLevel
	return l.Choices()
}

// GetTristateChoices returns valid Tristate values.
func GetTristateChoices() []string {
	return []string{"true", "false", "unset"}
}

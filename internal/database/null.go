package database

import (
	"database/sql"
	"encoding/json"
	"time"
)

// nullInt64ToPtr converts a sql.NullInt64 to a pointer (nil if not valid)
func nullInt64ToPtr(n sql.NullInt64) *int64 {
	if n.Valid {
		return &n.Int64
	}
	return nil
}

// nullTimeToPtr converts a sql.NullTime to a pointer (nil if not valid)
func nullTimeToPtr(n sql.NullTime) *time.Time {
	if n.Valid {
		return &n.Time
	}
	return nil
}

// nullStringValue converts a sql.NullString to a string (empty if not valid)
func nullStringValue(n sql.NullString) string {
	if n.Valid {
		return n.String
	}
	return ""
}

// marshalToPtr marshals a value to JSON and returns a pointer to the string
// Returns nil if the value is nil or empty
func marshalToPtr(v any) (*string, error) {
	if v == nil {
		return nil, nil
	}

	// Check for empty maps
	if m, ok := v.(map[string]any); ok && len(m) == 0 {
		return nil, nil
	}
	if m, ok := v.(map[string]any); ok && len(m) == 0 {
		return nil, nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	s := string(data)
	return &s, nil
}

// unmarshalFromNullString unmarshal JSON from a sql.NullString into a value
// If the string is not valid or empty, does nothing and returns nil
func unmarshalFromNullString(data sql.NullString, v any) error {
	if !data.Valid || data.String == "" {
		return nil
	}
	return json.Unmarshal([]byte(data.String), v)
}

// marshalToString marshals a value to a JSON string
// Useful when the column is NOT NULL and requires a string value
func marshalToString(v any) (string, error) {
	if v == nil {
		return "null", nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// unmarshalFromString unmarshal JSON from a string into a value
// Useful when the column is NOT NULL
func unmarshalFromString(data string, v any) error {
	if data == "" {
		return nil
	}
	return json.Unmarshal([]byte(data), v)
}

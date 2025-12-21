package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/saltyorg/autoplow/internal/throttle"
)

// Throttle settings database keys
const (
	throttleEnabledKey          = "throttle.enabled"
	throttleMaxBandwidthKey     = "throttle.max_bandwidth"
	throttleStreamBandwidthKey  = "throttle.stream_bandwidth"
	throttleMinBandwidthKey     = "throttle.min_bandwidth"
	throttleSkipUploadsBelowKey = "throttle.skip_uploads_below"
	throttlePollIntervalKey     = "throttle.poll_interval"
)

// SettingsThrottlePage renders the throttle settings page
func (h *Handlers) SettingsThrottlePage(w http.ResponseWriter, r *http.Request) {
	// Load config from database first
	config := h.loadThrottleConfigFromDB()

	if h.throttleMgr != nil {
		// Sync in-memory manager with database config
		h.throttleMgr.SetConfig(config)
	}

	// Check display units preference
	useBinary := true
	if val, _ := h.db.GetSetting("display.use_binary_units"); val == "false" {
		useBinary = false
	}
	useBits := true
	if val, _ := h.db.GetSetting("display.use_bits_for_bitrate"); val == "false" {
		useBits = false
	}

	// Convert bandwidth values to value+unit pairs for form display
	maxBw := bytesToValueUnit(config.MaxBandwidth, useBinary, useBits)
	streamBw := bytesToValueUnit(config.StreamBandwidth, useBinary, useBits)
	minBw := bytesToValueUnit(config.MinBandwidth, useBinary, useBits)
	skipBw := bytesToValueUnit(config.SkipUploadsBelow, useBinary, useBits)

	h.render(w, r, "settings.html", map[string]any{
		"Tab":               "throttle",
		"ThrottleConfig":    config,
		"UseBinaryUnits":    useBinary,
		"UseBitsForBitrate": useBits,
		// Form value+unit pairs
		"MaxBandwidthValue":     maxBw.Value,
		"MaxBandwidthUnit":      maxBw.Unit,
		"StreamBandwidthValue":  streamBw.Value,
		"StreamBandwidthUnit":   streamBw.Unit,
		"MinBandwidthValue":     minBw.Value,
		"MinBandwidthUnit":      minBw.Unit,
		"SkipUploadsBelowValue": skipBw.Value,
		"SkipUploadsBelowUnit":  skipBw.Unit,
	})
}

// SettingsThrottleUpdate handles throttle settings updates
func (h *Handlers) SettingsThrottleUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/throttle")
		return
	}

	var config throttle.Config

	// Parse form values
	config.Enabled = r.FormValue("enabled") == "on"

	// Parse bandwidth values with units
	config.MaxBandwidth = parseBandwidthWithUnit(
		r.FormValue("max_bandwidth_value"),
		r.FormValue("max_bandwidth_unit"),
	)

	config.StreamBandwidth = parseBandwidthWithUnit(
		r.FormValue("stream_bandwidth_value"),
		r.FormValue("stream_bandwidth_unit"),
	)

	config.MinBandwidth = parseBandwidthWithUnit(
		r.FormValue("min_bandwidth_value"),
		r.FormValue("min_bandwidth_unit"),
	)

	config.SkipUploadsBelow = parseBandwidthWithUnit(
		r.FormValue("skip_uploads_value"),
		r.FormValue("skip_uploads_unit"),
	)

	if pollInterval := r.FormValue("poll_interval"); pollInterval != "" {
		if secs, err := strconv.Atoi(pollInterval); err == nil && secs > 0 {
			config.PollInterval = parseDuration(secs)
		}
	} else {
		config.PollInterval = 30 * time.Second // default
	}

	// Save to database
	h.saveThrottleConfigToDB(config)

	// Update in-memory manager
	if h.throttleMgr != nil {
		h.throttleMgr.SetConfig(config)
	}

	h.flash(w, "Throttle settings updated")
	h.redirect(w, r, "/settings/throttle")
}

// SettingsThrottleStatus returns current throttle status (for HTMX polling)
func (h *Handlers) SettingsThrottleStatus(w http.ResponseWriter, r *http.Request) {
	if h.throttleMgr == nil {
		w.Write([]byte(`<span class="text-gray-500">Not initialized</span>`))
		return
	}

	stats := h.throttleMgr.Stats()

	var statusClass, statusText string
	if !stats.Enabled {
		statusClass = "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300"
		statusText = "Disabled"
	} else if stats.SessionCount > 0 {
		statusClass = "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200"
		statusText = "Throttling"
	} else {
		statusClass = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
		statusText = "Active"
	}

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(`<span class="inline-flex items-center rounded-md px-2.5 py-0.5 text-xs font-medium ` + statusClass + `">` + statusText + `</span>`))
}

// formatBandwidth converts bytes/sec to human-readable format using binary units (1024-based)
func formatBandwidth(bytesPerSec int64) string {
	return formatBandwidthWithUnits(bytesPerSec, true)
}

// formatBandwidthWithUnits converts bytes/sec to human-readable format
// useBinary: true = MiB/s (1024), false = MB/s (1000)
func formatBandwidthWithUnits(bytesPerSec int64, useBinary bool) string {
	return formatBandwidthWithOptions(bytesPerSec, useBinary, false)
}

// formatBandwidthWithOptions converts bytes/sec to human-readable format
// useBinary: true = 1024-based, false = 1000-based (only applies when useBits=false)
// useBits: true = display as Mbps (bits), false = display as MiB/s (bytes)
func formatBandwidthWithOptions(bytesPerSec int64, useBinary bool, useBits bool) string {
	if bytesPerSec == 0 {
		return "Unlimited"
	}

	// If displaying as bits, convert bytes/sec to bits/sec and use decimal (1000) base
	if useBits {
		bitsPerSec := bytesPerSec * 8
		var base float64 = 1000
		units := []string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}

		value := float64(bitsPerSec)
		unitIdx := 0
		for value >= base && unitIdx < len(units)-1 {
			value /= base
			unitIdx++
		}

		if unitIdx == 0 {
			return strconv.FormatInt(bitsPerSec, 10) + " " + units[unitIdx]
		}
		return strconv.FormatFloat(value, 'f', 1, 64) + " " + units[unitIdx]
	}

	// Bytes-based display
	var base int64
	var units []string
	if useBinary {
		base = 1024
		units = []string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"}
	} else {
		base = 1000
		units = []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	}

	value := float64(bytesPerSec)
	unitIdx := 0
	for value >= float64(base) && unitIdx < len(units)-1 {
		value /= float64(base)
		unitIdx++
	}

	if unitIdx == 0 {
		return strconv.FormatInt(bytesPerSec, 10) + " " + units[unitIdx]
	}
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + units[unitIdx]
}

// bandwidthValueUnit holds a bandwidth value split into numeric value and unit
type bandwidthValueUnit struct {
	Value float64
	Unit  string
}

// bytesToValueUnit converts bytes/sec to a value and unit for form display
// useBinary: true = 1024-based, false = 1000-based (only applies when useBits=false)
// useBits: true = display as Mbps (bits), false = display as MiB/s (bytes)
func bytesToValueUnit(bytesPerSec int64, useBinary bool, useBits bool) bandwidthValueUnit {
	if bytesPerSec == 0 {
		if useBits {
			return bandwidthValueUnit{Value: 0, Unit: "Mbit"}
		}
		return bandwidthValueUnit{Value: 0, Unit: "M"}
	}

	// If using bits, convert bytes/sec to bits/sec and use decimal (1000) base
	if useBits {
		bitsPerSec := bytesPerSec * 8
		const (
			Kbit int64 = 1000
			Mbit       = 1000 * Kbit
			Gbit       = 1000 * Mbit
		)

		switch {
		case bitsPerSec >= Gbit && bitsPerSec%Gbit == 0:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Gbit), Unit: "Gbit"}
		case bitsPerSec >= Mbit && bitsPerSec%Mbit == 0:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Mbit), Unit: "Mbit"}
		case bitsPerSec >= Kbit && bitsPerSec%Kbit == 0:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Kbit), Unit: "Kbit"}
		case bitsPerSec >= Gbit:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Gbit), Unit: "Gbit"}
		case bitsPerSec >= Mbit:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Mbit), Unit: "Mbit"}
		case bitsPerSec >= Kbit:
			return bandwidthValueUnit{Value: float64(bitsPerSec) / float64(Kbit), Unit: "Kbit"}
		default:
			// Fall back to bytes for very small values
			return bandwidthValueUnit{Value: float64(bytesPerSec), Unit: "B"}
		}
	}

	// Bytes-based display
	var base int64
	if useBinary {
		base = 1024
	} else {
		base = 1000
	}

	KB := base
	MB := base * KB
	GB := base * MB

	switch {
	case bytesPerSec >= GB && bytesPerSec%GB == 0:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(GB), Unit: "G"}
	case bytesPerSec >= MB && bytesPerSec%MB == 0:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(MB), Unit: "M"}
	case bytesPerSec >= KB && bytesPerSec%KB == 0:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(KB), Unit: "K"}
	case bytesPerSec >= GB:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(GB), Unit: "G"}
	case bytesPerSec >= MB:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(MB), Unit: "M"}
	case bytesPerSec >= KB:
		return bandwidthValueUnit{Value: float64(bytesPerSec) / float64(KB), Unit: "K"}
	default:
		return bandwidthValueUnit{Value: float64(bytesPerSec), Unit: "B"}
	}
}

// parseBandwidthWithUnit parses a value and unit into bytes/sec
// Always uses binary (1024-based) for internal storage consistency
// Supports both bytes-based units (B, K, M, G) and bits-based units (Kbit, Mbit, Gbit)
func parseBandwidthWithUnit(valueStr, unit string) int64 {
	if valueStr == "" {
		return 0
	}

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil || value == 0 {
		return 0
	}

	// Always store using binary base (1024) for consistency
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	// Bits-based units use decimal (1000) base, then convert to bytes
	const (
		Kbit = 1000 / 8        // Kbps to bytes/sec
		Mbit = 1000 * 1000 / 8 // Mbps to bytes/sec
		Gbit = 1000 * Mbit     // Gbps to bytes/sec
	)

	switch unit {
	case "K", "k":
		return int64(value * KB)
	case "M", "m":
		return int64(value * MB)
	case "G", "g":
		return int64(value * GB)
	case "Kbit":
		return int64(value * Kbit)
	case "Mbit":
		return int64(value * Mbit)
	case "Gbit":
		return int64(value * Gbit)
	default:
		return int64(value)
	}
}

// parseDuration converts seconds to time.Duration
func parseDuration(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}

// loadThrottleConfigFromDB loads throttle configuration from the database
func (h *Handlers) loadThrottleConfigFromDB() throttle.Config {
	return throttle.LoadConfigFromDB(h.db)
}

// saveThrottleConfigToDB saves throttle configuration to the database
func (h *Handlers) saveThrottleConfigToDB(config throttle.Config) {
	h.db.SetSetting(throttleEnabledKey, strconv.FormatBool(config.Enabled))
	h.db.SetSetting(throttleMaxBandwidthKey, strconv.FormatInt(config.MaxBandwidth, 10))
	h.db.SetSetting(throttleStreamBandwidthKey, strconv.FormatInt(config.StreamBandwidth, 10))
	h.db.SetSetting(throttleMinBandwidthKey, strconv.FormatInt(config.MinBandwidth, 10))
	h.db.SetSetting(throttleSkipUploadsBelowKey, strconv.FormatInt(config.SkipUploadsBelow, 10))
	h.db.SetSetting(throttlePollIntervalKey, strconv.Itoa(int(config.PollInterval.Seconds())))
}

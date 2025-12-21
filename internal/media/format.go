package media

import (
	"fmt"
	"regexp"
	"strings"
)

// resolutionRegex matches resolution patterns like "1080p", "1080i", "720p", "2160p", "4K", "8K"
var resolutionRegex = regexp.MustCompile(`\b(\d{3,4}[pi]|4K|8K)\b`)

// ParseVideoFormat extracts resolution and codec from DisplayTitle, falling back to height/codec if needed
func ParseVideoFormat(displayTitle string, height int, codec string) string {
	var res string

	// Try to extract resolution from DisplayTitle
	if displayTitle != "" {
		if match := resolutionRegex.FindString(displayTitle); match != "" {
			res = match
		}
	}

	// Fall back to height if no resolution found in DisplayTitle
	if res == "" {
		if height == 0 {
			return ""
		}
		res = fmt.Sprintf("%dp", height)
	}

	if codec == "" {
		return res
	}

	// Format codec nicely
	c := strings.ToUpper(codec)
	switch strings.ToLower(codec) {
	case "h264", "avc":
		c = "H.264"
	case "hevc", "h265":
		c = "HEVC"
	case "av1":
		c = "AV1"
	case "vp9":
		c = "VP9"
	case "mpeg4":
		c = "MPEG-4"
	}
	return fmt.Sprintf("%s (%s)", res, c)
}

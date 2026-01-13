package handlers

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/config"
	"github.com/saltyorg/autoplow/internal/processor"
)

// ProcessorSettings holds the processor configuration for display
type ProcessorSettings struct {
	AnchorEnabled   bool
	AnchorFiles     []string         // Default anchor files (all must exist)
	PathAnchorFiles []PathAnchorFile // Path-specific anchor files
	UploadsEnabled  bool
}

// PathAnchorFile represents a path-to-anchor file mapping
type PathAnchorFile struct {
	Path       string `json:"path"`
	AnchorFile string `json:"anchor_file"`
}

// SettingsProcessorPage renders the processor settings page
func (h *Handlers) SettingsProcessorPage(w http.ResponseWriter, r *http.Request) {
	loader := config.NewLoader(h.db)

	settings := ProcessorSettings{
		AnchorEnabled:  loader.BoolDefaultTrue("processor.anchor.enabled"),
		UploadsEnabled: loader.BoolDefaultTrue("uploads.enabled"),
	}

	// Load anchor files (JSON array)
	if val, _ := h.db.GetSetting("processor.anchor.anchor_files"); val != "" {
		if err := json.Unmarshal([]byte(val), &settings.AnchorFiles); err != nil {
			log.Error().Err(err).Msg("Failed to parse anchor_files setting")
		}
	}

	// Load path-specific anchor files (JSON array)
	if val, _ := h.db.GetSetting("processor.anchor.path_anchor_files"); val != "" {
		if err := json.Unmarshal([]byte(val), &settings.PathAnchorFiles); err != nil {
			log.Error().Err(err).Msg("Failed to parse path_anchor_files setting")
		}
	}

	h.render(w, r, "settings.html", map[string]any{
		"Tab":      "processor",
		"Settings": settings,
	})
}

// SettingsProcessorUpdate handles processor settings updates
func (h *Handlers) SettingsProcessorUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/settings/processor")
		return
	}

	// Parse form values
	anchorEnabled := r.FormValue("anchor_enabled") == "on"

	// Parse default anchor files list
	anchorFilesRaw := r.Form["anchor_files[]"]
	var anchorFiles []string
	for _, f := range anchorFilesRaw {
		f = strings.TrimSpace(f)
		if f != "" {
			f = filepath.Clean(f)
			anchorFiles = append(anchorFiles, f)
		}
	}

	// Parse path-specific anchor files
	pathAnchorPaths := r.Form["path_anchor_path[]"]
	pathAnchorFileNames := r.Form["path_anchor_file[]"]
	var pathAnchorFiles []PathAnchorFile
	pathAnchorMap := make(map[string]string)
	for i := 0; i < len(pathAnchorPaths) && i < len(pathAnchorFileNames); i++ {
		path := strings.TrimSpace(pathAnchorPaths[i])
		file := strings.TrimSpace(pathAnchorFileNames[i])
		if path != "" && file != "" {
			path = filepath.Clean(path)
			file = filepath.Clean(file)
			pathAnchorFiles = append(pathAnchorFiles, PathAnchorFile{Path: path, AnchorFile: file})
			pathAnchorMap[path] = file
		}
	}

	invalidAnchors := make([]string, 0, len(anchorFiles)+len(pathAnchorFiles))
	for _, file := range anchorFiles {
		if !filepath.IsAbs(file) {
			invalidAnchors = append(invalidAnchors, file)
		}
	}
	for _, mapping := range pathAnchorFiles {
		if !filepath.IsAbs(mapping.AnchorFile) {
			invalidAnchors = append(invalidAnchors, mapping.AnchorFile)
		}
	}
	if len(invalidAnchors) > 0 {
		h.flashErr(w, "Anchor files must be absolute paths")
		h.redirect(w, r, "/settings/processor")
		return
	}

	// Save to database
	var saveErr error
	if err := h.db.SetSetting("processor.anchor.enabled", strconv.FormatBool(anchorEnabled)); err != nil {
		saveErr = err
	}

	// Save default anchor files as JSON
	if anchorFilesJSON, err := json.Marshal(anchorFiles); err == nil {
		if err := h.db.SetSetting("processor.anchor.anchor_files", string(anchorFilesJSON)); err != nil {
			saveErr = err
		}
	}

	// Save path-specific anchor files as JSON
	if pathAnchorJSON, err := json.Marshal(pathAnchorFiles); err == nil {
		if err := h.db.SetSetting("processor.anchor.path_anchor_files", string(pathAnchorJSON)); err != nil {
			saveErr = err
		}
	}

	if saveErr != nil {
		log.Error().Err(saveErr).Msg("Failed to save some processor settings")
	}

	// Update processor config if available
	if h.processor != nil {
		newConfig := h.processor.Config()
		newConfig.Anchor = processor.AnchorConfig{
			Enabled:         anchorEnabled,
			AnchorFiles:     anchorFiles,
			PathAnchorFiles: pathAnchorMap,
		}
		h.processor.UpdateConfig(newConfig)
	}

	h.flash(w, "Processor settings saved")
	h.redirect(w, r, "/settings/processor")
}

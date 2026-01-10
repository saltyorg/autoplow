package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// RcloneOptionsPage renders the rclone options explorer page
func (h *Handlers) RcloneOptionsPage(w http.ResponseWriter, r *http.Request) {
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "global"
	}

	var globalOptions map[string]any
	var providers []providerSummary
	var configuredRemotes []string
	var err error
	var rcloneRunning bool

	if h.rcloneMgr != nil && h.rcloneMgr.IsRunning() {
		rcloneRunning = true

		// Get global options (these need to be fetched live as they change)
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		globalOptions, _ = h.rcloneMgr.Client().GetOptions(ctx)

		// Get providers list from cache (just names and descriptions for the list)
		providersList := h.rcloneMgr.Providers()
		for _, p := range providersList {
			providers = append(providers, providerSummary{
				Name:        p.Name,
				Description: p.Description,
				Prefix:      p.Prefix,
			})
		}

		// Sort providers alphabetically
		sort.Slice(providers, func(i, j int) bool {
			return providers[i].Name < providers[j].Name
		})

		// Get configured remotes from cache
		configuredRemotes = h.rcloneMgr.ConfiguredRemotes()
	}

	h.render(w, r, "rclone_options.html", map[string]any{
		"Tab":               tab,
		"GlobalOptions":     globalOptions,
		"Providers":         providers,
		"ConfiguredRemotes": configuredRemotes,
		"Error":             err,
		"RcloneRunning":     rcloneRunning,
	})
}

type providerSummary struct {
	Name        string
	Description string
	Prefix      string
}

// RcloneOptionsAPI returns rclone options as JSON
func (h *Handlers) RcloneOptionsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   "Rclone RCD is not running",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	options, err := h.rcloneMgr.Client().GetOptions(ctx)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"options": options,
	})
}

// RcloneProvidersAPI returns all backend providers as JSON
func (h *Handlers) RcloneProvidersAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   "Rclone RCD is not running",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	providers, err := h.rcloneMgr.Client().GetProviders(ctx)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":   true,
		"providers": providers,
	})
}

// RcloneProviderOptionsPartial returns the options for a specific provider as HTML partial
func (h *Handlers) RcloneProviderOptionsPartial(w http.ResponseWriter, r *http.Request) {
	providerName := chi.URLParam(r, "provider")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-red-500">Rclone RCD is not running</div>`))
		return
	}

	// Use cached providers
	providers := h.rcloneMgr.Providers()
	if len(providers) == 0 {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-yellow-500">Provider data is loading, please wait...</div>`))
		return
	}

	// Find the requested provider
	var provider *struct {
		Name        string
		Description string
		Options     []map[string]any
	}
	for _, p := range providers {
		if p.Name == providerName || p.Prefix == providerName {
			// Convert options to map for easier template handling
			opts := make([]map[string]any, 0, len(p.Options))
			for _, opt := range p.Options {
				optMap := map[string]any{
					"Name":       opt.Name,
					"Help":       opt.Help,
					"Default":    opt.Default,
					"DefaultStr": opt.DefaultStr,
					"Required":   opt.Required,
					"Advanced":   opt.Advanced,
					"IsPassword": opt.IsPassword,
					"Type":       opt.Type,
					"Provider":   opt.Provider,
				}
				if len(opt.Examples) > 0 {
					examples := make([]map[string]string, 0, len(opt.Examples))
					for _, ex := range opt.Examples {
						examples = append(examples, map[string]string{
							"Value": ex.Value,
							"Help":  ex.Help,
						})
					}
					optMap["Examples"] = examples
				}
				opts = append(opts, optMap)
			}
			provider = &struct {
				Name        string
				Description string
				Options     []map[string]any
			}{
				Name:        p.Name,
				Description: p.Description,
				Options:     opts,
			}
			break
		}
	}

	if provider == nil {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-red-500">Provider not found: ` + providerName + `</div>`))
		return
	}

	h.renderPartial(w, "rclone_options.html", "provider_options", map[string]any{
		"Provider": provider,
	})
}

// RcloneGlobalOptionsPartial returns global options for a specific category as HTML partial
func (h *Handlers) RcloneGlobalOptionsPartial(w http.ResponseWriter, r *http.Request) {
	category := chi.URLParam(r, "category")

	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-red-500">Rclone RCD is not running</div>`))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	options, err := h.rcloneMgr.Client().GetOptions(ctx)
	if err != nil {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-red-500">Error: ` + err.Error() + `</div>`))
		return
	}

	categoryOptions, ok := options[category]
	if !ok {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<div class="text-red-500">Category not found: ` + category + `</div>`))
		return
	}

	h.renderPartial(w, "rclone_options.html", "category_options", map[string]any{
		"Category": category,
		"Options":  categoryOptions,
	})
}

// RcloneOptionsUpdate handles updating global rclone options
func (h *Handlers) RcloneOptionsUpdate(w http.ResponseWriter, r *http.Request) {
	if h.rcloneMgr == nil || !h.rcloneMgr.IsRunning() {
		h.flashErr(w, "Rclone is not running")
		h.redirect(w, r, "/rclone")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.flashErr(w, "Invalid form data")
		h.redirect(w, r, "/rclone")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Build the options to set - structure: {"main": {"Transfers": 4, ...}}
	mainOpts := make(map[string]any)

	// Parse transfers
	if v := r.FormValue("transfers"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Transfers"] = i
		}
	}

	// Parse checkers
	if v := r.FormValue("checkers"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			mainOpts["Checkers"] = i
		}
	}

	// Parse buffer size (stored in bytes)
	if v := r.FormValue("buffer_size"); v != "" {
		if size, err := strconv.ParseInt(v, 10, 64); err == nil {
			mainOpts["BufferSize"] = size
		}
	}

	// Parse bandwidth limit
	if v := r.FormValue("bwlimit"); v != "" {
		mainOpts["BwLimit"] = v
	}

	if len(mainOpts) > 0 {
		options := map[string]any{
			"main": mainOpts,
		}

		if err := h.rcloneMgr.Client().SetOptions(ctx, options); err != nil {
			h.flashErr(w, "Failed to update options: "+err.Error())
			h.redirect(w, r, "/rclone")
			return
		}
	}

	h.flash(w, "Rclone options updated successfully")
	h.redirect(w, r, "/rclone")
}

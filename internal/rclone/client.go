package rclone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/saltyorg/autoplow/internal/httpclient"
)

// ClientConfig holds rclone RCD client configuration
type ClientConfig struct {
	Address  string        `json:"address"`  // Default: "127.0.0.1:5572"
	Username string        `json:"username"` // RCD authentication
	Password string        `json:"password"`
	Timeout  time.Duration `json:"timeout"`
}

// DefaultClientConfig returns default client configuration
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Address: "127.0.0.1:5572",
		Timeout: 30 * time.Second,
	}
}

// Client is the rclone RCD HTTP client
type Client struct {
	config     ClientConfig
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new rclone RCD client
func NewClient(config ClientConfig) *Client {
	if config.Address == "" {
		config.Address = "127.0.0.1:5572"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	return &Client{
		config:     config,
		httpClient: httpclient.NewTraceClient("rclone", config.Timeout),
		baseURL:    fmt.Sprintf("http://%s", config.Address),
	}
}

// CopyRequest represents parameters for copy/move operations
type CopyRequest struct {
	SrcFs           string         `json:"srcFs"`               // Source filesystem (e.g., "/local/path")
	DstFs           string         `json:"dstFs"`               // Destination filesystem (e.g., "remote:path")
	SrcRemote       string         `json:"srcRemote,omitempty"` // Relative source path
	DstRemote       string         `json:"dstRemote,omitempty"` // Relative destination path
	TransferOptions map[string]any `json:"-"`                   // Optional transfer options (merged into params)
}

// JobResponse represents the response from async operations
type JobResponse struct {
	JobID int64 `json:"jobid"`
}

// JobStatus represents rclone job status
type JobStatus struct {
	ID        int64   `json:"id"`
	Duration  float64 `json:"duration"`
	EndTime   string  `json:"endTime,omitempty"`
	Error     string  `json:"error,omitempty"`
	Finished  bool    `json:"finished"`
	StartTime string  `json:"startTime"`
	Success   bool    `json:"success"`
	Group     string  `json:"group,omitempty"`
	Output    any     `json:"output,omitempty"` // For batch jobs, contains per-operation results
}

// JobListResponse represents the response from job/list
type JobListResponse struct {
	JobIDs []int64 `json:"jobids"`
}

// TransferStats represents transfer statistics from core/stats
type TransferStats struct {
	Bytes          int64              `json:"bytes"`
	Checks         int64              `json:"checks"`
	DeletedDirs    int64              `json:"deletedDirs"`
	Deletes        int64              `json:"deletes"`
	ElapsedTime    float64            `json:"elapsedTime"`
	Errors         int64              `json:"errors"`
	ETA            *int64             `json:"eta,omitempty"`
	FatalError     bool               `json:"fatalError"`
	Renames        int64              `json:"renames"`
	RetryError     bool               `json:"retryError"`
	Speed          float64            `json:"speed"`
	TotalBytes     int64              `json:"totalBytes"`
	TotalChecks    int64              `json:"totalChecks"`
	TotalTransfers int64              `json:"totalTransfers"`
	TransferTime   float64            `json:"transferTime"`
	Transfers      int64              `json:"transfers"`
	Transferring   []TransferringFile `json:"transferring,omitempty"`
}

// TransferringFile represents an active transfer
type TransferringFile struct {
	Name       string  `json:"name"`
	Size       int64   `json:"size"`
	Bytes      int64   `json:"bytes"`
	Percentage int     `json:"percentage"`
	Speed      float64 `json:"speed"`
	SpeedAvg   float64 `json:"speedAvg"`
	ETA        *int64  `json:"eta,omitempty"`
}

// TransferredFile represents a completed transfer from core/transferred
type TransferredFile struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	Bytes     int64  `json:"bytes"`
	Checked   bool   `json:"checked"`
	Timestamp string `json:"timestamp"`
	Error     string `json:"error"`
	JobID     int64  `json:"jobid"`
}

// TransferredResponse represents the response from core/transferred
type TransferredResponse struct {
	Transferred []TransferredFile `json:"transferred"`
}

// BandwidthLimit represents bandwidth limit settings
type BandwidthLimit struct {
	BytesPerSecond   int64  `json:"bytesPerSecond"`
	BytesPerSecondTx int64  `json:"bytesPerSecondTx"`
	BytesPerSecondRx int64  `json:"bytesPerSecondRx"`
	Rate             string `json:"rate"`
}

// RemoteListResponse represents the response from config/listremotes
type RemoteListResponse struct {
	Remotes []string `json:"remotes"`
}

// BatchInput represents a single operation in a batch request
type BatchInput struct {
	Path   string         `json:"_path"` // RC endpoint path, e.g., "operations/copyfile"
	Params map[string]any `json:"-"`     // Additional parameters (merged into the input)
}

// MarshalJSON custom marshaler to flatten Params into the JSON object
func (b BatchInput) MarshalJSON() ([]byte, error) {
	m := make(map[string]any)
	m["_path"] = b.Path
	maps.Copy(m, b.Params)
	return json.Marshal(m)
}

// BatchResultItem represents the result of a single operation in a batch
type BatchResultItem struct {
	Success bool           `json:"success,omitempty"`
	Status  int            `json:"status,omitempty"`
	Error   string         `json:"error,omitempty"`
	Path    string         `json:"path,omitempty"`
	Input   map[string]any `json:"input,omitempty"`
	Result  map[string]any `json:"result,omitempty"`
}

// VersionResponse represents the response from core/version
type VersionResponse struct {
	Version   string `json:"version"`
	Arch      string `json:"arch"`
	GoVersion string `json:"goVersion"`
	Os        string `json:"os"`
}

// call makes an HTTP POST request to the RCD API
func (c *Client) call(ctx context.Context, endpoint string, params any, result any) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, endpoint)

	// Always send at least an empty JSON object - rclone RCD requires it
	var jsonData []byte
	var err error
	if params != nil {
		jsonData, err = json.Marshal(params)
		if err != nil {
			return fmt.Errorf("failed to marshal params: %w", err)
		}
	} else {
		jsonData = []byte("{}")
	}
	body := bytes.NewReader(jsonData)

	log.Debug().Str("endpoint", endpoint).RawJSON("params", jsonData).Msg("Rclone API request")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add basic auth if configured
	if c.config.Username != "" || c.config.Password != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	log.Trace().Str("endpoint", endpoint).RawJSON("response", respBody).Msg("Rclone API response")

	if resp.StatusCode != http.StatusOK {
		// Try to parse error response
		var errResp struct {
			Error  string `json:"error"`
			Status int    `json:"status"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			return fmt.Errorf("rclone error: %s", errResp.Error)
		}
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// Ping tests connectivity to rclone RCD
func (c *Client) Ping(ctx context.Context) error {
	return c.call(ctx, "rc/noop", nil, nil)
}

// Version returns the rclone version info
func (c *Client) Version(ctx context.Context) (*VersionResponse, error) {
	var result VersionResponse
	if err := c.call(ctx, "core/version", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CopyFile copies a file asynchronously
func (c *Client) CopyFile(ctx context.Context, req CopyRequest) (int64, error) {
	params := map[string]any{
		"srcFs":     req.SrcFs,
		"dstFs":     req.DstFs,
		"srcRemote": req.SrcRemote,
		"dstRemote": req.DstRemote,
		"_async":    true,
	}

	// Merge transfer options into params
	maps.Copy(params, req.TransferOptions)

	var result JobResponse
	if err := c.call(ctx, "operations/copyfile", params, &result); err != nil {
		return 0, err
	}
	return result.JobID, nil
}

// MoveFile moves a file asynchronously
func (c *Client) MoveFile(ctx context.Context, req CopyRequest) (int64, error) {
	params := map[string]any{
		"srcFs":     req.SrcFs,
		"dstFs":     req.DstFs,
		"srcRemote": req.SrcRemote,
		"dstRemote": req.DstRemote,
		"_async":    true,
	}

	// Merge transfer options into params
	maps.Copy(params, req.TransferOptions)

	var result JobResponse
	if err := c.call(ctx, "operations/movefile", params, &result); err != nil {
		return 0, err
	}
	return result.JobID, nil
}

// Copy copies a directory asynchronously
func (c *Client) Copy(ctx context.Context, srcFs, dstFs string) (int64, error) {
	params := map[string]any{
		"srcFs":  srcFs,
		"dstFs":  dstFs,
		"_async": true,
	}

	var result JobResponse
	if err := c.call(ctx, "sync/copy", params, &result); err != nil {
		return 0, err
	}
	return result.JobID, nil
}

// Move moves a directory asynchronously
func (c *Client) Move(ctx context.Context, srcFs, dstFs string) (int64, error) {
	params := map[string]any{
		"srcFs":              srcFs,
		"dstFs":              dstFs,
		"deleteEmptySrcDirs": true,
		"_async":             true,
	}

	var result JobResponse
	if err := c.call(ctx, "sync/move", params, &result); err != nil {
		return 0, err
	}
	return result.JobID, nil
}

// Batch executes multiple RC operations as a single batch job.
// Returns the batch job ID which can be polled via GetJobStatus.
// The batch uses rclone's Transfers setting for concurrency.
func (c *Client) Batch(ctx context.Context, inputs []BatchInput) (int64, error) {
	params := map[string]any{
		"inputs": inputs,
		"_async": true,
	}

	var result JobResponse
	if err := c.call(ctx, "job/batch", params, &result); err != nil {
		return 0, err
	}
	return result.JobID, nil
}

// GetJobStatus returns the status of a specific job
func (c *Client) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	params := map[string]any{
		"jobid": jobID,
	}

	var result JobStatus
	if err := c.call(ctx, "job/status", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetStats returns current transfer statistics
func (c *Client) GetStats(ctx context.Context) (*TransferStats, error) {
	var result TransferStats
	if err := c.call(ctx, "core/stats", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetJobStats returns transfer statistics for a specific job
func (c *Client) GetJobStats(ctx context.Context, jobID int64) (*TransferStats, error) {
	params := map[string]any{
		"group": fmt.Sprintf("job/%d", jobID),
	}

	var result TransferStats
	if err := c.call(ctx, "core/stats", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetGroupStats returns transfer statistics for a specific stats group
func (c *Client) GetGroupStats(ctx context.Context, group string) (*TransferStats, error) {
	params := map[string]any{
		"group": group,
	}

	var result TransferStats
	if err := c.call(ctx, "core/stats", params, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetGroupTransferred returns completed transfers for a specific stats group
func (c *Client) GetGroupTransferred(ctx context.Context, group string) ([]TransferredFile, error) {
	params := map[string]any{
		"group": group,
	}

	var result TransferredResponse
	if err := c.call(ctx, "core/transferred", params, &result); err != nil {
		return nil, err
	}
	return result.Transferred, nil
}

// ListJobs returns all active/recent job IDs
func (c *Client) ListJobs(ctx context.Context) ([]int64, error) {
	var result JobListResponse
	if err := c.call(ctx, "job/list", nil, &result); err != nil {
		return nil, err
	}
	return result.JobIDs, nil
}

// StopJob stops a running job
func (c *Client) StopJob(ctx context.Context, jobID int64) error {
	params := map[string]any{
		"jobid": jobID,
	}
	return c.call(ctx, "job/stop", params, nil)
}

// StopGroup stops all running jobs in a specific group.
func (c *Client) StopGroup(ctx context.Context, group string) error {
	params := map[string]any{
		"group": group,
	}
	return c.call(ctx, "job/stopgroup", params, nil)
}

// Mkdir creates a directory on the remote.
// fs should be the full remote path like "remote:path/to/directory"
func (c *Client) Mkdir(ctx context.Context, fs string) error {
	params := map[string]any{
		"fs":     fs,
		"remote": "",
	}
	return c.call(ctx, "operations/mkdir", params, nil)
}

// SetBandwidthLimit sets the bandwidth limit
// rate can be: "off", "1M", "10M:1M" (upload:download), etc.
func (c *Client) SetBandwidthLimit(ctx context.Context, rate string) error {
	params := map[string]any{
		"rate": rate,
	}
	return c.call(ctx, "core/bwlimit", params, nil)
}

// GetBandwidthLimit returns current bandwidth limit settings
func (c *Client) GetBandwidthLimit(ctx context.Context) (*BandwidthLimit, error) {
	var result BandwidthLimit
	if err := c.call(ctx, "core/bwlimit", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListRemotes returns configured rclone remotes
func (c *Client) ListRemotes(ctx context.Context) ([]string, error) {
	var result RemoteListResponse
	if err := c.call(ctx, "config/listremotes", nil, &result); err != nil {
		return nil, err
	}
	return result.Remotes, nil
}

// StatsReset resets the global stats
func (c *Client) StatsReset(ctx context.Context) error {
	return c.call(ctx, "core/stats-reset", nil, nil)
}

// StatsDelete deletes stats for a specific group (job)
func (c *Client) StatsDelete(ctx context.Context, group string) error {
	params := map[string]any{
		"group": group,
	}
	return c.call(ctx, "core/stats-delete", params, nil)
}

// GetJobTransferred returns completed transfers for a specific job's stats group.
// Uses the group parameter to filter by "job/{jobID}" to avoid cross-contamination
// from other concurrent jobs. Returns the last 100 completed transfers for the group.
func (c *Client) GetJobTransferred(ctx context.Context, jobID int64) ([]TransferredFile, error) {
	params := map[string]any{
		"group": fmt.Sprintf("job/%d", jobID),
	}

	var result TransferredResponse
	if err := c.call(ctx, "core/transferred", params, &result); err != nil {
		return nil, err
	}
	return result.Transferred, nil
}

// GetOptions returns all rclone global options via options/get
func (c *Client) GetOptions(ctx context.Context) (map[string]any, error) {
	var result map[string]any
	if err := c.call(ctx, "options/get", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// SetOptions sets rclone global options via options/set
func (c *Client) SetOptions(ctx context.Context, options map[string]any) error {
	return c.call(ctx, "options/set", options, nil)
}

// OptionVisibility controls whether the options are visible in the
// configurator or the command line.
type OptionVisibility byte

const (
	OptionHideCommandLine OptionVisibility = 1 << iota
	OptionHideConfigurator
	OptionHideBoth = OptionHideCommandLine | OptionHideConfigurator
)

// OptionExample describes an example for an Option
type OptionExample struct {
	Value    string `json:"Value"`
	Help     string `json:"Help"`
	Provider string `json:"Provider,omitempty"`
}

// Option describes an option for a backend or global config.
// Based on rclone's fs.Option struct.
type Option struct {
	Name       string           `json:"Name"`
	FieldName  string           `json:"FieldName,omitempty"`
	Help       string           `json:"Help"`
	Groups     string           `json:"Groups,omitempty"`
	Provider   string           `json:"Provider,omitempty"`
	Default    any              `json:"Default"`
	Value      any              `json:"Value,omitempty"`
	Examples   []OptionExample  `json:"Examples,omitempty"`
	ShortOpt   string           `json:"ShortOpt,omitempty"`
	Hide       OptionVisibility `json:"Hide,omitempty"`
	Required   bool             `json:"Required"`
	IsPassword bool             `json:"IsPassword"`
	NoPrefix   bool             `json:"NoPrefix,omitempty"`
	Advanced   bool             `json:"Advanced"`
	Exclusive  bool             `json:"Exclusive,omitempty"`
	Sensitive  bool             `json:"Sensitive,omitempty"`
	DefaultStr string           `json:"DefaultStr,omitempty"`
	ValueStr   string           `json:"ValueStr,omitempty"`
	Type       string           `json:"Type,omitempty"`
}

// Provider represents a backend provider (e.g., s3, gdrive, etc.)
type Provider struct {
	Name        string   `json:"Name"`
	Description string   `json:"Description"`
	Prefix      string   `json:"Prefix"`
	Options     []Option `json:"Options"`
}

// ProvidersResponse represents the response from config/providers
type ProvidersResponse struct {
	Providers []Provider `json:"providers"`
}

// GetProviders returns all available backend providers and their options
func (c *Client) GetProviders(ctx context.Context) ([]Provider, error) {
	var result ProvidersResponse
	if err := c.call(ctx, "config/providers", nil, &result); err != nil {
		return nil, err
	}
	return result.Providers, nil
}

// GetBackendOptions returns options for a specific backend/remote
func (c *Client) GetBackendOptions(ctx context.Context, remoteName string) (map[string]any, error) {
	params := map[string]any{
		"fs": remoteName + ":",
	}
	var result map[string]any
	if err := c.call(ctx, "options/local", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

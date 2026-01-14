package gdrive

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"google.golang.org/api/drive/v3"
)

// PathResolver resolves Google Drive file or folder IDs to full paths with caching.
type PathResolver struct {
	ctx        context.Context
	svc        *drive.Service
	driveID    string
	rootID     string
	rootPrefix string
	mu         sync.Mutex
	cache      map[string]*drive.File
	pathCache  map[string]resolvedPath
}

type resolvedPath struct {
	path      string
	ancestors []string
}

// NewPathResolver creates a resolver for a specific drive ID and name.
func NewPathResolver(ctx context.Context, svc *drive.Service, driveID, driveName, rootID string) *PathResolver {
	prefix := RootPrefix(driveID, driveName)
	if ctx == nil {
		ctx = context.Background()
	}

	return &PathResolver{
		ctx:        ctx,
		svc:        svc,
		driveID:    driveID,
		rootID:     strings.TrimSpace(rootID),
		rootPrefix: prefix,
		cache:      make(map[string]*drive.File),
		pathCache:  make(map[string]resolvedPath),
	}
}

// ResolvePath resolves a file ID to a full path and its ancestor IDs.
func (r *PathResolver) ResolvePath(fileID string) (string, []string, error) {
	if r.driveID == "" && r.rootID == "" {
		if err := r.ensureRootID(); err != nil {
			return "", nil, err
		}
	}

	r.mu.Lock()
	if cached, ok := r.pathCache[fileID]; ok {
		path := cached.path
		ancestors := append([]string(nil), cached.ancestors...)
		r.mu.Unlock()
		return path, ancestors, nil
	}
	r.mu.Unlock()

	file, err := r.getFile(fileID)
	if err != nil {
		return "", nil, err
	}

	path, ancestors, err := r.resolve(file)
	if err != nil {
		return "", nil, err
	}

	r.mu.Lock()
	r.pathCache[fileID] = resolvedPath{path: path, ancestors: ancestors}
	r.mu.Unlock()

	return path, ancestors, nil
}

// SeedFile stores a change-list file in the cache to avoid extra API calls.
func (r *PathResolver) SeedFile(file *drive.File) {
	if file == nil {
		return
	}
	fileID := strings.TrimSpace(file.Id)
	if fileID == "" {
		return
	}

	cleaned := &drive.File{
		Id:       fileID,
		Name:     strings.TrimSpace(file.Name),
		MimeType: strings.TrimSpace(file.MimeType),
		Trashed:  file.Trashed,
	}
	if len(file.Parents) > 0 {
		parents := make([]string, 0, len(file.Parents))
		for _, parent := range file.Parents {
			parent = strings.TrimSpace(parent)
			if parent != "" {
				parents = append(parents, parent)
			}
		}
		cleaned.Parents = parents
	}

	r.mu.Lock()
	if cached, ok := r.cache[fileID]; ok && cached != nil {
		if cleaned.Name != "" {
			cached.Name = cleaned.Name
		}
		if len(cleaned.Parents) > 0 {
			cached.Parents = cleaned.Parents
		}
		if cleaned.MimeType != "" {
			cached.MimeType = cleaned.MimeType
		}
		cached.Trashed = cleaned.Trashed
	} else {
		r.cache[fileID] = cleaned
	}
	delete(r.pathCache, fileID)
	r.mu.Unlock()
}

func (r *PathResolver) resolve(file *drive.File) (string, []string, error) {
	if file == nil {
		return "", nil, fmt.Errorf("file not found")
	}

	if r.isRootID(file.Id) {
		return r.rootPrefix, []string{file.Id}, nil
	}

	if len(file.Parents) == 0 || file.Parents[0] == "root" || (r.driveID != "" && file.Parents[0] == r.driveID) || (r.rootID != "" && file.Parents[0] == r.rootID) {
		path := filepath.ToSlash(filepath.Join(r.rootPrefix, file.Name))
		return path, []string{file.Id}, nil
	}

	parentID := file.Parents[0]
	parentFile, err := r.getFile(parentID)
	if err != nil {
		return "", nil, err
	}

	parentPath, ancestors, err := r.resolve(parentFile)
	if err != nil {
		return "", nil, err
	}

	path := filepath.ToSlash(filepath.Join(parentPath, file.Name))
	ancestors = append(ancestors, file.Id)
	return path, ancestors, nil
}

func (r *PathResolver) getFile(fileID string) (*drive.File, error) {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	if cached, ok := r.cache[fileID]; ok {
		r.mu.Unlock()
		return cached, nil
	}
	r.mu.Unlock()

	req := r.svc.Files.Get(fileID).
		SupportsAllDrives(true).
		Fields("id,name,parents,mimeType,trashed")

	file, err := doGDriveRequest(ctx, "gdrive.files.get", func() (*drive.File, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return nil, err
	}
	if log.Trace().Enabled() {
		log.Trace().
			Str("file_id", fileID).
			Interface("response", file).
			Msg("GDrive file response")
	}

	file.Name = strings.TrimSpace(file.Name)

	r.mu.Lock()
	r.cache[fileID] = file
	r.mu.Unlock()

	return file, nil
}

func (r *PathResolver) ensureRootID() error {
	r.mu.Lock()
	if r.rootID != "" {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	req := r.svc.Files.Get("root").SupportsAllDrives(true).Fields("id")
	root, err := doGDriveRequest(ctx, "gdrive.files.get_root", func() (*drive.File, error) {
		return req.Context(ctx).Do()
	})
	if err != nil {
		return err
	}

	rootID := strings.TrimSpace(root.Id)
	if rootID == "" {
		return fmt.Errorf("missing gdrive root id")
	}

	r.mu.Lock()
	r.rootID = rootID
	r.mu.Unlock()
	return nil
}

func (r *PathResolver) isRootID(fileID string) bool {
	if fileID == "" {
		return false
	}
	if fileID == "root" {
		return true
	}
	if r.driveID != "" && fileID == r.driveID {
		return true
	}
	return r.rootID != "" && fileID == r.rootID
}

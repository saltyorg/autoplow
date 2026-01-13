package gdrive

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"google.golang.org/api/drive/v3"
)

// PathResolver resolves Google Drive file or folder IDs to full paths with caching.
type PathResolver struct {
	svc        *drive.Service
	driveID    string
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
func NewPathResolver(svc *drive.Service, driveID, driveName string) *PathResolver {
	prefix := RootPrefix(driveID, driveName)

	return &PathResolver{
		svc:        svc,
		driveID:    driveID,
		rootPrefix: prefix,
		cache:      make(map[string]*drive.File),
		pathCache:  make(map[string]resolvedPath),
	}
}

// ResolvePath resolves a file ID to a full path and its ancestor IDs.
func (r *PathResolver) ResolvePath(fileID string) (string, []string, error) {
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

func (r *PathResolver) resolve(file *drive.File) (string, []string, error) {
	if file == nil {
		return "", nil, fmt.Errorf("file not found")
	}

	if len(file.Parents) == 0 || file.Parents[0] == "root" || (r.driveID != "" && file.Parents[0] == r.driveID) {
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
	r.mu.Lock()
	if cached, ok := r.cache[fileID]; ok {
		r.mu.Unlock()
		return cached, nil
	}
	r.mu.Unlock()

	req := r.svc.Files.Get(fileID).
		SupportsAllDrives(true).
		Fields("id,name,parents,mimeType,trashed")

	file, err := req.Do()
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

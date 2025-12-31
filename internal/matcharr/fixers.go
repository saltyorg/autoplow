package matcharr

import (
	"context"
	"fmt"

	"github.com/saltyorg/autoplow/internal/database"
)

// TargetFixer is the interface that targets must implement to support matcharr fixes
type TargetFixer interface {
	// GetLibraryItemsWithProviderIDs returns all items in a library with their provider IDs
	GetLibraryItemsWithProviderIDs(ctx context.Context, libraryID string) ([]MediaServerItem, error)

	// MatchItem updates an item's metadata to match a specific provider ID
	MatchItem(ctx context.Context, itemID string, idType string, idValue string, title string) error

	// Name returns the target name for logging
	Name() string

	// Type returns the target type
	Type() database.TargetType
}

// FixMismatch fixes a single mismatch by updating the media server item
func FixMismatch(ctx context.Context, fixer TargetFixer, mismatch *Mismatch) error {
	if fixer == nil {
		return fmt.Errorf("no fixer available for target type")
	}

	return fixer.MatchItem(
		ctx,
		mismatch.ServerItem.ItemID,
		mismatch.ExpectedIDType,
		mismatch.ExpectedID,
		mismatch.ArrMedia.Title,
	)
}

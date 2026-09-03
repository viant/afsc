package op

import (
	"context"
	"io"
	"os"

	"github.com/viant/afs/storage"
)

// Upload is not supported.
func (s *Storager) Upload(ctx context.Context, resourceID string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	return errUnsupported
}

// Create is not supported.
func (s *Storager) Create(ctx context.Context, resourceID string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	return errUnsupported
}

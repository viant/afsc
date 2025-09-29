package assets

import (
	"context"
	"io"
	"os"

	"github.com/viant/afs/storage"
)

// Create creates a resource. Directories are not applicable for OpenAI assets; files delegate to Upload.
func (s *Storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	if isDir {
		return nil
	}
	return s.Upload(ctx, destination, mode, reader, options...)
}

package secretmanager

import (
	"context"
	"io"
	"os"

	"github.com/viant/afs/storage"
)

// Create create file or directory
func (s *Storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	return nil
}

package ssm

import (
	"context"
	"github.com/viant/afs/storage"
	"io"
	"os"
)

//Create create file or directory
func (s *storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	return nil
}

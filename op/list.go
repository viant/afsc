package op

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
)

// Exists returns true when op read succeeds for the supplied reference.
func (s *Storager) Exists(ctx context.Context, resourceID string, options ...storage.Option) (bool, error) {
	ref, err := reference(s.baseURL, resourceID)
	if err != nil {
		return false, err
	}
	_, err = s.cli.Read(ctx, ref)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// Get returns file info for a readable secret reference.
func (s *Storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	list, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("no found: %v", location)
	}
	return list[0], nil
}

// Delete is not supported.
func (s *Storager) Delete(ctx context.Context, location string, options ...storage.Option) error {
	return errUnsupported
}

// Close closes the storager.
func (s *Storager) Close() error {
	return nil
}

// List returns file info for a readable secret reference.
func (s *Storager) List(ctx context.Context, resourceID string, options ...storage.Option) ([]os.FileInfo, error) {
	ref, err := reference(s.baseURL, resourceID)
	if err != nil {
		return nil, err
	}
	data, err := s.cli.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	name := resourceID
	if name == "" || name == "/" {
		name = ref
	}
	return []os.FileInfo{
		file.NewInfo(name, int64(len(data)), file.DefaultFileOsMode, time.Now(), false),
	}, nil
}

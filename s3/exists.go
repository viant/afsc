package s3

import (
	"context"
	"path"
	"strings"

	"github.com/viant/afs/storage"
)

// Exists returns true if object exists
func (s *Storager) Exists(ctx context.Context, location string, options ...storage.Option) (bool, error) {
	object, err := s.Get(ctx, location)
	if isNotFound(err) {
		err = nil
	}
	if object == nil {
		return false, nil
	}
	name := location
	if strings.Index(location, "/") != -1 {
		_, name = path.Split(location)
	}
	return object.Name() == name, err
}

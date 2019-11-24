package s3

import (
	"context"
	"github.com/viant/afs/storage"
)

//Exists returns true if object exists
func (s *storager) Exists(ctx context.Context, location string, options ...storage.Option) (bool, error) {
	object, err := s.Get(ctx, location)
	if isNotFound(err) {
		err = nil
	}
	return object != nil, err
}

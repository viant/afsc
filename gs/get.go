package gs

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"os"
	"strings"
)

//Get returns an object for supplied location
func (s *storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	location = strings.Trim(location, "/")
	objectCall := s.Objects.Get(s.bucket, location)
	objectCall.Context(ctx)
	object, _ := objectCall.Do()
	if object != nil {
		return newFileInfo(object)
	}
	options = append(options, option.NewPage(0, 1))
	objects, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, errors.Errorf("%v not found", location)
	}
	return objects[0], err
}

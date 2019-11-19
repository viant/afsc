package gs

import (
	"context"
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
	list, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	return list[0], err
}

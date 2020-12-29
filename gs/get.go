package gs

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
	"os"
	"strings"
)

//Get returns an object for supplied location
func (s *storager) get(ctx context.Context, location string, options []storage.Option) (os.FileInfo, error) {
	object, err := s.getObject(ctx, location, options)
	if object != nil {
		return newFileInfo(object)
	}
	return nil, err
}

//Get returns an object for supplied location
func (s *storager) getObject(ctx context.Context, location string, options []storage.Option) (object *gstorage.Object, err error) {
	location = strings.Trim(location, "/")
	objectCall := s.Objects.Get(s.bucket, location)
	objectCall.Context(ctx)
	key := &option.AES256Key{}
	option.Assign(options, &key)
	if len(key.Key) != 0 {
		if err := SetCustomKeyHeader(key, objectCall.Header()); err != nil {
			return nil, err
		}
	}
	err = runWithRetries(ctx, func() error {
		object, err = objectCall.Do()
		return err
	}, s)

	if err == nil {
		meta := &content.Meta{}
		if _, ok := option.Assign(options, &meta); ok {
			meta.Values = make(map[string]string)
			if len(object.Metadata) > 0 {
				for k, v := range object.Metadata {
					meta.Values[k] = v
				}
			}
		}
		generation := &option.Generation{}
		if _, ok := option.Assign(options, &generation); ok {
			generation.Generation = object.Generation
		}
	}

	return object, err
}

//Get returns an object for supplied location
func (s *storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	info, err := s.get(ctx, location, options)
	if err == nil {
		return info, err
	}
	if isNotFound(err) {
		objectOpt := &option.ObjectKind{}
		if _, ok := option.Assign(options, &objectOpt); ok && objectOpt.File {
			return nil, err
		}
	}
	options = append(options, option.NewPage(0, 1))
	objects, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, errors.Errorf("%v %v", location, notFound)
	}
	return objects[0], err
}

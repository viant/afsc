package gs

import (
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"path"
	"strings"
)

//Delete removes an resource
func (s *storager) Delete(ctx context.Context, location string, options ...storage.Option) (err error) {
	location = strings.Trim(location, "/")
	if location == "" {
		call := s.Buckets.Delete(s.bucket)
		call.Context(ctx)
		err = call.Do()
		return err
	}
	call := s.Objects.Delete(s.bucket, location)
	call.Context(ctx)
	s.setGeneration(func(generation int64) {
		call.IfGenerationMatch(generation)
	}, func(generation int64) {
		call.IfGenerationNotMatch(generation)
	}, options)

	call.Context(ctx)
	err = runWithRetries(ctx, func() error {
		return call.Do()
	}, s)

	if isNotFound(err) {
		objectKind := &option.ObjectKind{}
		if _, ok := option.Assign(options, &objectKind); ok && objectKind.File {
			return err
		}
		notFound := err
		infoList, err := s.List(ctx, location)
		if err != nil {
			return err
		}
		if len(infoList) > 1 {
			for i := 1; i < len(infoList); i++ {
				if err = s.Delete(ctx, path.Join(location, infoList[i].Name())); err != nil {
					return err
				}
			}
			return nil
		} else {
			return notFound
		}
	}
	return err
}

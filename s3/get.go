package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"os"
	"path"
	"strings"
	"time"
)

//Get returns an object for supplied location
func (s *storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	location = strings.Trim(location, "/")
	_, name := path.Split(location)

	object, _ := s.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &location,
	})

	if object != nil && object.ContentLength != nil {
		contentLength := int64(0)
		modifiled := time.Now()
		if object.LastModified != nil {
			modifiled = *object.LastModified
		}
		if object.ContentLength != nil {
			contentLength = *object.ContentLength
		}
		if object.Body != nil {
			_ = object.Body.Close()
		}
		return file.NewInfo(name, contentLength, file.DefaultFileOsMode, modifiled, false, object), nil
	}

	options = append(options, option.NewPage(0, 1))
	objects, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, errors.Errorf("%v %v", location, doesNotExistsMessage)
	}
	return objects[0], err
}

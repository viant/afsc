package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"path"
)

//Delete removes an resource
func (s *storager) Delete(ctx context.Context, location string, options ...storage.Option) error {
	_, err := s.S3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &location,
	})
	if isNotFound(err) {
		objectKind := &option.ObjectKind{}
		if _, ok := option.Assign(options, &objectKind); ok && objectKind.File {
			return err
		}
	}
	infoList, err := s.List(ctx, location)
	if err != nil {
		return err
	}
	for i := 1; i < len(infoList); i++ {
		if err = s.Delete(ctx, path.Join(location, infoList[i].Name())); err != nil {
			return err
		}
	}
	return err
}

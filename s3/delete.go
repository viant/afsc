package s3

import (
	"context"
	"path"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

// Delete removes an resource
func (s *Storager) Delete(ctx context.Context, location string, options ...storage.Option) error {

	// In SDK v2, DeleteObject does not handle leading slashes, so remove them here
	deleteLocation := location
	if len(deleteLocation) > 0 && deleteLocation[0] == '/' {
		deleteLocation = deleteLocation[1:]
	}
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &deleteLocation,
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

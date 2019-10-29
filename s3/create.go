package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/storage"
	"io"
	"os"
	"strings"
)

//Create creates a resource
func (s *storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	if !isDir {
		return s.Upload(ctx, destination, mode, reader, options...)
	}
	if destination == "" {
		_, err := s.List(ctx, "", options...)
		if isBucketNotFound(err) {
			if err = s.createBucket(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *storager) createBucket(ctx context.Context) error {
	_, err := s.S3.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: &s.bucket,
	})
	if err != nil {
		err = errors.Wrapf(err, "failed to create bucket: `%v`", s.bucket)
	}
	return err
}

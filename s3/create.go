package s3

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/storage"
)

// Create creates a resource
func (s *Storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
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

func (s *Storager) createBucket(ctx context.Context) error {
	_, err := s.Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &s.bucket,
	})
	if err != nil {
		err = errors.Wrapf(err, "failed to create bucket: `%v`", s.bucket)
	}
	return err
}

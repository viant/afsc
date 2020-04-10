package gs

import (
	"context"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
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
		if _, err := s.List(ctx, ""); err != nil {
			if isBucketNotFound(err) {
				if createErr := s.createBucket(ctx); createErr != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *storager) createBucket(ctx context.Context) error {

	bucket := &gstorage.Bucket{
		Name:     s.bucket,
		Location: s.client.region,
	}

	call := s.Buckets.Insert(s.client.projectID, bucket)

	call.Context(ctx)
	_, err := call.Do()
	return err
}

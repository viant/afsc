package gs

import (
	"context"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
	"os"
	"strings"
)

//Create creates a resource
func (s *storager) Create(ctx context.Context, destination string, mode os.FileMode, content []byte, isDir bool, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	if !isDir {
		return s.Upload(ctx, destination, mode, content, options...)
	}
	if destination == "" {
		if _, err := s.List(ctx, ""); err != nil {
			if isBucketNotFound(err) {
				if err = s.createBucket(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *storager) createBucket(ctx context.Context) error {
	bucket := &gstorage.Bucket{
		Name: s.bucket,
	}
	call := s.Buckets.Insert(s.client.projectID, bucket)
	call.Context(ctx)
	_, err := call.Do()
	return err
}

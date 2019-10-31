package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/storage"
	"path"
	"strings"
)

func (s *storager) Copy(ctx context.Context, sourcePath, destBucket, destPath string, options ...storage.Option) error {

	sourcePath = strings.Trim(sourcePath, "/")
	destPath = strings.Trim(destPath, "/")
	infoList, err := s.List(ctx, sourcePath, options...)
	if err != nil {
		return errors.Wrapf(err, "unable list copy source: gs://%v/%v", s.bucket, sourcePath)
	}
	if len(infoList) == 0 {
		return fmt.Errorf("%v: not found", sourcePath)
	}
	for i := 1; i < len(infoList); i++ {
		name := infoList[i].Name()
		if err = s.Copy(ctx, path.Join(sourcePath, name), destBucket, path.Join(destPath, name), options...); err != nil {
			return err
		}
	}
	if infoList[0].IsDir() {
		return nil
	}

	_, err = s.S3.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		CopySource: aws.String(s.bucket + "/" + sourcePath),
		Key:        &destPath,
		Bucket:     &destBucket,
	})
	if err != nil {
		err = errors.Wrapf(err, "failed to copy: s3://%v/%v to s3://%v/%v", s.bucket, sourcePath, destBucket, destPath)
	}
	return err
}

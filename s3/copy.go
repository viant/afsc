package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/viant/afs/storage"
	"path"
	"strings"
)

func (s *storager) Copy(ctx context.Context, sourcePath, destBucket, destPath string, options ...storage.Option) error {
	sourcePath = strings.Trim(sourcePath, "/")
	destPath = strings.Trim(destPath, "/")
	infoList, err := s.List(ctx, sourcePath)
	if err != nil {
		return err
	}
	if len(infoList) == 0 {
		return fmt.Errorf("%v: not found", sourcePath)
	}
	for i := 1; i < len(infoList); i++ {
		name := infoList[i].Name()
		if err = s.Copy(ctx, path.Join(sourcePath, name), destBucket, path.Join(destPath, name)); err != nil {
			return err
		}
	}
	_, err = s.S3.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		CopySource: aws.String(s.bucket + "/" + sourcePath),
		Key:        &destPath,
		Bucket:     &destBucket,
	})
	return err
}

package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"path"
)

//Delete removes an resource
func (s *storager) Delete(ctx context.Context, location string) error {
	infoList, err := s.List(ctx, location)
	if err != nil {
		return err
	}
	for i := 1; i < len(infoList); i++ {
		if err = s.Delete(ctx, path.Join(location, infoList[i].Name())); err != nil {
			return err
		}
	}
	_, err = s.S3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &location,
		BypassGovernanceRetention: aws.Bool(true),
	})
	return err
}

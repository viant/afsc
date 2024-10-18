package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

func (s *Storager) presign(ctx context.Context, destination string, options []storage.Option) error {
	preSign := &option.PreSign{}
	if _, ok := option.Assign(options, &preSign); ok {
		_, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(destination),
		}, func(o *s3.PresignOptions) {
			o.Expires = 15 * time.Minute
		})
		if err != nil {
			return errors.Wrapf(err, "failed to presign url, s3://%v/%v", s.bucket, destination)
		}
	}
	return nil
}

package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"time"
)

func (s *storager) presign(ctx context.Context, destination string, options []storage.Option) error {
	preSign := &option.PreSign{}
	var err error
	if _, ok := option.Assign(options, &preSign); ok {
		request, _ := s.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(destination),
		})
		preSign.URL, preSign.Header, err = request.PresignRequest(15 * time.Minute)
		if err != nil {
			return errors.Wrapf(err, "failed to presign url, s3://%v/%v", s.bucket, destination)
		}
	}
	return err
}

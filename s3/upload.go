package s3

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"

	"os"
	"strings"
)

//Upload uploads content
func (s *storager) Upload(ctx context.Context, destination string, mode os.FileMode, content []byte, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	err := s.upload(ctx, destination, mode, content, options)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "NoSuchBucket") {
		if err = s.createBucket(ctx); err == nil {
			return s.upload(ctx, destination, mode, content, options)
		}
	}
	return err
}

func (s *storager) updateChecksum(input *s3.PutObjectInput, md5Hash *option.Md5, data []byte) {
	if len(md5Hash.Hash) == 0 {
		md5Hash = option.NewMd5(data)
	}
	input.ContentMD5 = aws.String(md5Hash.Encode())
}

func (s *storager) upload(ctx context.Context, destination string, mode os.FileMode, content []byte, options []storage.Option) error {
	md5Hash := &option.Md5{}
	_, _ = option.Assign(options, &md5Hash)
	input := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      aws.String(destination),
		Body:     bytes.NewReader(content),
		Metadata: map[string]*string{},
	}
	s.updateChecksum(input, md5Hash, content)
	input.Metadata[contentMD5MetaKey] = input.ContentMD5
	_, err := s.PutObjectWithContext(ctx, input)
	return err
}

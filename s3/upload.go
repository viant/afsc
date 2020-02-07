package s3

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

//Upload uploads content
func (s *storager) Upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	err := s.upload(ctx, destination, mode, reader, options)
	if err != nil {
		return err
	}
	return s.presign(ctx, destination, options)
}

func (s *storager) updateChecksum(input *s3.PutObjectInput, md5Hash *option.Md5, data []byte) {
	if len(md5Hash.Hash) == 0 {
		md5Hash = option.NewMd5(data)
	}
	input.ContentMD5 = aws.String(md5Hash.Encode())
}

func (s *storager) upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options []storage.Option) error {

	md5Hash := &option.Md5{}
	key := &option.AES256Key{}
	checksum := &option.SkipChecksum{}
	option.Assign(options, &md5Hash, &key, &checksum)

	if !checksum.Skip {
		input := &s3.PutObjectInput{
			Bucket:   &s.bucket,
			Key:      aws.String(destination),
			Metadata: map[string]*string{},
		}
		content, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		s.updateChecksum(input, md5Hash, content)
		input.Metadata[contentMD5MetaKey] = input.ContentMD5
		input.Body = bytes.NewReader(content)

		if len(key.Key) > 0 {
			input.SetSSECustomerKey(string(key.Key))
			input.SetSSECustomerKeyMD5(key.Base64KeyMd5Hash)
			input.SetSSECustomerAlgorithm(customEncryptionAlgorithm)
		}

		_, err = s.PutObjectWithContext(ctx, input)
		if err != nil {
			if strings.Contains(err.Error(), noSuchBucketMessage) {
				if err = s.createBucket(ctx); err != nil {
					return err
				}
				input.Body = bytes.NewReader(content)
				_, err = s.PutObjectWithContext(ctx, input)
			}
		}
		if err != nil {
			err = errors.Wrapf(err, "failed to upload: s3://%v/%v", s.bucket, destination)
		}
		return err
	}
	var sess *session.Session
	if s.config == nil {
		sess = session.New()
	} else {
		sess = session.New(s.config)
	}
	uploader := s3manager.NewUploader(sess)
	input := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(destination),
		Body:   reader,
	}
	_, err := uploader.Upload(input)
	if err != nil {
		return err
	}

	sizer, ok := reader.(storage.Sizer)
	if !ok {
		return nil
	}
	if objects, err := s.List(ctx, destination); err == nil && len(objects) == 1 {
		if objects[0].Size() != sizer.Size() {
			err = errors.Errorf("corrupted upload: s3://%v/%v expected size: %v, but had: %v", s.bucket, destination, sizer.Size(), objects[0].Size())
		}
	}
	return err
}

package s3

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"

	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
)

//Download return content reader and hash values if md5 or crc option is supplied or error
func (s *storager) Download(ctx context.Context, location string, options ...storage.Option) (io.ReadCloser, error) {
	var sess *session.Session
	if s.config == nil {
		sess = session.New()
	} else {
		sess = session.New(s.config)
	}
	key := &AES256Key{}
	_, _ = option.Assign(options, &key)
	downloader := s3manager.NewDownloader(sess)
	writer := NewWriter()

	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &location,
	}
	if len(key.Key) > 0 {
		input.SetSSECustomerAlgorithm(customEncryptionAlgorithm)
		input.SetSSECustomerKey(string(key.Key))
		input.SetSSECustomerKeyMD5(key.Base64KeyHash)
	}

	_, err := downloader.DownloadWithContext(ctx, writer, input)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download: %v", location)
	}
	return ioutil.NopCloser(bytes.NewReader(writer.Buffer)), nil
}

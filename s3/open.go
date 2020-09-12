package s3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"github.com/viant/afs/option"
	"strings"

	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
)

//Open return content reader and hash values if md5 or crc option is supplied or error
func (s *storager) Open(ctx context.Context, location string, options ...storage.Option) (io.ReadCloser, error) {
	var sess *session.Session
	if s.config == nil {
		sess = session.New()
	} else {
		sess = session.New(s.config)
	}
	var err error
	stream := &option.Stream{}
	key := &option.AES256Key{}
	option.Assign(options, &key, &stream)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &location,
	}

	if len(key.Key) > 0 {
		input.SetSSECustomerAlgorithm(customEncryptionAlgorithm)
		input.SetSSECustomerKey(string(key.Key))
		input.SetSSECustomerKeyMD5(key.Base64KeyMd5Hash)
	}

	downloader := s3manager.NewDownloader(sess)
	if stream.PartSize > 0 {
		objects, err := s.List(ctx, location, key)
		if err != nil {
			return nil, err
		}
		if len(objects) == 0 {
			return nil, fmt.Errorf("s3://%v/%v no found", s.bucket, location)
		}
		downloader.PartSize = int64(stream.PartSize)
		stream.Size = int(objects[0].Size())
		readSeeker := NewReadSeeker(ctx, input, downloader, stream.PartSize, stream.Size)
		reader := base.NewStreamReader(stream, readSeeker)
		return reader, nil
	}

	writer := NewWriter(32 * 1024)
	location = strings.Trim(location, "/")
	_, err = downloader.DownloadWithContext(ctx, writer, input)
	data := writer.Bytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download: s3://%v/%v", s.bucket, location)
	}
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

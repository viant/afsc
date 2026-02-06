package s3

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"github.com/viant/afs/option"

	"github.com/viant/afs/storage"
)

// Open return content reader and hash values if md5 or crc option is supplied or error
func (s *Storager) Open(ctx context.Context, location string, options ...storage.Option) (io.ReadCloser, error) {

	// In SDK v2, multiple functions do not handle leading slashes, so remove them here
	parsedLocation := location
	if len(parsedLocation) > 0 && parsedLocation[0] == '/' {
		parsedLocation = parsedLocation[1:]
	}

	started := time.Now()
	defer func() {
		s.logF("s3:Open %v %s\n", location, time.Since(started))
	}()

	stream := &option.Stream{}
	key := &option.AES256Key{}
	option.Assign(options, &key, &stream)

	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &parsedLocation,
	}

	if len(key.Key) > 0 {
		stringKey := string(key.Key)
		algorithm := customEncryptionAlgorithm
		input.SSECustomerAlgorithm = &algorithm
		input.SSECustomerKey = &stringKey
		input.SSECustomerKeyMD5 = &key.Base64KeyMd5Hash
	}

	objects, err := s.List(ctx, parsedLocation, key)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, fmt.Errorf("s3://%v/%v no found", s.bucket, parsedLocation)
	}

	client := s3.NewFromConfig(*s.config)

	if len(key.Key) > 0 {
		stringKey := string(key.Key)
		algorithm := customEncryptionAlgorithm
		input.SSECustomerAlgorithm = &algorithm
		input.SSECustomerKey = &stringKey
		input.SSECustomerKeyMD5 = &key.Base64KeyMd5Hash
	}

	if stream.PartSize > 0 {
		stream.Size = int(objects[0].Size())
		readSeeker := NewReadSeeker(ctx, input, client, stream.PartSize, stream.Size)
		streamReader := base.NewStreamReader(stream, readSeeker)
		return streamReader, nil
	}

	output, getErr := client.GetObject(ctx, input)
	if getErr != nil {
		return nil, errors.Wrapf(getErr, "failed to get object: s3://%v/%v", s.bucket, parsedLocation)
	}
	return output.Body, nil
}

package gs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"github.com/viant/afs/http"
	nhttp "net/http"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"io"
	"strings"
)

func (s *storager) Download(ctx context.Context, location string, options ...storage.Option) (reader io.ReadCloser, err error) {
	for i := 0; i < maxRetries; i++ {
		reader, err = s.download(ctx, location, options)
		if !isRetryError(err) {
			return reader, err
		}
		sleepBeforeRetry()
	}
	return reader, err
}

//Download return content reader and hash values if md5 or crc option is supplied or error
func (s *storager) download(ctx context.Context, location string, options []storage.Option) (io.ReadCloser, error) {
	location = strings.Trim(location, "/")
	call := s.Objects.Get(s.bucket, location)
	call.Context(ctx)
	crc := &option.Crc{}
	md5 := &option.Md5{}
	key := &option.AES256Key{}
	stream := &option.Stream{}
	option.Assign(options, &md5, &crc, &key, &stream)
	if len(key.Key) != 0 {
		if err := SetCustomKeyHeader(key, call.Header()); err != nil {
			return nil, err
		}
	}
	object, err := call.Do()
	if err == nil {
		if err = md5.Decode(object.Md5Hash); err == nil {
			err = crc.Decode(object.Crc32c)
		}
	}
	if err != nil {
		return nil, err
	}

	if len(key.Key) != 0 {
		if err := SetCustomKeyHeader(key, call.Header()); err != nil {
			return nil, err
		}
	}
	if stream.PartSize > 0 {
		stream.Size = int(object.Size)
		readSeeker := NewReadSeeker(call, int(object.Size))
		reader := base.NewStreamReader(stream, readSeeker)
		return reader, nil
	}

	var response *nhttp.Response
	for i := 0; i < maxRetries; i++ {
		response, err = call.Download()
		if err == nil {
			break
		}
		if !isRetryError(err) {
			return nil, errors.Wrapf(err, "failed to download gs://%v/%v ", s.bucket, location)
		}
		sleepBeforeRetry()
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to download gs://%v/%v ", s.bucket, location)
	}

	if !http.IsStatusOK(response) {
		return nil, fmt.Errorf("invalid status code: %v", response.StatusCode)
	}
	return response.Body, nil
}

package gs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/http"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"io"
	"strings"
)

//Download return content reader and hash values if md5 or crc option is supplied or error
func (s *storager) Download(ctx context.Context, location string, options ...storage.Option) (io.ReadCloser, error) {
	location = strings.Trim(location, "/")
	call := s.Objects.Get(s.bucket, location)
	call.Context(ctx)
	crc := &option.Crc{}
	md5 := &option.Md5{}
	key := &AES256Key{}
	post, _ := option.Assign(options, &md5, &crc, &key)
	if len(key.Key) != 0 {
		if err := key.SetHeader(call.Header()); err != nil {
			return nil, err
		}
	}
	if len(post) != len(options) {
		object, err := call.Do()
		if err == nil {
			if err = md5.Decode(object.Md5Hash); err == nil {
				err = crc.Decode(object.Crc32c)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	if len(key.Key) != 0 {
		if err := key.SetHeader(call.Header()); err != nil {
			return nil, err
		}
	}
	response, err := call.Download()
	if err != nil {
		return nil, errors.Wrap(err, "failed to download "+location)
	}
	if !http.IsStatusOK(response) {
		return nil, fmt.Errorf("invalid status code: %v", response.StatusCode)
	}
	return response.Body, nil
}

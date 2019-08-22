package gs

import (
	"bytes"
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"google.golang.org/api/googleapi"
	gstorage "google.golang.org/api/storage/v1"
	"net/http"
	"os"
	"strings"
)

func (s *storager) updateChecksum(object *gstorage.Object, crcHash *option.Crc, md5Hash *option.Md5, content []byte) {
	if crcHash.Hash > 0 {
		return
	}
	if len(md5Hash.Hash) == 0 {
		md5Hash.Hash = option.NewMd5(content).Hash
	}
}

//Upload uploads content
func (s *storager) Upload(ctx context.Context, destination string, mode os.FileMode, content []byte, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	object := &gstorage.Object{
		Bucket: s.bucket,
		Name:   destination,
	}

	crcHash := &option.Crc{}
	md5Hash := &option.Md5{}
	key := &CustomKey{}
	_, _ = option.Assign(options, &md5Hash, &crcHash, &key)

	s.updateChecksum(object, crcHash, md5Hash, content)
	call := s.Objects.Insert(s.bucket, object)

	call.Context(ctx)

	if len(key.Key) > 0 {
		if err := key.SetHeader(call.Header()); err != nil {
			return err
		}
	}
	call.Media(bytes.NewReader(content))
	object, err := call.Do()
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			if err = s.createBucket(ctx); err != nil {
				return err
			}
			object, err = call.Do()
		}
	}

	return err
}

package gs

import (
	"bytes"
	"context"
	"crypto/md5"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"google.golang.org/api/googleapi"
	gstorage "google.golang.org/api/storage/v1"
	"hash/crc32"
	"net/http"
	"os"
	"strings"
)

func (s *storager) updateChecksum(object *gstorage.Object, crcHash *option.Crc, md5Hash *option.Md5, content []byte) {
	if len(md5Hash.Hash) == 0 {
		hash := md5.New()
		_, _ = hash.Write(content)
		md5Hash.Hash = hash.Sum(nil)
	}
	if crcHash.Hash == 0 {
		crc32Hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		_, _ = crc32Hash.Write(content)
		crcHash.Hash = crc32Hash.Sum32()
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
	_, _ = option.Assign(options, &md5Hash, &crcHash)
	s.updateChecksum(object, crcHash, md5Hash, content)
	call := s.Objects.Insert(s.bucket, object)
	call.Context(ctx)
	call.Media(bytes.NewReader(content))
	_, err := call.Do()
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			if err = s.createBucket(ctx); err != nil {
				return err
			}
			_, err = call.Do()
		}
	}
	return err
}

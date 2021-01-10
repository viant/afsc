package gs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/object"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
	"google.golang.org/api/googleapi"
	gstorage "google.golang.org/api/storage/v1"
	"io"
	"io/ioutil"
	"os"
	"path"
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
func (s *storager) Upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options ...storage.Option) (err error) {
	return s.upload(ctx, destination, mode, reader, options)
}

//Upload uploads content
func (s *storager) upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options []storage.Option) error {
	destination = strings.Trim(destination, "/")
	gobject := &gstorage.Object{
		Bucket: s.bucket,
		Name:   destination,
	}
	var newObject *storage.Object
	checksum := &option.SkipChecksum{}
	crcHash := &option.Crc{}
	md5Hash := &option.Md5{}
	key := &option.AES256Key{}
	meta := &content.Meta{}
	option.Assign(options, &md5Hash, &crcHash, &key, &checksum, &newObject)
	var err error
	var content []byte
	if !checksum.Skip {
		content, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		s.updateChecksum(gobject, crcHash, md5Hash, content)
		reader = bytes.NewReader(content)
	}
	call := s.Objects.Insert(s.bucket, gobject)
	call.Context(ctx)
	if len(key.Key) > 0 {
		if err := SetCustomKeyHeader(key, call.Header()); err != nil {
			return err
		}
	}
	s.setGeneration(func(generation int64) {
		call.IfGenerationMatch(generation)
	}, func(generation int64) {
		call.IfGenerationNotMatch(generation)
	}, options)

	updateMetaContent(meta, gobject)

	if readerAt, ok := reader.(io.ReaderAt); ok {
		sizer := reader.(storage.Sizer)
		call = call.ResumableMedia(ctx, readerAt, sizer.Size(), detectContentType(destination))
	} else {
		mediaOpts := []googleapi.MediaOption{
			googleapi.ChunkSize(DefaultUploadChunkSize),
			googleapi.ContentType(gobject.ContentType),
		}
		call.Media(reader, mediaOpts...).Projection("full")
	}
	gobject, err = s.uploadWithRetires(ctx, call, content)
	if isBucketNotFound(err) {
		if createErr := s.createBucket(ctx); createErr != nil {
			return err
		}
		gobject, err = call.Do()
	}

	if err != nil {
		err = errors.Wrapf(err, "failed to upload: gs://%v/%v", s.bucket, destination)
		return err
	}
	sizer, ok := reader.(storage.Sizer)
	if !ok {
		return nil
	}
	if newObject != nil {
		info, _ := newFileInfo(gobject)
		*newObject = object.New(fmt.Sprintf("%v://%v/%v", Scheme, s.bucket, destination), info, gobject)
	}
	if int64(gobject.Size) != sizer.Size() {
		err = errors.Errorf("corrupted upload: gs://%v/%v expected size: %v, but had: %v", s.bucket, destination, sizer.Size(), gobject.Size)
	}
	return err
}

func updateMetaContent(meta *content.Meta, gobject *gstorage.Object) {
	if len(meta.Values) > 0 {
		for k := range meta.Values {
			value := meta.Values[k]
			switch k {
			case content.Type:
				gobject.ContentType = value
				continue
			case content.Encoding:
				gobject.ContentEncoding = value
				continue
			case content.Language:
				gobject.ContentLanguage = value
				continue
			}
			gobject.Metadata[k] = value
		}
	}
	if gobject.ContentType == "" {
		gobject.ContentType = detectContentType(gobject.Name)
	}
}

func (s *storager) uploadWithRetires(ctx context.Context, call *gstorage.ObjectsInsertCall, data []byte) (object *gstorage.Object, err error) {
	if len(data) == 0 { //no data - thus no retries once reader is exhausted
		return call.Do()
	}
	err = runWithRetries(ctx, func() error {
		object, err = call.Do()
		if err != nil {
			call.Media(bytes.NewReader(data))
		}
		return err
	}, s)
	return object, err
}

var textContentTypes = map[string]bool{
	"json": true,
	"txt":  true,
	"csv":  true,
	"text": true,
	"tsv":  true,
	"yaml": true,
	"yml":  true,
	"html": true,
	"htm":  true,
	"css":  true,
}

func detectContentType(location string) string {
	ext := path.Ext(location)
	if textContentTypes[strings.ToLower(ext)] {
		return "text/" + strings.ToLower(ext)
	}
	return "application/octet-stream"
}

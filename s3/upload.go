package s3

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
)

// Upload uploads content
func (s *Storager) Upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	destination = strings.Trim(destination, "/")
	err := s.upload(ctx, destination, mode, reader, options)
	if err != nil {
		return err
	}
	return s.presign(ctx, destination, options)
}

func (s *Storager) upload(ctx context.Context, destination string, _ os.FileMode, reader io.Reader, options []storage.Option) error {
	meta := &content.Meta{}
	serverSideEncryption := &option.ServerSideEncryption{}
	stream := &option.Stream{}
	grant := &option.Grant{}
	acl := &option.ACL{}
	option.Assign(options, &meta, &serverSideEncryption, &stream, &grant, &acl)

	input := &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(destination),
		Body:     reader,
		Metadata: map[string]string{},
	}
	if grant.FullControl != "" {
		input.GrantFullControl = &grant.FullControl
		input.GrantRead = &grant.Read
		input.GrantReadACP = &grant.ReadACP
		input.GrantWriteACP = &grant.WriteACP
	}
	if acl.ACL != "" {
		input.ACL = types.ObjectCannedACL(acl.ACL)
	}
	if serverSideEncryption.Algorithm != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(serverSideEncryption.Algorithm)
	}

	if len(meta.Values) > 0 {
		for k := range meta.Values {
			value := meta.Values[k]
			switch k {
			case content.Type:
				input.ContentType = &value
				continue
			case content.Encoding:
				input.ContentEncoding = &value
				continue
			case content.Language:
				input.ContentLanguage = &value
				continue
			}
			input.Metadata[k] = value
		}
	}

	client := s3.NewFromConfig(*s.config)
	_, err := client.PutObject(context.Background(), input)
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

package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"os"
	"path"
	"strings"
	"time"
)

//List list directory or returns a file info
func (s *storager) List(ctx context.Context, location string, options ...storage.Option) ([]os.FileInfo, error) {
	location = strings.Trim(location, "/")
	matcher, page := option.GetListOptions(options)
	var result = make([]os.FileInfo, 0)
	if location == "" {
		info := file.NewInfo("/", int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
		if matcher("", info) {
			result = append(result, info)
		}
	}

	err := s.list(ctx, location, &result, page, matcher)
	return result, err
}

func (s *storager) addFolders(parent string, result *[]os.FileInfo, prefixes []*s3.CommonPrefix, page *option.Page, matcher option.Match) {
	for i := range prefixes {
		folder := strings.Trim(*prefixes[i].Prefix, "/")
		_, name := path.Split(folder)
		info := file.NewInfo(name, int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
		page.Increment()
		if page.ShallSkip() {
			continue
		}
		if !matcher(parent, info) {
			continue
		}
		*result = append(*result, info)

		if page.HasReachedLimit() {
			return
		}
	}

}

func (s *storager) addFiles(parent string, result *[]os.FileInfo, objects []*s3.Object, page *option.Page, matcher option.Match) {
	for i := range objects {
		_, name := path.Split(*objects[i].Key)
		if name == "" {
			continue
		}
		info := file.NewInfo(name, *objects[i].Size, file.DefaultFileOsMode, *objects[i].LastModified, false, objects[i])
		page.Increment()
		if page.ShallSkip() {
			continue
		}
		if !matcher(parent, info) {
			continue
		}
		*result = append(*result, info)
		if page.HasReachedLimit() {
			return
		}
	}
}

func (s *storager) list(ctx context.Context, parent string, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
	started := time.Now()
	defer func() {
		fmt.Printf("s3:List %v %s\n", parent, time.Since(started))
	}()

	input := &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(parent),
		Delimiter: aws.String("/"),
	}
	var folders int
	var files int
	err := s.ListObjectsPagesWithContext(ctx, input, func(output *s3.ListObjectsOutput, lastPage bool) bool {
		s.addFolders(parent, result, output.CommonPrefixes, page, matcher)
		assets := exactMatched(output, input)
		s.addFiles(parent, result, assets, page, matcher)
		folders = len(output.CommonPrefixes)
		files = len(output.Contents)
		return (!page.HasReachedLimit()) && !lastPage
	})

	if files == 1 && folders == 0 {
		return nil
	}
	if err != nil {
		if err == credentials.ErrNoValidProvidersFoundInChain {
			s.initS3Client()
		}
		err = errors.Wrapf(err, "failed to list: s3://%v/%v", s.bucket, parent)
	}

	input = &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(parent + "/"),
		Delimiter: aws.String("/"),
	}
	err = s.ListObjectsPagesWithContext(ctx, input, func(output *s3.ListObjectsOutput, lastPage bool) bool {
		s.addFolders(parent, result, output.CommonPrefixes, page, matcher)
		s.addFiles(parent, result, output.Contents, page, matcher)
		return (!page.HasReachedLimit()) && !lastPage

	})

	return err
}

func exactMatched(output *s3.ListObjectsOutput, input *s3.ListObjectsInput) []*s3.Object {
	var assets = []*s3.Object{}
	if len(output.Contents) == 0 {
		return assets
	}
	for i, match := range output.Contents {

		if len(*match.Key) > len(*input.Prefix) {
			continue
		}
		assets = append(assets, output.Contents[i])
	}
	return assets
}

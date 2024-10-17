package s3

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

// List list directory or returns a file info
func (s *Storager) List(ctx context.Context, location string, options ...storage.Option) ([]os.FileInfo, error) {
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

func (s *Storager) addFolders(parent string, result *[]os.FileInfo, prefixes []types.CommonPrefix, page *option.Page, matcher option.Match) {
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

func (s *Storager) addFiles(parent string, result *[]os.FileInfo, objects []types.Object, page *option.Page, matcher option.Match) {
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

func (s *Storager) list(ctx context.Context, parent string, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
	started := time.Now()
	defer func() {
		s.logF("s3:List %v %s\n", parent, time.Since(started))
	}()

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(parent),
		Delimiter: aws.String("/"),
	}
	var folders int
	var files int
	paginator := s3.NewListObjectsV2Paginator(s.Client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			err = errors.Wrapf(err, "failed to list: s3://%v/%v", s.bucket, parent)
			break
		}
		s.addFolders(parent, result, output.CommonPrefixes, page, matcher)
		assets := exactMatched(output, input)
		s.addFiles(parent, result, assets, page, matcher)
		folders = len(output.CommonPrefixes)
		files = len(output.Contents)
	}

	if files == 1 && folders == 0 {
		return nil
	}

	input = &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(parent + "/"),
		Delimiter: aws.String("/"),
	}

	paginator = s3.NewListObjectsV2Paginator(s.Client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		s.addFolders(parent, result, output.CommonPrefixes, page, matcher)
		s.addFiles(parent, result, output.Contents, page, matcher)
	}

	return nil
}

func exactMatched(output *s3.ListObjectsV2Output, input *s3.ListObjectsV2Input) []types.Object {
	var assets []types.Object
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

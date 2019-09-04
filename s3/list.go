package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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
	var result = make([]os.FileInfo, 0)
	if location == "" {
		info := file.NewInfo("/", int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
		result = append(result, info)
	}
	page := &option.Page{}
	var matcher option.Matcher
	option.Assign(options, &page, &matcher)
	matcher = option.GetMatcher(matcher)
	err := s.list(ctx, location, &result, page, matcher)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 || (len(result) == 1 && result[0].IsDir()) {
		err = s.list(ctx, location+"/", &result, page, matcher)
	}
	return result, err
}

func (s *storager) addFolders(parent string, result *[]os.FileInfo, prefixes []*s3.CommonPrefix, page *option.Page, matcher option.Matcher) {
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

func (s *storager) addFiles(parent string, result *[]os.FileInfo, objects []*s3.Object, page *option.Page, matcher option.Matcher) {
	for i := range objects {
		_, name := path.Split(*objects[i].Key)
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

func (s *storager) list(ctx context.Context, parent string, result *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	input := &s3.ListObjectsInput{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(parent),
		Delimiter: aws.String("/"),
	}
	return s.ListObjectsPagesWithContext(ctx, input, func(output *s3.ListObjectsOutput, lastPage bool) bool {

		s.addFolders(parent, result, output.CommonPrefixes, page, matcher)
		s.addFiles(parent, result, output.Contents, page, matcher)
		return page.HasReachedLimit()
	})

}

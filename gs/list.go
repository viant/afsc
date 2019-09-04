package gs

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

//List list directory or returns a file info
func (s *storager) List(ctx context.Context, location string, options ...storage.Option) ([]os.FileInfo, error) {
	location = strings.Trim(location, "/")
	var result = make([]os.FileInfo, 0)
	page := &option.Page{}
	var matcher option.Matcher
	option.Assign(options, &page, &matcher)
	matcher = option.GetMatcher(matcher)
	err := s.list(ctx, location, &result, page, matcher)
	return result, err
}

//List list directory, returns a file info
func (s *storager) list(ctx context.Context, parent string, result *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	var err error
	call := s.Objects.List(s.bucket)
	if page.MaxResult() > 0 {
		call.MaxResults(page.MaxResult())
	}

	if parent == "" {
		info := file.NewInfo("/", int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
		*result = append(*result, info)
	}

	err = s.listPage(ctx, call, parent, result, page, matcher)
	if err != nil {
		return err
	}
	if len(*result) == 0 || (len(*result) == 1 && (*result)[0].IsDir()) {
		err = s.listPage(ctx, call, parent+"/", result, page, matcher)
	}
	return err
}

func (s *storager) listPage(ctx context.Context, call *gstorage.ObjectsListCall, location string, result *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	var err error
	for {
		call.Prefix(location)
		call.Delimiter("/")
		call.Context(ctx)

		if err = s.listObjects(ctx, location, call, result, page, matcher); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
	}
	return err
}

func (s *storager) addFolders(ctx context.Context, parent string, objects *gstorage.Objects, result *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	if folders := objects.Prefixes; len(folders) > 0 {
		for _, folder := range folders {
			folder = strings.Trim(folder, "/")
			_, name := path.Split(folder)
			info := file.NewInfo(name, int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
			if !matcher(parent, info) {
				continue
			}
			page.Increment()
			if page.ShallSkip() {
				continue
			}
			*result = append(*result, info)
			if page.HasReachedLimit() {
				return io.EOF
			}
		}
	}
	return nil
}

func (s *storager) addFiles(ctx context.Context, parent string, objects *gstorage.Objects, result *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	if items := objects.Items; len(items) > 0 {
		for i, object := range items {
			modified, err := time.Parse(time.RFC3339, object.Updated)
			if err != nil {
				return err
			}
			mode := file.DefaultFileOsMode
			isDir := strings.HasSuffix(object.Name, "/")
			if isDir {
				mode = file.DefaultDirOsMode
				object.Name = string(object.Name[:len(object.Name)-1])
			}
			_, name := path.Split(object.Name)
			info := file.NewInfo(name, int64(object.Size), mode, modified, isDir, items[i])
			if !matcher(parent, info) {
				continue
			}
			page.Increment()
			if page.ShallSkip() {
				continue
			}
			*result = append(*result, info)
			if page.HasReachedLimit() {
				return io.EOF
			}
		}
	}
	return nil
}

func (s *storager) listObjects(ctx context.Context, location string, call *gstorage.ObjectsListCall, infoList *[]os.FileInfo, page *option.Page, matcher option.Matcher) error {
	objects, err := call.Do()
	if err != nil {
		return err
	}
	if err = s.addFolders(ctx, location, objects, infoList, page, matcher); err != nil {
		return err
	}
	if err = s.addFiles(ctx, location, objects, infoList, page, matcher); err != nil {
		return err
	}
	call.PageToken(objects.NextPageToken)
	if objects.NextPageToken == "" {
		return io.EOF
	}
	return nil
}

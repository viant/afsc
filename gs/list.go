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
	"sync/atomic"
	"time"
)

var listCounter uint64

//List list directory or returns a file info
func (s *storager) List(ctx context.Context, location string, options ...storage.Option) (files []os.FileInfo, err error) {
	return s.listFiles(ctx, location, options)
}

//List list directory or returns a file info
func (s *storager) listFiles(ctx context.Context, location string, options []storage.Option) ([]os.FileInfo, error) {
	location = strings.Trim(location, "/")
	if location != "" {
		location += "/"
	}
	var result = make([]os.FileInfo, 0)
	matcher, page := option.GetListOptions(options)
	err := s.list(ctx, location, &result, page, matcher)
	return result, err
}

//List list directory, returns a file info
func (s *storager) list(ctx context.Context, location string, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
	var err error
	call := s.Objects.List(s.bucket)
	if page.MaxResult() > 0 {
		call.MaxResults(page.MaxResult())
	}

	_, name := path.Split(strings.Trim(location, "/"))
	if name == "" {
		name = "/"
	}

	info := file.NewInfo(name, int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
	if location == "" {
		if matcher("", info) {
			*result = append(*result, info)
		}
	}
	files, folders, err := s.listPage(ctx, call, location, result, page, matcher)
	if err == nil && files == 0 && folders == 0 {
		_, _, err = s.listPage(ctx, call, strings.Trim(location, "/"), result, page, matcher)
	}
	if len(*result) > 0 {
		if (*result)[0].Name() != info.Name() {
			*result = append([]os.FileInfo{info}, *result...)
		}
	}
	return err
}

func (s *storager) listPage(ctx context.Context, call *gstorage.ObjectsListCall, location string, result *[]os.FileInfo, page *option.Page, matcher option.Match) (files, folders int, err error) {
	for {
		call.Prefix(location)
		call.Delimiter("/")
		call.Context(ctx)
		if files, folders, err = s.listObjects(ctx, location, call, result, page, matcher); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
	}
	return files, folders, err
}

func (s *storager) addFolders(ctx context.Context, parent string, objects *gstorage.Objects, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
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

func newFileInfo(object *gstorage.Object) (os.FileInfo, error) {
	modified, err := time.Parse(time.RFC3339, object.Updated)
	if err != nil {
		return nil, err
	}
	mode := file.DefaultFileOsMode
	isDir := strings.HasSuffix(object.Name, "/")
	if isDir {
		mode = file.DefaultDirOsMode
		object.Name = string(object.Name[:len(object.Name)-1])
	}
	_, name := path.Split(object.Name)
	info := file.NewInfo(name, int64(object.Size), mode, modified, isDir, object)
	return info, nil
}

func (s *storager) addFile(parent string, info os.FileInfo, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
	if !matcher(parent, info) {
		return nil
	}
	page.Increment()
	if page.ShallSkip() {
		return nil
	}
	*result = append(*result, info)
	if page.HasReachedLimit() {
		return io.EOF
	}
	return nil
}

func (s *storager) addFiles(ctx context.Context, parent string, objects *gstorage.Objects, result *[]os.FileInfo, page *option.Page, matcher option.Match) error {
	if items := objects.Items; len(items) > 0 {
		for i := range items {
			info, err := newFileInfo(items[i])
			if err != nil {
				return err
			}
			if err = s.addFile(parent, info, result, page, matcher); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *storager) listObjects(ctx context.Context, location string, call *gstorage.ObjectsListCall, infoList *[]os.FileInfo, page *option.Page, matcher option.Match) (int, int, error) {
	atomic.AddUint64(&listCounter, 1)

	var objects *gstorage.Objects
	var err error
	err = runWithRetries(ctx, func() error {
		objects, err = call.Do()
		return err
	}, s)
	if err != nil {
		return 0, 0, err
	}
	if err = s.addFolders(ctx, location, objects, infoList, page, matcher); err != nil {
		return 0, 0, err
	}
	if err = s.addFiles(ctx, location, objects, infoList, page, matcher); err != nil {
		return 0, 0, err
	}
	call.PageToken(objects.NextPageToken)
	files := len(objects.Items)
	folders := len(objects.Prefixes)
	if objects.NextPageToken == "" {
		return files, folders, io.EOF
	}
	return files, folders, nil
}

//GetListCounter returns count of list operations
func GetListCounter(reset bool) int {
	result := atomic.LoadUint64(&listCounter)
	if reset {
		atomic.StoreUint64(&listCounter, 0)
	}
	return int(result)
}

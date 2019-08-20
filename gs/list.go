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
	_, _ = option.Assign(options, &page)
	err := s.list(ctx, location, &result, page)
	return result, err
}

//List list directory, returns a file info
func (s *storager) list(ctx context.Context, location string, result *[]os.FileInfo, page *option.Page) error {
	var err error
	call := s.Objects.List(s.bucket)
	if page.MaxResult() > 0 {
		call.MaxResults(page.MaxResult())
	}

	if location == "" {
		info := file.NewInfo("/", int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
		*result = append(*result, info)
	}

	err = s.listPage(ctx, call, location, result, page)
	if err != nil {
		return err
	}
	if len(*result) == 1 {
		_, locationName := path.Split(location)
		if locationName == (*result)[0].Name() {
			err = s.listPage(ctx, call, location+"/", result, page)
		}
	}
	return err
}

func (s *storager) listPage(ctx context.Context, call *gstorage.ObjectsListCall, location string, result *[]os.FileInfo, page *option.Page) error {
	var err error
	for {
		call.Prefix(location)
		call.Delimiter("/")
		call.Context(ctx)

		if err = s.listObjects(ctx, call, result, page); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
	}
	return err
}

func (s *storager) addFolders(ctx context.Context, objects *gstorage.Objects, infoList *[]os.FileInfo, page *option.Page) error {
	if folders := objects.Prefixes; len(folders) > 0 {
		for _, folder := range folders {
			page.Increment()
			if page.ShallSkip() {
				continue
			}
			folder = strings.Trim(folder, "/")

			_, name := path.Split(folder)
			info := file.NewInfo(name, int64(0), file.DefaultDirOsMode, time.Now(), true, nil)
			*infoList = append(*infoList, info)
			if page.HasReachedLimit() {
				return io.EOF
			}
		}
	}
	return nil
}

func (s *storager) addFiles(ctx context.Context, objects *gstorage.Objects, infoList *[]os.FileInfo, page *option.Page) error {
	if items := objects.Items; len(items) > 0 {
		for i, object := range items {
			page.Increment()
			if page.ShallSkip() {
				continue
			}
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
			*infoList = append(*infoList, info)
			if page.HasReachedLimit() {
				return io.EOF
			}
		}
	}
	return nil
}

func (s *storager) listObjects(ctx context.Context, call *gstorage.ObjectsListCall, infoList *[]os.FileInfo, page *option.Page) error {
	objects, err := call.Do()
	if err != nil {
		return err
	}
	if err = s.addFolders(ctx, objects, infoList, page); err != nil {
		return err
	}
	if err = s.addFiles(ctx, objects, infoList, page); err != nil {
		return err
	}
	call.PageToken(objects.NextPageToken)
	if objects.NextPageToken == "" {
		return io.EOF
	}
	return nil
}

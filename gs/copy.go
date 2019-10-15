package gs

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
	"path"
	"strings"
)

func (s *storager) Copy(ctx context.Context, sourcePath, destBucket, destPath string, options ...storage.Option) error {
	sourcePath = strings.Trim(sourcePath, "/")
	destPath = strings.Trim(destPath, "/")
	infoList, err := s.List(ctx, sourcePath)
	if err != nil {
		return err
	}
	if len(infoList) == 0 {
		return fmt.Errorf("%v: not found", sourcePath)
	}
	for i := 1; i < len(infoList); i++ {
		name := infoList[i].Name()
		if err = s.Copy(ctx, path.Join(sourcePath, name), destBucket, path.Join(destPath, name)); err != nil {
			return err
		}
	}
	if infoList[0].IsDir() {
		return nil
	}
	info, ok := infoList[0].(*file.Info)
	if !ok {
		return fmt.Errorf("unable location source,  expected: %T, but had: %v", info, infoList[0])
	}
	object, _ := info.Source.(*gstorage.Object)
	object.Name = destPath
	call := s.Objects.Copy(s.bucket, sourcePath, destBucket, destPath, object)
	call.Context(ctx)
	_, err = call.Do()
	return err
}

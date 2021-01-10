package gs

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	gstorage "google.golang.org/api/storage/v1"
	"path"
	"strings"
)

func (s *storager) Move(ctx context.Context, sourcePath, destBucket, destPath string, options ...storage.Option) error {
	return s.move(ctx, sourcePath, destBucket, destPath, options)
}

func (s *storager) move(ctx context.Context, sourcePath, destBucket, destPath string, options []storage.Option) error {
	sourcePath = strings.Trim(sourcePath, "/")
	destPath = strings.Trim(destPath, "/")
	objectInfo, err := s.get(ctx, sourcePath, options)

	if isNotFound(err) {
		objectOpt := &option.ObjectKind{}
		if _, ok := option.Assign(options, &objectOpt); ok && objectOpt.File {
			return err
		}
		infoList, err := s.List(ctx, sourcePath)
		if err != nil {
			return err
		}
		if len(infoList) == 0 {
			return fmt.Errorf("%v: not found", sourcePath)
		}
		for i := 1; i < len(infoList); i++ {
			name := infoList[i].Name()
			if err = s.Move(ctx, path.Join(sourcePath, name), destBucket, path.Join(destPath, name)); err != nil {
				return err
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	info, ok := objectInfo.(*file.Info)
	if !ok {
		return fmt.Errorf("unable move,  expected: %T, but had: %v", info, objectInfo)
	}
	object, _ := info.Source.(*gstorage.Object)
	object.Name = destPath
	call := s.Objects.Rewrite(s.bucket, sourcePath, destBucket, destPath, object)
	call.Context(ctx)
	s.setGeneration(func(generation int64) {
		call.IfGenerationMatch(generation)
	}, func(generation int64) {
		call.IfGenerationNotMatch(generation)
	}, options)
	err = runWithRetries(ctx, func() error {
		_, err = call.Do()
		return err
	}, s)
	if err == nil {
		err = s.Delete(ctx, sourcePath)
	}
	return err
}

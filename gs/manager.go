package gs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/logger"
	"google.golang.org/api/googleapi"
)

const defaultPartSize = 32 * 1024 * 1024

type manager struct {
	*base.Manager
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	return newStorager(ctx, baseURL, options...)
}

func (m *manager) copyInMemory(ctx context.Context, sourceURL, destURL string, options []storage.Option) error {
	objects, err := m.List(ctx, sourceURL, options...)
	if err != nil {
		return errors.Wrapf(err, "copy source not found %v", sourceURL)
	}
	downloadOptions := append(options, option.NewStream(defaultPartSize, int(objects[0].Size())))
	reader, err := m.OpenURL(ctx, sourceURL, downloadOptions...)
	if err != nil {
		return errors.Wrapf(err, "failed open %v for copy %v", sourceURL, destURL)
	}
	defer reader.Close()
	uploadOptions := append(options, option.NewSkipChecksum(true))
	return m.Upload(ctx, destURL, file.DefaultFileOsMode, reader, uploadOptions...)
}

//Move moves data from source to dest
func (m *manager) Copy(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	gsStorager, err := m.Storager(ctx, sourceURL, options)
	if err != nil {
		return nil
	}
	rawStorager, ok := gsStorager.(*storager)
	if !ok {
		return fmt.Errorf("expected: %T, but had: %T", rawStorager, gsStorager)
	}
	sourcePath := url.Path(sourceURL)
	destBucket := url.Host(destURL)
	destPath := url.Path(destURL)
	key := &option.AES256Key{}
	_, hasKey := option.Assign(options, &key)
	if !hasKey {
		err = rawStorager.Copy(ctx, sourcePath, destBucket, destPath, options...)
	}
	if isFallbackError(err) || hasKey { //simulate move operation in process
		if err != nil {
			logger.Logf("fallback copy: %v", err)
		}
		err = m.copyInMemory(ctx, sourceURL, destURL, options)
		if err != nil {
			err = errors.Wrapf(err, "failed to copy in memory")
		}
		return err
	}
	return err
}

//Move moves data from source to dest
func (m *manager) Move(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	gsStorager, err := m.Storager(ctx, sourceURL, options)
	if err != nil {
		return nil
	}
	rawStorager, ok := gsStorager.(*storager)
	if !ok {
		return fmt.Errorf("expected: %T, but had: %T", rawStorager, gsStorager)
	}
	sourcePath := url.Path(sourceURL)
	destBucket := url.Host(destURL)
	destPath := url.Path(destURL)
	key := &option.AES256Key{}
	_, hasKey := option.Assign(options, &key)
	if !hasKey {
		err = rawStorager.Move(ctx, sourcePath, destBucket, destPath, options...)
	}
	if isFallbackError(err) || hasKey { //simulate move operation in process
		if err != nil {
			logger.Logf("fallback move: %v", err)
		}
		err = m.copyInMemory(ctx, sourceURL, destURL, options)
		if err == nil {
			err = m.Delete(ctx, sourceURL)
		}
		if err != nil {
			err = errors.Wrapf(err, "failed to move in memory")
		}
	}
	return err
}

func (m *manager) ErrorCode(err error) int {
	if err == nil {
		return 0
	}
	if googleError, ok := err.(*googleapi.Error); ok {
		return googleError.Code
	}
	origin := errors.Cause(err)
	if googleError, ok := origin.(*googleapi.Error); ok {
		return googleError.Code
	}
	return 0
}

func newManager(options ...storage.Option) *manager {
	result := &manager{}
	baseMgr := base.New(result, Scheme, result.provider, options)
	result.Manager = baseMgr
	return result
}

//New creates scp manager
func New(options ...storage.Option) storage.Manager {
	return newManager(options...)
}

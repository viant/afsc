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
)


const defaultPartSize = 32 * 1024 * 1024

type manager struct {
	*base.Manager
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	return newStorager(ctx, baseURL, options...)
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
	err = rawStorager.Move(ctx, sourcePath, destBucket, destPath, options...)
	if isFallbackError(err) {
		objects, err := m.List(ctx, sourceURL, options...)
		if err != nil {
			return errors.Wrapf(err, "move source not found %v", sourceURL)
		}
		downloadOptions := append(options, option.NewStream(defaultPartSize, int(objects[0].Size())))
		//simulate move operation in process
		reader, err := m.DownloadWithURL(ctx, sourceURL, downloadOptions...)
		if err != nil {
			return errors.Wrapf(err, "failed download %v for copy %v", sourceURL, destURL)
		}
		defer reader.Close()
		options = append(options, option.NewChecksum(true))
		if err = m.Upload(ctx, destURL, file.DefaultFileOsMode, reader, options...); err == nil {
			err = m.Delete(ctx, sourceURL, options...)
		}
		return nil
	}
	return err
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
	err = rawStorager.Copy(ctx, sourcePath, destBucket, destPath, options...)
	if isFallbackError(err) { //simulate move operation in process
		objects, err := m.List(ctx, sourceURL, options...)
		if err != nil {
			return errors.Wrapf(err, "copy source not found %v", sourceURL)
		}
		downloadOptions := append(options, option.NewStream(defaultPartSize, int(objects[0].Size())))
		reader, err := m.DownloadWithURL(ctx, sourceURL, downloadOptions...)
		if err != nil {
			return errors.Wrapf(err, "failed download %v for copy %v", sourceURL, destURL)
		}
		defer reader.Close()
		options = append(options, option.NewChecksum(true))
		return m.Upload(ctx, destURL, file.DefaultFileOsMode, reader, options...)
	}
	return err
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

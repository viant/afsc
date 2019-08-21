package gs

import (
	"context"
	"fmt"
	"github.com/viant/afs/base"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
)

type manager struct {
	*base.Manager
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	return newStorager(ctx, baseURL, options...)
}

//Move moves data from source to dest
func (s *manager) Move(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	gsStorager, err := s.Storager(ctx, sourceURL, options...)
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
	return rawStorager.Move(ctx, sourcePath, destBucket, destPath, options...)
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

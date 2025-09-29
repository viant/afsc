package openai

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"strings"

	"github.com/viant/afs/base"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"

	"github.com/viant/afsc/openai/assets"
)

const (
	assetsStorage   = "assets"
	defaultPartSize = 10 * 1024 * 1024
)

type manager struct {
	*base.Manager
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	authority := strings.ToLower(url.Host(baseURL))
	switch authority {
	case assetsStorage:
		return assets.NewStorager(ctx, baseURL, options...)
	default:
		return nil, fmt.Errorf("unsupported: %v", authority)
	}
}

func (m *manager) copyInMemory(ctx context.Context, sourceURL, destURL string, options []storage.Option) error {
	// Try to get size for stream optimization
	objects, err := m.List(ctx, sourceURL, options...)
	if err != nil {
		return errors.Wrapf(err, "copy source not found %v", sourceURL)
	}
	downloadOptions := options
	if len(objects) > 0 {
		downloadOptions = append(downloadOptions, option.NewStream(defaultPartSize, int(objects[0].Size())))
	}
	reader, err := m.OpenURL(ctx, sourceURL, downloadOptions...)
	if err != nil {
		return errors.Wrapf(err, "failed download %v for copy %v", sourceURL, destURL)
	}
	defer reader.Close()
	uploadOptions := append(options, option.NewSkipChecksum(true))
	return m.Upload(ctx, destURL, file.DefaultFileOsMode, reader, uploadOptions...)
}

// Copy copies data via memory streaming
func (m *manager) Copy(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	return m.copyInMemory(ctx, sourceURL, destURL, options)
}

// Move moves data via memory streaming
func (m *manager) Move(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	if err := m.copyInMemory(ctx, sourceURL, destURL, options); err != nil {
		return err
	}
	return m.Delete(ctx, sourceURL)
}

func newManager(options ...storage.Option) *manager {
	result := &manager{}
	baseMgr := base.New(result, Scheme, result.provider, options)
	result.Manager = baseMgr
	return result
}

// New creates OpenAI manager
func New(options ...storage.Option) storage.Manager {
	return newManager(options...)
}

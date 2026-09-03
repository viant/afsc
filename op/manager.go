package op

import (
	"context"
	"fmt"

	"github.com/viant/afs/base"
	"github.com/viant/afs/storage"
)

var errUnsupported = fmt.Errorf("unsupported operation")

type manager struct {
	*base.Manager
}

func (m *manager) Copy(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	return errUnsupported
}

func (m *manager) Move(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	return errUnsupported
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	return NewStorager(ctx, baseURL, options...)
}

func newManager(options ...storage.Option) *manager {
	result := &manager{}
	baseMgr := base.New(result, Scheme, result.provider, options)
	result.Manager = baseMgr
	return result
}

// New creates a 1Password secret manager.
func New(options ...storage.Option) storage.Manager {
	return newManager(options...)
}

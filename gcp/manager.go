package gcp

import (
	"context"
	"fmt"
	"github.com/viant/afs/base"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gcp/secretmanager"
	"strings"
)

var errUnsupported = fmt.Errorf("unsupported operation")

const (
	secretManagerStorage = "secretmanager"
)

type manager struct {
	*base.Manager
}

//Copy moves data from source to dest
func (m *manager) Copy(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	return errUnsupported
}

//Move moves data from source to dest
func (m *manager) Move(ctx context.Context, sourceURL, destURL string, options ...storage.Option) error {
	return errUnsupported
}

func (m *manager) provider(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	options = m.Options(options)
	authority := strings.ToLower(url.Host(baseURL))
	switch authority {
	case secretManagerStorage:
		return secretmanager.NewStorager(ctx, baseURL, options...)
	default:
		return nil, fmt.Errorf("unsupported: %v", authority)
	}
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

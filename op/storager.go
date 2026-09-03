package op

import (
	"context"

	"github.com/viant/afs/storage"
)

// Storager reads 1Password secrets for a vault base URL such as op://Private.
type Storager struct {
	baseURL string
	cli     CLI
}

// NewStorager creates a new 1Password storager.
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*Storager, error) {
	return &Storager{
		baseURL: baseURL,
		cli:     cliFromOptions(options),
	}, nil
}

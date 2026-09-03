package op

import (
	"bytes"
	"context"
	"io"

	"github.com/viant/afs/storage"
)

// Open returns a reader for the supplied 1Password secret reference.
func (s *Storager) Open(ctx context.Context, resourceID string, options ...storage.Option) (io.ReadCloser, error) {
	ref, err := reference(s.baseURL, resourceID)
	if err != nil {
		return nil, err
	}
	data, err := s.cli.Read(ctx, ref)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

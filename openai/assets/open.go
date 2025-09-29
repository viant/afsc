package assets

import (
	"bytes"
	"context"
	"io"
	"path"

	"github.com/viant/afs/storage"
	afsurl "github.com/viant/afs/url"
)

// Open returns content reader for asset by id (last segment of location).
func (s *Storager) Open(ctx context.Context, location string, options ...storage.Option) (io.ReadCloser, error) {
	id := path.Base(afsurl.Path(location))
	if id == "" || id == "." || id == "/" {
		// No id; return empty reader
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	data, err := s.doRaw(ctx, "GET", "/files/"+id+"/content", nil)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

package assets

import (
	"context"
	"path"

	"github.com/viant/afs/storage"
	afsurl "github.com/viant/afs/url"
)

// Exists checks if an asset with given id or filename exists.
func (s *Storager) Exists(ctx context.Context, location string, options ...storage.Option) (bool, error) {
	target := path.Base(afsurl.Path(location))
	var resp ListResponse
	if err := s.doJSON(ctx, "GET", "/files", nil, &resp); err != nil {
		return false, err
	}
	if target == "" || target == "." || target == "/" {
		return true, nil
	}
	for _, f := range resp.Data {
		if f.ID == target || f.Filename == target {
			return true, nil
		}
	}
	return false, nil
}

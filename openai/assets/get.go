package assets

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	afsurl "github.com/viant/afs/url"
)

// Get returns a file info by id or filename. If none specified, returns first asset.
func (s *Storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	target := path.Base(afsurl.Path(location))

	var resp ListResponse
	if err := s.doJSON(ctx, "GET", "/files", nil, &resp); err != nil {
		return nil, err
	}
	if target == "" || target == "." || target == "/" {
		if len(resp.Data) == 0 {
			return nil, fmt.Errorf("not found")
		}
		f := resp.Data[0]
		return file.NewInfo(f.Filename, f.Bytes, file.DefaultFileOsMode, f.ModTime(), false, f), nil
	}
	for _, f := range resp.Data {
		if f.ID == target || f.Filename == target {
			// Support presign/meta options no-op for assets
			_ = options
			return file.NewInfo(f.Filename, f.Bytes, file.DefaultFileOsMode, f.ModTime(), false, f), nil
		}
	}
	return nil, fmt.Errorf("%s does not exist", location)
}

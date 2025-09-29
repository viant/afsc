package assets

import (
	"context"
	"os"
	"path"

	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

// List lists OpenAI files. Location is optional and used only for simple prefix filtering by filename.
func (s *Storager) List(ctx context.Context, location string, options ...storage.Option) ([]os.FileInfo, error) {
	matcher, page := option.GetListOptions(options)

	var resp ListResponse
	if err := s.doJSON(ctx, "GET", "/files", nil, &resp); err != nil {
		return nil, err
	}

	var result []os.FileInfo
	parent := path.Clean(location)
	for i := range resp.Data {
		f := resp.Data[i]
		info := file.NewInfo(f.Filename, f.Bytes, file.DefaultFileOsMode, f.ModTime(), false, f)
		page.Increment()
		if page.ShallSkip() {
			continue
		}
		if !matcher(parent, info) {
			continue
		}
		result = append(result, info)
		if page.HasReachedLimit() {
			break
		}
	}
	return result, nil
}

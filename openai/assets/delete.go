package assets

import (
	"context"
	"path"

	"github.com/viant/afs/storage"
	afsurl "github.com/viant/afs/url"
)

// Delete removes an OpenAI file by id (last segment of location).
func (s *Storager) Delete(ctx context.Context, location string, _ ...storage.Option) error {
	id := path.Base(afsurl.Path(location))
	if id == "" || id == "/" || id == "." {
		return nil
	}
	var resp DeleteResponse
	return s.doJSON(ctx, "DELETE", "/files/"+id, nil, &resp)
}

package assets

import (
	"context"
	"io"
	"os"
	"path"
	"strings"

	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
)

// Upload uploads content as OpenAI file. Destination's base name is used as filename.
// Set purpose via content.Meta option, key "purpose" (defaults to "assistants").
func (s *Storager) Upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	meta := &content.Meta{}
	option.Assign(options, &meta)
	purpose := meta.Values["purpose"]
	if purpose == "" {
		purpose = "assistants"
	}
	filename := path.Base(strings.TrimSpace(destination))
	if filename == "" || filename == "." || filename == "/" {
		filename = "asset"
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	fields := map[string]string{
		"purpose": purpose,
	}
	return s.doMultipart(ctx, "/files", fields, "file", filename, data)
}

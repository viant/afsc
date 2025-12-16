package assets

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
)

// Upload uploads content as OpenAI file. Destination's base name is used as filename.
// Set purpose via content.Meta option, key "purpose" (defaults to "assistants").
//
// Expiration (expires_after):
//   - You can pass *openai.FileNewParamsExpiresAfter in options to set anchor and seconds.
//   - Or use meta keys: "expires_after[anchor]" and "expires_after[seconds]" (or dot-notation
//     equivalents: "expires_after.anchor", "expires_after.seconds"). Optional "days" is also
//     supported via "expires_after[days]" or "expires_after.days".
func (s *Storager) Upload(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	meta := &content.Meta{}
	option.Assign(options, &meta)
	purpose := meta.Values["purpose"]
	if purpose == "" {
		purpose = string(openai.FilePurposeAssistants)
	}

	fileName := strings.TrimSpace(destination)
	fileName = strings.TrimLeft(fileName, "/")

	// Check base name, and still allow file name as "some/dir/file.txt
	if base := path.Base(fileName); base == "" || base == "." || base == "/" {
		return fmt.Errorf("invalid filename: %q", destination)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	fields := map[string]string{
		"purpose": purpose,
	}

	// Accept ExpiresAfter provided via typed OpenAI option
	for _, opt := range options {
		if ea, ok := opt.(*openai.FileNewParamsExpiresAfter); ok && ea != nil {
			if ea.Seconds > 0 {
				fields["expires_after[seconds]"] = strconv.FormatInt(ea.Seconds, 10)
			}
			if anchor := string(ea.Anchor); anchor != "" {
				fields["expires_after[anchor]"] = anchor
			}
		}
	}

	// Accept ExpiresAfter via meta values (both bracket and dot notation)
	if len(meta.Values) > 0 {
		// Helper to set normalized field names if present
		setIf := func(keys ...string) {
			for _, k := range keys {
				if v := strings.TrimSpace(meta.Values[k]); v != "" {
					switch k {
					case "expires_after.anchor", "expires_after[anchor]", "expires_after_anchor":
						fields["expires_after[anchor]"] = v
					case "expires_after.seconds", "expires_after[seconds]", "expires_after_seconds":
						fields["expires_after[seconds]"] = v
					case "expires_after.days", "expires_after[days]", "expires_after_days":
						fields["expires_after[days]"] = v
					}
				}
			}
		}
		setIf(
			"expires_after.anchor", "expires_after[anchor]", "expires_after_anchor",
			"expires_after.seconds", "expires_after[seconds]", "expires_after_seconds",
			"expires_after.days", "expires_after[days]", "expires_after_days",
		)
	}

	// If expires_after seconds/days provided but no anchor, default to created_at
	if (fields["expires_after[seconds]"] != "" || fields["expires_after[days]"] != "") && fields["expires_after[anchor]"] == "" {
		fields["expires_after[anchor]"] = "created_at"
	}

	return s.doMultipart(ctx, "/files", fields, "file", fileName, data)
}

package gs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/viant/afs/option"
	astorage "github.com/viant/afs/storage"
	"time"
)

func (s *storager) presign(ctx context.Context, destination string, options []astorage.Option) error {
	preSign := &option.PreSign{}
	var err error
	if _, ok := option.Assign(options, &preSign); !ok {
		return nil
	}
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(preSign.TimeToLive),
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()
	preSign.URL, _ = client.Bucket(s.bucket).SignedURL(destination, opts)
	return err
}

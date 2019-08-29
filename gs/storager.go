package gs

import (
	"context"
	"github.com/viant/afs/http"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	goption "google.golang.org/api/option"
	gstorage "google.golang.org/api/storage/v1"
)

type storager struct {
	*gstorage.Service
	client *client
	bucket string
}

//Close closes storager
func (s *storager) Close() error {
	http.CloseIdleConnections(s.client)
	return nil
}

func (s *storager) Bucket(ctx context.Context) (*gstorage.Bucket, error) {
	call := s.Buckets.Get(s.bucket)
	call.Context(ctx)
	return call.Do()
}

func newStorager(ctx context.Context, baseURL string, options ...storage.Option) (*storager, error) {
	var gcpOptions ClientOptions
	project := &Project{}
	option.Assign(options, &gcpOptions, &project)
	var err error
	client := &client{
		ctx: ctx,
	}

	if len(gcpOptions) == 0 {
		client, err = newClient(ctx, options)
		if err != nil {
			return nil, err
		}
		gcpOptions = make(ClientOptions, 0)
		gcpOptions = append(gcpOptions, goption.WithHTTPClient(client.Client))
	}

	service, err := gstorage.NewService(ctx, gcpOptions...)
	if err != nil {
		return nil, err
	}

	if project.ID != "" {
		client.projectID = project.ID
	}

	return &storager{
		client:  client,
		Service: service,
		bucket:  url.Host(baseURL),
	}, nil
}

//NewStorager returns new storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	return newStorager(ctx, baseURL, options...)
}

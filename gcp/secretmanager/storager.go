package secretmanager

import (
	"context"
	"fmt"
	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"golang.org/x/oauth2/jwt"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"

	"github.com/viant/afsc/gs"
)

type Storager struct {
	options []storage.Option
	client  *secretmanager.Client
	service string
	config  *jwt.Config
}

// Exists returns true if location exists
func (s *Storager) Exists(ctx context.Context, resourceID string, options ...storage.Option) (bool, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return false, err
	}
	secret, _ := s.client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: resource.Name()})
	return secret != nil, nil
}

// Get returns a file info for supplied location
func (s *Storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	list, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("no found: %v", location)
	}
	return list[0], nil
}

// Delete deletes locations
func (s *Storager) Delete(ctx context.Context, location string, options ...storage.Option) error {
	return fmt.Errorf("unsupported operation")
}

// Close closes storage
func (s *Storager) Close() error {
	return s.client.Close()
}

// NewStorager create a new secreate manager storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*Storager, error) {
	authority := strings.ToLower(url.Host(baseURL))
	var gcpOptions gs.ClientOptions
	option.Assign(options, &gcpOptions)
	var err error
	if len(gcpOptions) == 0 {
		gcpOptions = make(gs.ClientOptions, 0)
	}
	gcpOptions = gs.Options(gs.DefaultOptions, gcpOptions)
	client, err := secretmanager.NewClient(ctx, gcpOptions...)
	if err != nil {
		return nil, err
	}
	return &Storager{service: authority, options: options, client: client}, nil
}

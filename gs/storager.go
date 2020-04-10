package gs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/http"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"golang.org/x/oauth2/jwt"
	goption "google.golang.org/api/option"
	gstorage "google.golang.org/api/storage/v1"
)

type storager struct {
	*gstorage.Service
	client *client
	bucket string
	config *jwt.Config
}

//Close closes storager
func (s *storager) Close() error {
	http.CloseIdleConnections(s.client)
	return nil
}

//Bucket returns bucket
func (s *storager) Bucket(ctx context.Context) (*gstorage.Bucket, error) {
	call := s.Buckets.Get(s.bucket)
	call.Context(ctx)
	return call.Do()
}

//FilterAuthOptions filters auth options
func (s storager) FilterAuthOptions(options []storage.Option) []storage.Option {
	var authOptions = make([]storage.Option, 0)
	if awsConfig, _ := s.filterAuthOption(options); awsConfig != nil {
		authOptions = append(authOptions, awsConfig)
	}
	return authOptions

}

//FilterAuthOptions filters auth options
func (s storager) filterAuthOption(options []storage.Option) (config *jwt.Config, err error) {
	config = &jwt.Config{}
	if _, ok := option.Assign(options, &config); ok {
		return config, nil
	}
	var provider JWTProvider
	if _, ok := option.Assign(options, &provider); ok {
		config, _, err = provider.JWTConfig(gstorage.CloudPlatformScope, gstorage.DevstorageFullControlScope)
	}
	return config, err
}

//IsAuthChanged return true if auth has changes
func (s *storager) IsAuthChanged(options []storage.Option) bool {
	authOptions := s.FilterAuthOptions(options)
	changed := s.isAuthChanged(authOptions)
	return changed
}

//IsAuthChanged return true if auth has changes
func (s *storager) isAuthChanged(authOptions []storage.Option) bool {
	if len(authOptions) == 0 {
		return false
	}
	jwtConfig, _ := s.filterAuthOption(authOptions)
	if jwtConfig == nil || s.config == nil {
		return true
	}
	return jwtConfig.PrivateKeyID != s.config.PrivateKeyID || !bytes.Equal(jwtConfig.PrivateKey, s.config.PrivateKey)
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
			return nil, errors.Wrapf(err, "failed to client %T", client)
		}
		gcpOptions = make(ClientOptions, 0)
		gcpOptions = append(gcpOptions, goption.WithHTTPClient(client.Client))
	}

	service, err := gstorage.NewService(ctx, gcpOptions...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create %T", service)
	}
	if project.ID != "" {
		client.projectID = project.ID
	}
	bucket := url.Host(baseURL)
	if bucket == "" {
		return nil, fmt.Errorf("bucket was empty, URL: %v", baseURL)
	}
	result := &storager{
		client:  client,
		Service: service,
		bucket:  bucket,
	}
	result.config, _ = result.filterAuthOption(options)
	return result, nil
}

func (s *storager) disableProxy(ctx context.Context) error {
	s.client.disableProxy()
	gcpOptions := make(ClientOptions, 0)
	gcpOptions = append(gcpOptions, goption.WithHTTPClient(s.client.Client))
	service, err := gstorage.NewService(ctx, gcpOptions...)
	if err != nil {
		return err
	}
	s.Service = service
	return nil
}

//NewStorager returns new storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (storage.Storager, error) {
	return newStorager(ctx, baseURL, options...)
}

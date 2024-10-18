package secretmanager

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/viant/afs/storage"
)

type Storager struct {
	region string
	client *secretsmanager.Client
	mux    sync.Mutex
}

// Exists returns true if location exists
func (s *Storager) Exists(ctx context.Context, resourceID string, options ...storage.Option) (bool, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return false, err
	}
	client := s.secretManager(resource.Region)
	_, err = client.GetSecretValue(ctx,
		&secretsmanager.GetSecretValueInput{
			SecretId:     &resource.Secret,
			VersionStage: aws.String("AWSCURRENT"),
		})
	return !isNotFound(err), nil
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
	return nil
}

func (s *Storager) secretManager(region string) *secretsmanager.Client {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.region == "" || s.region != region {
		s.region = region
		s.client = secretsmanager.New(secretsmanager.Options{Region: region})
		return s.client
	}
	return s.client
}

// NewStorager create a new secret manager storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*Storager, error) {
	result := &Storager{}
	return result, nil
}

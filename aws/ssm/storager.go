package ssm

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/viant/afs/storage"
	"os"
	"sync"
)

type storager struct {
	sess   *session.Session
	region string
	client *ssm.SSM
	mux    sync.Mutex
}

//Exists returns true if location exists
func (s *storager) Exists(ctx context.Context, resourceID string, options ...storage.Option) (bool, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return false, err
	}
	client := s.systemManager(resource.Region)
	param, _ := s.getParameter(ctx, client, resource)
	return param != nil, nil
}

//Get returns a file info for supplied location
func (s *storager) Get(ctx context.Context, location string, options ...storage.Option) (os.FileInfo, error) {
	list, err := s.List(ctx, location, options...)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("no found: %v", location)
	}
	return list[0], nil
}

//Delete deletes locations
func (s *storager) Delete(ctx context.Context, location string, options ...storage.Option) error {
	return fmt.Errorf("unsupported operation")
}

//Close closes storage
func (s *storager) Close() error {
	return nil
}

func (s *storager) systemManager(region string) *ssm.SSM {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.region == "" || s.region != region {
		s.region = region
		s.client = ssm.New(s.sess, aws.NewConfig().WithRegion(region))
		return s.client
	}
	return s.client
}

//NewStorager create a new secreate manager storager
func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*storager, error) {
	result := &storager{}
	var err error
	if result.sess, err = session.NewSession(); err != nil {
		return nil, err
	}
	return result, nil
}

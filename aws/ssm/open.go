package ssm

import (
	"context"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/viant/afs/storage"
	"io/ioutil"
	"strings"

	"io"
)

//Open returns a reader closer for supplied resources
func (s *storager) Open(ctx context.Context, resourceID string, options ...storage.Option) (io.ReadCloser, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return nil, err
	}
	client := s.systemManager(resource.Region)
	parameter, err := s.getParameter(ctx, client, resource)
	if err != nil {
		return nil, err
	}
	value := ""
	if parameter != nil && parameter.Value != nil {
		value = *parameter.Value
	}
	return ioutil.NopCloser(strings.NewReader(value)), nil
}

func (s *storager) getParameter(ctx context.Context, client *ssm.SSM, resource *Resource) (*ssm.Parameter, error) {
	withDecryption := true
	output, err := client.GetParameterWithContext(ctx,
		&ssm.GetParameterInput{
			Name:           &resource.Name,
			WithDecryption: &withDecryption,
		})
	if err != nil || output == nil {
		return nil, err
	}
	return output.Parameter, nil
}

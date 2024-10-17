package ssm

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/viant/afs/storage"
)

// Open returns a reader closer for supplied resources
func (s *Storager) Open(ctx context.Context, resourceID string, options ...storage.Option) (io.ReadCloser, error) {
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
	return io.NopCloser(strings.NewReader(value)), nil
}

func (s *Storager) getParameter(ctx context.Context, client *ssm.Client, resource *Resource) (*types.Parameter, error) {
	withDecryption := true
	output, err := client.GetParameter(ctx,
		&ssm.GetParameterInput{
			Name:           &resource.Name,
			WithDecryption: &withDecryption,
		})
	if err != nil || output == nil {
		return nil, err
	}
	return output.Parameter, nil
}

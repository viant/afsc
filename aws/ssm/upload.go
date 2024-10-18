package ssm

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/viant/afs/storage"
)

// Upload uploads
func (s *Storager) Upload(ctx context.Context, resourceID string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	resource, err := newResource(resourceID)
	if err != nil {
		return err
	}
	data, _ := io.ReadAll(reader)
	stringValue := string(data)
	client := s.systemManager(resource.Region)
	overwrite := true
	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      &resource.Name,
		Type:      "SecureString",
		Value:     &stringValue,
		Overwrite: &overwrite})

	return err
}

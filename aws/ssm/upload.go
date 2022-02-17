package ssm

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
	"os"
)

//Upload uploads
func (s *storager) Upload(ctx context.Context, resourceID string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	resource, err := newResource(resourceID)
	if err != nil {
		return err
	}
	data, _ := ioutil.ReadAll(reader)
	stringValue := string(data)
	client := s.systemManager(resource.Region)
	overwrite := true
	_, err = client.PutParameterWithContext(ctx, &ssm.PutParameterInput{
		Name:      &resource.Name,
		Type:      aws.String("SecureString"),
		Value:     &stringValue,
		Overwrite: &overwrite})

	return err
}

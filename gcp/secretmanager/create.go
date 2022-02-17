package secretmanager

import (
	"context"
	"github.com/viant/afs/storage"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
	"io"
	"os"
)

//Create create file or directory
func (s *storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	return nil
}

func (s *storager) createSecret(ctx context.Context, resource *Resource) (*secretmanagerpb.Secret, error) {
	request := &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/" + resource.ProjectID,
		SecretId: resource.Secret,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	}
	secret, err := s.client.CreateSecret(ctx, request)
	return secret, err
}

package secretmanager

import (
	"context"
	"io"
	"os"

	"github.com/viant/afs/storage"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

// Create create file or directory
func (s *Storager) Create(ctx context.Context, destination string, mode os.FileMode, reader io.Reader, isDir bool, options ...storage.Option) error {
	return nil
}

func (s *Storager) createSecret(ctx context.Context, resource *Resource) (*secretmanagerpb.Secret, error) {
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

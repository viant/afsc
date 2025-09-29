package secretmanager

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
)

// List lists secret resources
func (s *Storager) List(ctx context.Context, resourceID string, options ...storage.Option) ([]os.FileInfo, error) {
	var result []os.FileInfo
	resource, err := newResource(resourceID)
	if err != nil {
		return nil, err
	}
	client, err := s.secretManager(ctx, resource.Region)
	if err != nil {
		return nil, err
	}

	var nextToken *string
	for {
		output, err := client.ListSecrets(ctx, &secretsmanager.ListSecretsInput{
			NextToken: nextToken,
			Filters: []types.Filter{
				{
					Key:    "name",
					Values: []string{resource.Secret},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		nextToken = output.NextToken
		for _, entry := range output.SecretList {
			modTime := *entry.CreatedDate
			if entry.LastChangedDate != nil {
				modTime = *entry.LastChangedDate
			}
			result = append(result, file.NewInfo(*entry.Name, 0, file.DefaultFileOsMode, modTime, false))
		}
		if nextToken == nil {
			break
		}
	}
	return result, nil
}

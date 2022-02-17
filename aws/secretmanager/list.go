package secretmanager

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"os"
)

//List lists secret resources
func (s *storager) List(ctx context.Context, resourceID string, options ...storage.Option) ([]os.FileInfo, error) {
	var result []os.FileInfo
	resource, err := newResource(resourceID)
	if err != nil {
		return nil, err
	}
	client := s.secretManager(resource.Region)
	var nextToken *string
	for {
		output, err := client.ListSecretsWithContext(ctx, &secretsmanager.ListSecretsInput{
			NextToken: nextToken,
			Filters: []*secretsmanager.Filter{
				{
					Key:    aws.String("name"),
					Values: []*string{&resource.Secret},
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

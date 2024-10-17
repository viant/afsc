package ssm

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
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
	client := s.systemManager(resource.Region)

	var info []os.FileInfo
	for {
		var nextToken *string
		output, err := client.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{ParameterFilters: []types.ParameterStringFilter{
			{
				Key:    aws.String("name"),
				Values: []string{resource.Name},
			},
		}, NextToken: nextToken})
		if err != nil {
			return nil, err
		}
		for _, param := range output.Parameters {
			var modified time.Time
			if param.LastModifiedDate != nil {
				modified = *param.LastModifiedDate
			}
			info = append(info, file.NewInfo(*param.Name, int64(len(*param.Value)), file.DefaultFileOsMode, modified, false))
		}
		nextToken = output.NextToken
		if nextToken != nil {
			break
		}
	}
	return result, nil
}

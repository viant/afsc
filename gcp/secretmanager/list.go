package secretmanager

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

// List lists location assets
func (s *Storager) List(ctx context.Context, resourceID string, options ...storage.Option) ([]os.FileInfo, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return nil, err
	}
	var info []os.FileInfo
	if resource.Secret == "" {
		if err = s.listSecret(ctx, resource, &info); err != nil {
			return nil, err
		}
		return info, nil
	}
	if resource.Version == "" {
		if err = s.listSecretVersions(ctx, resource, &info); err != nil {
			return nil, err
		}
		return info, nil
	}
	return nil, fmt.Errorf("invalid resource: %v", resourceID)
}

func (s *Storager) listSecret(ctx context.Context, resource *Resource, info *[]os.FileInfo) error {
	pageToken := ""
	for {
		request := &secretmanagerpb.ListSecretsRequest{
			Parent:    "projects/" + resource.ProjectID,
			PageToken: pageToken,
		}
		iterator := s.client.ListSecrets(ctx, request)
		for {
			secret, err := iterator.Next()
			if secret == nil {
				break
			}
			match := "/secrets/"
			index := strings.Index(secret.Name, match)
			name := secret.Name[index+len(match):]
			*info = append(*info, file.NewInfo(name, 0, file.DefaultDirOsMode, secret.CreateTime.AsTime(), true, nil))
			if err != nil {
				return err
			}
		}
		pageInfo := iterator.PageInfo()
		if pageInfo != nil {
			pageToken = pageInfo.Token
		}
		if pageToken == "" {
			break
		}
	}
	return nil
}

func (s *Storager) listSecretVersions(ctx context.Context, resource *Resource, info *[]os.FileInfo) error {
	pageToken := ""
	for {
		request := &secretmanagerpb.ListSecretVersionsRequest{
			Parent:    "projects/" + resource.ProjectID + "/secrets/" + resource.Secret,
			PageToken: pageToken,
		}
		iterator := s.client.ListSecretVersions(ctx, request)
		for {
			secret, err := iterator.Next()
			if secret == nil {
				break
			}
			match := "/secrets/" + resource.Secret
			index := strings.Index(secret.Name, match)
			name := secret.Name[index+len(match):]
			*info = append(*info, file.NewInfo(name, 0, file.DefaultDirOsMode, secret.CreateTime.AsTime(), true, nil))
			if err != nil {
				return err
			}
		}
		pageInfo := iterator.PageInfo()
		if pageInfo != nil {
			pageToken = pageInfo.Token
		}
		if pageToken == "" {
			break
		}
	}
	return nil
}

package secretmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/viant/afs/storage"
)

// Open returns a reader closer for supplied resources
func (s *Storager) Open(ctx context.Context, resourceID string, options ...storage.Option) (io.ReadCloser, error) {
	resource, err := newResource(resourceID)
	if err != nil {
		return nil, err
	}
	client := s.secretManager(resource.Region)
	output, err := s.getSecret(ctx, client, resource)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	var secretString string
	if output.SecretString != nil {
		secretString = *output.SecretString
		reader = strings.NewReader(secretString)
	}
	if len(output.SecretBinary) > 0 {
		decodedBinarySecretBytes := make([]byte, base64.StdEncoding.DecodedLen(len(output.SecretBinary)))
		size, err := base64.StdEncoding.Decode(decodedBinarySecretBytes, output.SecretBinary)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode: %w", err)
		}
		if len(secretString) > 0 {
			binarySecret := string(decodedBinarySecretBytes[:size])
			if len(secretString) > 0 && secretString != binarySecret {
				return nil, fmt.Errorf("both binary and string secrets are set, but are different")
			}
		} else {
			reader = bytes.NewReader(decodedBinarySecretBytes[:size])
		}
	}
	return io.NopCloser(reader), nil
}

func (s *Storager) getSecret(ctx context.Context, client *secretsmanager.Client, resource *Resource) (*secretsmanager.GetSecretValueOutput, error) {
	return client.GetSecretValue(ctx,
		&secretsmanager.GetSecretValueInput{
			SecretId:     &resource.Secret,
			VersionStage: aws.String("AWSCURRENT"),
		})
}

package secretmanager

import (
	"context"
	"encoding/base64"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
	"os"
	"unicode"
)

//Upload uploads
func (s *storager) Upload(ctx context.Context, resourceID string, mode os.FileMode, reader io.Reader, options ...storage.Option) error {
	resource, err := newResource(resourceID)
	if err != nil {
		return err
	}
	data, _ := ioutil.ReadAll(reader)
	var secretBinary []byte
	var secretString *string
	if isASCII(string(data)) {
		text := string(data)
		secretString = &text
	} else {
		secretBinary = make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(secretBinary, data)
	}

	client := s.secretManager(resource.Region)
	secret, err := s.getSecret(ctx, client, resource)
	if isNotFound(err) {
		_, err = client.CreateSecretWithContext(ctx, &secretsmanager.CreateSecretInput{
			Name:         aws.String(resource.Secret),
			SecretString: secretString,
			SecretBinary: secretBinary,
		})
		return err
	}
	_, err = client.UpdateSecretWithContext(ctx, &secretsmanager.UpdateSecretInput{
		SecretId:     secret.ARN,
		SecretString: secretString,
		SecretBinary: secretBinary,
	})
	return err
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

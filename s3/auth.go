package s3

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"io"
	"io/ioutil"
	"os"
)

//AwsConfigProvider represents aws config provider
type AwsConfigProvider interface {
	AwsConfig() (*aws.Config, error)
}

//AuthConfig represents an auth config
type AuthConfig struct {
	Key       string `json:",omitempty"`
	Secret    string `json:",omitempty"`
	Region    string `json:",omitempty"`
	AccountID string `json:"-"`
	Token     string `json:"-"`
}

//AwsConfig returns aws config
func (c *AuthConfig) AwsConfig() (*aws.Config, error) {
	awsCredentials := credentials.NewStaticCredentials(c.Key, c.Secret, c.Token)
	_, err := awsCredentials.Get()
	if err != nil {
		return nil, errors.Wrap(err, "invalid credentials")
	}
	return aws.NewConfig().WithRegion(c.Region).WithCredentials(awsCredentials), nil
}

//NewAuthConfig returns new auth config from location
func NewAuthConfig(options ...storage.Option) (*AuthConfig, error) {
	location := &option.Location{}
	var JSONPayload = make([]byte, 0)
	option.Assign(options, &location, &JSONPayload)
	if location.Path == "" && len(JSONPayload) == 0 {
		return nil, errors.New("location was empty")
	}

	var reader io.Reader
	if location.Path != "" {
		file, err := os.Open(location.Path)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open auth config")
		}
		defer func() { _ = file.Close() }()
		if JSONPayload, err = ioutil.ReadAll(reader);err != nil {
			return nil, err
		}

	}
	config := &AuthConfig{}
	err := json.NewDecoder(bytes.NewReader(JSONPayload)).Decode(config)
	return config, err

}

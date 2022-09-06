package s3

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

//AwsConfigProvider represents aws config provider
type AwsConfigProvider interface {
	AwsConfig() (*aws.Config, error)
}

//AuthConfig represents an auth config
type AuthConfig struct {
	Key       string    `json:",omitempty"`
	Secret    string    `json:",omitempty"`
	Region    string    `json:",omitempty"`
	AccountID string    `json:"-"`
	Token     string    `json:"-"`
	Expiry    time.Time `json:"-"`
	RoleArn   string    `json:",omitempty"`
}

//AwsConfig returns aws config
func (c *AuthConfig) AwsConfig() (*aws.Config, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(os.Getenv("AWS_REGION")),
		Credentials: credentials.NewStaticCredentials(c.Key, c.Secret, ""),
	})
	if err != nil {
		log.Println("NewSession Error", err)
		return nil, err
	}

	svc := sts.New(sess)

	if c.RoleArn != "" && c.Token == "" {
		roleToAssumeArn := c.RoleArn
		sessionName := os.Getenv("AWS_LAMBDA_FUNCTION_NAME") + "_session"
		result, err := svc.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         &roleToAssumeArn,
			RoleSessionName: &sessionName,
		})

		if err != nil {
			return nil, err
		}

		c.Key = *result.Credentials.AccessKeyId
		c.Secret = *result.Credentials.SecretAccessKey
		c.Token = *result.Credentials.SessionToken
		c.Expiry = *result.Credentials.Expiration
	}

	// Functions running for 8 hrs (Max expiration time) need to assume role again
	if time.Now().After(c.Expiry) {
		c.Token = ""
	}

	awsCredentials := credentials.NewStaticCredentials(c.Key, c.Secret, c.Token)
	_, err = awsCredentials.Get()
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
		return nil, errors.New("auth location was empty")
	}
	if location.Path != "" {
		locationPath := location.Path
		if strings.HasPrefix(locationPath, "~/") {
			locationPath = path.Join(os.Getenv("HOME"), locationPath[2:])
		}
		file, err := os.Open(locationPath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open auth config")
		}
		defer func() { _ = file.Close() }()
		if JSONPayload, err = ioutil.ReadAll(file); err != nil {
			return nil, err
		}

	}
	config := &AuthConfig{}
	err := json.NewDecoder(bytes.NewReader(JSONPayload)).Decode(config)
	return config, err

}

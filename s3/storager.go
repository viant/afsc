package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/logger"
	"log"
	"os"
)

const (
	awsRegionEnvKey = "AWS_REGION"
	awsCredentials  = "AWS_CREDENTIALS"
)

var awsDefaultRegion = "us-east-1"

type storager struct {
	*s3.S3
	bucket string
	region string
	config *aws.Config
}

//Close closes storager
func (s *storager) Close() error {
	return nil
}

//FilterAuthOptions filters auth options
func (s storager) FilterAuthOptions(options []storage.Option) []storage.Option {
	var authOptions = make([]storage.Option, 0)
	if awsConfig, _ := filterAuthOption(options); awsConfig != nil {
		authOptions = append(authOptions, awsConfig)
	}
	return authOptions
}

//FilterAuthOptions filters auth options
func filterAuthOption(options []storage.Option) (*aws.Config, error) {
	config := &aws.Config{}
	if _, ok := option.Assign(options, &config); ok {
		return config, nil
	}
	var provider AwsConfigProvider
	if _, ok := option.Assign(options, &provider); ok {
		return provider.AwsConfig()
	}
	if credLocation := os.Getenv(awsCredentials); credLocation != "" {
		authConfig, err := NewAuthConfig(&option.Location{Path: credLocation})
		if err != nil {
			log.Print(err)
		}
		if err == nil {
			return authConfig.AwsConfig()
		}
	}
	return nil, nil
}

//IsAuthChanged return true if auth has changes
func (s *storager) IsAuthChanged(authOptions []storage.Option) bool {
	changed := s.isAuthChanged(authOptions)
	return changed
}

//IsAuthChanged return true if auth has changes
func (s *storager) isAuthChanged(authOptions []storage.Option) bool {
	if len(authOptions) == 0 {
		return false
	}
	awsConfig, _ := filterAuthOption(authOptions)
	if awsConfig == nil {
		return false
	}
	cred, err := s.config.Credentials.Get()
	if err != nil {
		return true
	}
	candidateCred, err := awsConfig.Credentials.Get()
	if err != nil {
		return true
	}
	return cred.AccessKeyID != candidateCred.AccessKeyID || cred.SecretAccessKey != candidateCred.SecretAccessKey
}

func getAwsConfig(options []storage.Option) (config *aws.Config, err error) {
	if config, err = filterAuthOption(options); err != nil {
		return nil, err
	}
	if config == nil {
		config = &aws.Config{}
	}
	region := &option.Region{}
	if _, ok := option.Assign(options, &region); ok {
		config.Region = &region.Name
	}
	if awsRegion := os.Getenv(awsRegionEnvKey); awsRegion != "" {
		config.Region = &awsRegion
	}
	return config, err
}

func newStorager(ctx context.Context, baseURL string, options ...storage.Option) (*storager, error) {
	result := &storager{
		bucket: url.Host(baseURL),
	}
	var err error
	result.config, err = getAwsConfig(options)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get aws config")
	}

	if result.config != nil {
		result.S3 = s3.New(session.New(), result.config)
	} else {
		result.S3 = s3.New(session.New())
	}

	if result.S3.Config.Region == nil || *result.S3.Config.Region == "" {
		result.S3.Config.Region = &awsDefaultRegion
		result.Config.Region = &awsDefaultRegion
		result.S3 = s3.New(session.New(), result.config)
	}
	result.adjustRegionIfNeeded()
	return result, nil
}

func (s *storager) adjustRegionIfNeeded() {
	output, err := s.S3.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: &s.bucket})
	if err != nil {
		logger.Logf("unable to get '%v' bucket location: %v", s.bucket, err)
		return
	}
	if output.LocationConstraint != nil {
		if s.config.Region == nil || *s.config.Region != *output.LocationConstraint {
			s.config.Region = output.LocationConstraint
			s.S3 = s3.New(session.New(), s.config)
		}
	} else if s.config != nil {
		if s.config.Region != nil && *s.config.Region != awsDefaultRegion {
			s.config.Region = &awsDefaultRegion
			s.S3 = s3.New(session.New(), s.config)
		}

	}
}

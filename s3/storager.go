package s3

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"

	"github.com/viant/afsc/logger"
)

const (
	awsRegionEnvKey = "AWS_REGION"
	awsCredentials  = "AWS_CREDENTIALS"
)

var awsDefaultRegion = "us-east-1"

type Storager struct {
	*s3.Client
	presignClient *s3.PresignClient
	bucket        string
	region        string
	config        *aws.Config
	logger        *option.Logger
}

// Close closes storager
func (s *Storager) Close() error {
	return nil
}

func (s *Storager) logF(format string, args ...interface{}) {
	if s.logger == nil {
		return
	}
	s.logger.Logf(format, args...)
}

// FilterAuthOptions filters auth options
func (s *Storager) FilterAuthOptions(options []storage.Option) []storage.Option {
	var authOptions = make([]storage.Option, 0)
	if awsConfig, _ := filterAuthOption(options); awsConfig != nil {
		authOptions = append(authOptions, awsConfig)
	}
	return authOptions
}

// FilterAuthOptions filters auth options
func filterAuthOption(options []storage.Option) (*aws.Config, error) {
	awsConfig := &aws.Config{}
	if _, ok := option.Assign(options, &awsConfig); ok {
		return awsConfig, nil
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

// IsAuthChanged return true if auth has changes
func (s *Storager) IsAuthChanged(authOptions []storage.Option) bool {
	changed := s.isAuthChanged(authOptions)
	return changed
}

// IsAuthChanged return true if auth has changes
func (s *Storager) isAuthChanged(authOptions []storage.Option) bool {
	if len(authOptions) == 0 {
		return false
	}
	awsConfig, _ := filterAuthOption(authOptions)
	if awsConfig == nil {
		return false
	}
	cred, err := s.config.Credentials.Retrieve(context.Background())
	if err != nil {
		return true
	}
	candidateCred, err := awsConfig.Credentials.Retrieve(context.Background())
	if err != nil {
		return true
	}
	return cred.AccessKeyID != candidateCred.AccessKeyID || cred.SecretAccessKey != candidateCred.SecretAccessKey
}

func getAwsConfig(options []storage.Option) (awsConfig *aws.Config, err error) {
	if awsConfig, err = filterAuthOption(options); err != nil {
		return nil, err
	}
	if awsConfig == nil {
		defaultConfig, loadErr := config.LoadDefaultConfig(context.Background())
		if loadErr != nil {
			return nil, loadErr
		}
		awsConfig = &defaultConfig
	}
	region := &option.Region{}
	if _, ok := option.Assign(options, &region); ok {
		awsConfig.Region = region.Name
	}
	if awsRegion := os.Getenv(awsRegionEnvKey); awsRegion != "" {
		awsConfig.Region = awsRegion
	}
	return awsConfig, err
}

func NewStorager(ctx context.Context, baseURL string, options ...storage.Option) (*Storager, error) {
	result := &Storager{
		bucket: url.Host(baseURL),
	}

	var err error
	result.config, err = getAwsConfig(options)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get aws config")
	}
	err = result.initS3Client()
	if err != nil {
		return nil, err
	}
	result.logger = &option.Logger{}
	option.Assign(options, &result.logger)
	return result, nil
}

func (s *Storager) initS3Client() error {
	if s.config != nil {
		s.Client = s3.NewFromConfig(*s.config)
	} else {
		defaultConfig, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return err
		}
		s.Client = s3.NewFromConfig(defaultConfig)
	}
	if s.Client.Options().Region == "" {
		s.config.Region = awsDefaultRegion
		s.Client = s3.NewFromConfig(*s.config)
	}
	s.adjustRegionIfNeeded()
	s.presignClient = s3.NewPresignClient(s.Client)
	return nil
}

func (s *Storager) adjustRegionIfNeeded() {
	started := time.Now()
	defer func() {
		s.logF("s3:GetBucketLocation %v %s\n", s.bucket, time.Since(started))
	}()
	output, err := s.Client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: &s.bucket})
	if err != nil {
		logger.Logf("unable to get '%v' bucket location: %v", s.bucket, err)
		return
	}
	if output.LocationConstraint != "" {
		if s.config.Region == "" || s.config.Region != string(output.LocationConstraint) {
			s.config.Region = string(output.LocationConstraint)
			s.Client = s3.NewFromConfig(*s.config)
		}
	} else if s.config != nil {
		if s.config.Region == "" || (s.config.Region != "" && s.config.Region != awsDefaultRegion) {
			s.config.Region = awsDefaultRegion
			s.Client = s3.NewFromConfig(*s.config)
		}
	}
}

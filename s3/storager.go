package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
)

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

func getAwsConfig(options []storage.Option) (config *aws.Config, err error) {
	config = &aws.Config{}
	var provider AwsConfigProvider
	region := &Region{}
	authConfig := &AuthConfig{}
	optionsCount := len(options)
	options, _ = option.Assign(options, &config)
	option.Assign(options, &region)
	if hasAssign := len(options) != optionsCount; !hasAssign {
		options, _ = option.Assign(options, &provider, &authConfig)
		if provider != nil {
			if config, err = provider.AwsConfig(); err != nil {
				return nil, err
			}
		} else if authConfig.Key != "" {
			config, err = authConfig.AwsConfig()
		}
	}
	if err == nil && region.Name != "" {
		config.Region = &region.Name
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
		return nil, err
	}
	if result.config != nil {
		result.S3 = s3.New(session.New(), result.config)
	} else {
		result.S3 = s3.New(session.New())
	}
	output, err := result.S3.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: &result.bucket})
	if err == nil {
		if output.LocationConstraint != nil {
			result.config.Region = output.LocationConstraint
			result.S3 = s3.New(session.New(), result.config)
		}
	}
	return result, nil
}

package s3

import (
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3UnitBucket = "afsc-unit-test-bucket"

const s3UnitEmptyList = `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`

// s3UnitHTTPClient intercepts every SDK request; unit tests never contact AWS.
type s3UnitHTTPClient func(*http.Request) (*http.Response, error)

func (f s3UnitHTTPClient) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func newS3UnitStorager(httpClient s3UnitHTTPClient) *Storager {
	config := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("unit-access-key", "unit-secret-key", ""),
		HTTPClient:  httpClient,
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}
	client := awss3.NewFromConfig(config, func(options *awss3.Options) {
		options.UsePathStyle = true
	})
	return &Storager{
		Client:        client,
		presignClient: awss3.NewPresignClient(client),
		bucket:        s3UnitBucket,
		region:        config.Region,
		config:        &config,
	}
}

func s3UnitResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Header:        http.Header{"Content-Type": {"application/xml"}, "Content-Length": {strconv.Itoa(len(body))}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

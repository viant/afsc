package s3

import (
	"context"
	"fmt"
	"github.com/viant/afs/option"
	"os"
	"path"
	"time"
)

//TestBucket test bucket
var TestBucket = fmt.Sprintf("viante2etest%v", time.Now().Format("20060102"))

//NewTestAuthConfig returns an auth config
func NewTestAuthConfig() (*AuthConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "aws-e2e.json")
	return NewAuthConfig(option.NewLocation(secretPath))
}

//NewTestStorager returns a test instance
func NewTestStorager(ctx context.Context, bucket string) (*storager, error) {
	if bucket == "" {
		bucket = TestBucket
	}
	authConfig, err := NewTestAuthConfig()
	if err != nil {
		return nil, err
	}
	return newStorager(ctx, fmt.Sprintf("s3://%s", bucket), authConfig)

}

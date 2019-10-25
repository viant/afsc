package s3

import (
	"context"
	"fmt"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"os"
	"path"
	"time"
)

//TestBucket test bucket
var TestBucket = fmt.Sprintf("viantv0e2e%v", time.Now().Format("20060102"))

//NewTestAuthConfig returns an auth config
func NewTestAuthCustomConfig(cred string) (*AuthConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", cred+".json")
	return NewAuthConfig(option.NewLocation(secretPath))
}

//NewTestAuthConfig returns an auth config
func NewTestAuthConfig() (*AuthConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "aws-e2e.json")
	return NewAuthConfig(option.NewLocation(secretPath))
}

//NewTestStorager returns a test instance
func NewTestStorager(ctx context.Context, bucket string) (storage.Storager, error) {
	if bucket == "" {
		bucket = TestBucket
	}
	authConfig, err := NewTestAuthConfig()
	if err != nil {
		return nil, err
	}
	return newStorager(ctx, fmt.Sprintf("s3://%s", bucket), authConfig)

}

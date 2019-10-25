package gs

import (
	"context"
	"fmt"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"os"
	"path"
	"time"
)

//TestProject test project
var TestProject = "viant-e2e"

//TestBucket test bucket
var TestBucket = fmt.Sprintf("%v-test%v", TestProject, time.Now().Format("2006-01-02"))

//NewTestJwtConfig returns a jwt config
func NewTestJwtConfig() (*JwtConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "gcp-e2e.json")
	return NewJwtConfig(option.NewLocation(secretPath))
}

//NewCustomTestJwtConfig returns a custom jwt confi
func NewCustomTestJwtConfig(cred string) (*JwtConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", cred+".json")
	return NewJwtConfig(option.NewLocation(secretPath))
}

//NewTestStorager returns a test instance
func NewTestStorager(ctx context.Context, bucket string) (storage.Storager, error) {
	if bucket == "" {
		bucket = TestBucket
	}
	jwtConfig, err := NewTestJwtConfig()
	if err != nil {
		return nil, err
	}
	return newStorager(ctx, fmt.Sprintf("gs://%s", bucket), jwtConfig)

}

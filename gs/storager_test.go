package gs

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"

	"github.com/viant/afsc/auth"
)

// TestProject test project
var TestProject = "viant-e2e"

// TestBucket test bucket
var TestBucket = fmt.Sprintf("%v-test%v", TestProject, time.Now().Format("2006-01-02"))

// NewTestJwtConfig returns a jwt config
func NewTestJwtConfig() (*auth.JwtConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "gcp-e2e.json")
	return auth.NewJwtConfig(option.NewLocation(secretPath))
}

// NewCustomTestJwtConfig returns a custom jwt confi
func NewCustomTestJwtConfig(cred string) (*auth.JwtConfig, error) {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", cred+".json")
	return auth.NewJwtConfig(option.NewLocation(secretPath))
}

// NewTestStorager returns a test instance
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

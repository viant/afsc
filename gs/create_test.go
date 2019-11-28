package gs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/url"
	"io"
	"testing"
)

func TestStorager_Create(t *testing.T) {

	jwtConfig, err := NewTestJwtConfig()
	if err != nil {
		t.Skip(err)
		return
	}
	ctx := context.Background()
	var useCases = []struct {
		description string
		URL         string
		assets      []*asset.Resource
	}{
		{
			description: "single asset create",
			URL:         fmt.Sprintf("gs://%v/001_create/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("asset1.txt", []byte("test is test"), 0655),
			},
		},
		{
			description: "multi asset create",
			URL:         fmt.Sprintf("gs://%v/002_create/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("folder1/asset1.txt", []byte("test is test"), 0655),
				asset.NewFile("folder1/asset2.txt", []byte("test is test"), 0655),
			},
		},
	}

	mgr := New(jwtConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("gs://%v/", TestBucket))
	for _, useCase := range useCases {
		for _, asset := range useCase.assets {
			var reader io.Reader
			if len(asset.Data) > 0 {
				reader = bytes.NewReader(asset.Data)
			}

			err := mgr.Create(ctx, url.Join(useCase.URL, asset.Name), 0644, asset.Dir, reader)
			assert.Nil(t, err, useCase.description)
		}
		actuals, err := asset.Load(mgr, useCase.URL)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.Nil(t, err, useCase.description)
		for _, asset := range useCase.assets {
			actual, ok := actuals[asset.Name]
			assert.True(t, ok, useCase.description+" "+asset.Name)
			assert.NotNil(t, actual, useCase.description+" "+asset.Name)
		}

	}

}

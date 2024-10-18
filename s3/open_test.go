package s3

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/url"
)

func TestStorager_Download(t *testing.T) {

	authConfig, err := NewTestAuthConfig()
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
			description: "single asset download",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("download/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
		{
			description: "multi asset download",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("download/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("download/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	mgr := New(authConfig)
	defer func() {
		_ = mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))
	}()
	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		if !assert.Nil(t, err, useCase.description) {

		}

		for _, uasset := range useCase.assets {
			reader, err := mgr.OpenURL(ctx, url.Join(useCase.URL, uasset.Name))
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			data, err := io.ReadAll(reader)
			assert.EqualValues(t, uasset.Data, data, useCase.description+" "+uasset.Name)

		}

	}

}

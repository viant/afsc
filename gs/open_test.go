package gs

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
			description: "single asset open",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("open/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
		{
			description: "multi asset open",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("open/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("open/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	mgr := New(jwtConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("gs://%v/", TestBucket))
	for _, useCase := range useCases {

		err = asset.Create(mgr, useCase.URL, useCase.assets)
		assert.Nil(t, err, useCase.description)

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

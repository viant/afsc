package gs

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"testing"
)

func TestStorager_List(t *testing.T) {
	jwtConfig, err := NewTestJwtConfig()
	if err != nil {
		t.Skip(err)
		return
	}
	ctx := context.Background()
	var useCases = []struct {
		description string
		URL         string
		listURL     string
		assets      []*asset.Resource
		expect      []string
	}{
		{
			description: "single asset list",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("list01/asset1.txt", []byte("test is test 1 "), 0655),
			},
			listURL: fmt.Sprintf("gs://%v/list01", TestBucket),
			expect:  []string{"list01", "asset1.txt"},
		},
		{
			description: "multi asset list",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("list02/asset3.txt", []byte("test is test 1 "), 0655),
				asset.NewFile("list02/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("list02/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
			listURL: fmt.Sprintf("gs://%v/list02", TestBucket),
			expect:  []string{"list02", "asset3.txt", "folder1"},
		},
	}
	mgr := New(jwtConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("gs://%v/", TestBucket))

	for _, useCase := range useCases {

		err = asset.Create(mgr, useCase.URL, useCase.assets)
		assert.Nil(t, err, useCase.description)
		objects, err := mgr.List(ctx, useCase.listURL)
		assert.Nil(t, err, useCase.description)
		actuals := make(map[string]bool)
		for _, object := range objects {
			actuals[object.Name()] = true
		}
		for _, expect := range useCase.expect {
			ok := actuals[expect]
			assert.True(t, ok, useCase.description+" "+expect)
		}

	}
}

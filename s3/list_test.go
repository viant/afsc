package s3

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/storage"
	"testing"
)

func TestStorager_List(t *testing.T) {

	authConfig, err := NewTestAuthConfig()
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
		options     []storage.Option
		expect      []string
	}{
		{
			description: "single asset list",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("list01/asset1.txt", []byte("test is test 1 "), 0655),
			},
			listURL: fmt.Sprintf("s3://%v/list01", TestBucket),
			expect:  []string{"list01", "asset1.txt"},
		},
		{
			description: "multi asset list",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("list02/asset3.txt", []byte("test is test 1 "), 0655),
				asset.NewFile("list02/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("list02/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
			listURL: fmt.Sprintf("s3://%v/list02", TestBucket),
			expect:  []string{"list02", "asset3.txt", "folder1"},
		},
		{
			description: "multi asset list with matcher",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("list03/asset1.txt", []byte("test is test 1 "), 0655),
				asset.NewFile("list03/asset2.json", []byte("test is test 1 "), 0655),
				asset.NewFile("list03/asset3.csv", []byte("test is test 1 "), 0655),
			},
			options: []storage.Option{
				&matcher.Basic{Suffix: ".json"},
			},
			listURL: fmt.Sprintf("s3://%v/list03", TestBucket),
			expect:  []string{"asset2.json"},
		},
	}

	mgr := New(authConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))

	for _, useCase := range useCases {

		err = asset.Create(mgr, useCase.URL, useCase.assets)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		objects, err := mgr.List(ctx, useCase.listURL, useCase.options...)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		assert.Equal(t, len(useCase.expect), len(objects), useCase.description)
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

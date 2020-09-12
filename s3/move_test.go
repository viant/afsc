package s3

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/url"
	"io/ioutil"
	"strings"
	"testing"
)

func TestStorager_Move(t *testing.T) {
	authConfig, err := NewTestAuthConfig()
	if err != nil {
		t.Skip(err)
		return

	}

	ctx := context.Background()
	var useCases = []struct {
		description string
		URL         string
		source      string
		dest        string
		assets      []*asset.Resource
	}{
		{
			description: "single asset download",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			dest:        "move001/dst",
			source:      "move001/src",
			assets: []*asset.Resource{
				asset.NewFile("move001/src/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
		{
			description: "multi asset download",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			dest:        "move002/dst",
			source:      "move002/src",
			assets: []*asset.Resource{
				asset.NewFile("move002/src/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("move002/src/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	mgr := newManager(authConfig)
	defer func() {
		_ = mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))
	}()
	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		assert.Nil(t, err, useCase.description)
		err := mgr.Move(ctx, url.Join(useCase.URL, useCase.source), url.Join(useCase.URL, useCase.dest))
		assert.Nil(t, err)
		for _, resource := range useCase.assets {
			URL := url.Join(useCase.URL, resource.Name)
			URL = strings.Replace(URL, useCase.source, useCase.dest, 1)
			reader, err := mgr.OpenURL(ctx, URL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			data, err := ioutil.ReadAll(reader)
			assert.EqualValues(t, resource.Data, data, useCase.description+" "+resource.Name)

		}

	}

}

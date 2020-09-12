package s3

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"io/ioutil"
	"strings"
	"testing"
)

func TestStorager_Copy(t *testing.T) {
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
			dest:        "copy001/dst",
			source:      "copy001/src",
			assets: []*asset.Resource{
				asset.NewFile("copy001/src/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
		{
			description: "multi asset download",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			dest:        "copy002/dst",
			source:      "copy002/src",
			assets: []*asset.Resource{
				asset.NewFile("copy002/src/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("copy002/src/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	fs := afs.New()
	mgr := newManager(authConfig)
	defer func() {
		_ = mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))
	}()
	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		err := fs.Copy(ctx, url.Join(useCase.URL, useCase.source), url.Join(useCase.URL, useCase.dest), option.NewSource(authConfig))
		if !assert.Nil(t, err, useCase.description) {
			t.Errorf("failed to copy %v", err)
			continue
		}

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

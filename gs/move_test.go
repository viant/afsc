package gs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/url"
)

func TestStorager_Move(t *testing.T) {
	jwtConfig, err := NewTestJwtConfig()
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
			description: "single asset open",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			dest:        "move001/dst",
			source:      "move001/src",
			assets: []*asset.Resource{
				asset.NewFile("move001/src/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
		{
			description: "multi asset open",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			dest:        "move002/dst",
			source:      "move002/src",
			assets: []*asset.Resource{
				asset.NewFile("move002/src/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("move002/src/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	mgr := newManager(jwtConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("gs://%v/", TestBucket))
	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		assert.Nil(t, err, useCase.description)
		err := mgr.Move(ctx, url.Join(useCase.URL, useCase.source), url.Join(useCase.URL, useCase.dest))
		assert.Nil(t, err)
		for _, uasset := range useCase.assets {
			URL := url.Join(useCase.URL, uasset.Name)
			URL = strings.Replace(URL, useCase.source, useCase.dest, 1)
			reader, err := mgr.OpenURL(ctx, URL)
			if !assert.Nil(t, err, useCase.description) {
				continue
			}
			data, err := io.ReadAll(reader)
			assert.EqualValues(t, uasset.Data, data, useCase.description+" "+uasset.Name)

		}

	}

}

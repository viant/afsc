package gs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"google.golang.org/api/option"
	"testing"
)

func TestStorager_Exists(t *testing.T) {

	ctx := context.Background()

	jwtConfig, err := NewTestJwtConfig()
	if err != nil {
		t.Skip(err)
		return
	}
	JSON, err := json.Marshal(jwtConfig)

	var useCases = []struct {
		description string
		URL         string
		assets      []*asset.Resource
	}{
		{
			description: "exists with custom option",
			URL:         fmt.Sprintf("gs://%v/", TestBucket),
			assets: []*asset.Resource{
				asset.NewFile("option001/folder1/asset1.txt", []byte("test is test 2"), 0655),
				asset.NewFile("option001/folder1/asset2.txt", []byte("test is test 3"), 0655),
			},
		},
	}
	jsonAuth := option.WithCredentialsJSON(JSON)
	mgr := New(NewClientOptions(jsonAuth), NewProject(TestProject))
	defer func() {
		_ = mgr.Delete(ctx, fmt.Sprintf("gs://%v/", TestBucket))
	}()

	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		checker := mgr.(storage.Checker)
		err = asset.Create(mgr, useCase.URL, useCase.assets)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		for _, resource := range useCase.assets {

			{
				URL := url.Join(useCase.URL, resource.Name)
				exists, err := checker.Exists(ctx, URL)
				assert.Nil(t, err)
				assert.True(t, exists)
			}

			{
				URL := url.Join(useCase.URL, resource.Name+".not")
				exists, err := checker.Exists(ctx, URL)
				assert.Nil(t, err)
				assert.False(t, exists, useCase.description)
			}

		}

	}
}

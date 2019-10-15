package s3

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/storage"
	"testing"
)

func TestStorager_Delete(t *testing.T) {

	authConfig, err := NewTestAuthConfig()
	if err != nil {
		t.Skip(err)
		return

	}
	ctx := context.Background()
	var useCases = []struct {
		description string
		assets      []*asset.Resource
		URL         string
		exists      bool
		hasError    bool
	}{
		{
			description: "delete test",
			URL:         fmt.Sprintf("s3://%v/delete_0001/asset1.txt", TestBucket),
			exists:      false,
			assets: []*asset.Resource{
				asset.NewFile("exists001/asset1.txt", []byte("test is test 1 "), 0655),
			},
		},
	}

	mgr := New(authConfig)
	defer mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))

	for _, useCase := range useCases {
		err = asset.Create(mgr, useCase.URL, useCase.assets)

		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		err := mgr.Delete(ctx, useCase.URL)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		checker := mgr.(storage.Checker)
		actual, err := checker.Exists(ctx, useCase.URL)
		assert.Nil(t, err, useCase.description)
		assert.Equal(t, useCase.exists, actual, useCase.description)
	}

}

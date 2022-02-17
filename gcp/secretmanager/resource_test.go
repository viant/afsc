package secretmanager

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_NewResource(t *testing.T) {
	var testCases = []struct {
		resourceID string
		expect     Resource
		hasError   bool
	}{
		{
			resourceID: "/projects/v-e2e/secrets",
			expect: Resource{
				ProjectID: "v-e2e",
				Secret:    "",
				Version:   "",
			},
		},
		{
			resourceID: "/projects/v-e2e/secrets/my-secret",
			expect: Resource{
				ProjectID: "v-e2e",
				Secret:    "my-secret",
				Version:   "",
			},
		},
		{
			resourceID: "/projects/v-e2e/secrets/my-secret/versions/latest",
			expect: Resource{
				ProjectID: "v-e2e",
				Secret:    "my-secret",
				Version:   "latest",
			},
		},
		{
			resourceID: "/projecs/v-e2e/secrets/my-secret/versions/latest",
			hasError:   true,
		},
	}

	for _, testCase := range testCases {
		resource, err := newResource(testCase.resourceID)
		if testCase.hasError {
			assert.NotNil(t, err, testCase.resourceID)
			continue
		}
		if !assert.Nil(t, err, testCase.resourceID) {
			continue
		}
		assert.EqualValues(t, &testCase.expect, resource)
	}
}

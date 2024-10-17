package ssm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewResource(t *testing.T) {
	var testCases = []struct {
		resourceID string
		expect     Resource
		hasError   bool
	}{
		{
			resourceID: "us-west-1/parameter/test2",
			expect: Resource{
				Region: "us-west-1",
				Name:   "test2",
			},
		},
		{
			resourceID: "/us-west-1/v-e2e/secrets/my-secret/versions/latest",
			hasError:   true,
		},
	}

	for _, testCase := range testCases {
		resource, err := newResource(testCase.resourceID)
		if testCase.hasError {
			assert.NotNil(t, err, testCase.resourceID, testCase.resourceID)
			continue
		}
		if !assert.Nil(t, err, testCase.resourceID, testCase.resourceID) {
			continue
		}
		assert.EqualValues(t, &testCase.expect, resource)
	}
}

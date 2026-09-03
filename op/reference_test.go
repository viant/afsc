package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReference(t *testing.T) {
	var testCases = []struct {
		baseURL    string
		resourceID string
		expect     string
		hasError   bool
	}{
		{
			baseURL:    "op://Private",
			resourceID: "/e2e-account.json/notesPlain",
			expect:     "op://Private/e2e-account.json/notesPlain",
		},
		{
			baseURL:    "op://Engineering",
			resourceID: "shared/gcp-sa/password",
			expect:     "op://Engineering/shared/gcp-sa/password",
		},
		{
			baseURL:    "op://Private",
			resourceID: "op://Private/e2e-account.json/notesPlain",
			expect:     "op://Private/e2e-account.json/notesPlain",
		},
		{
			baseURL:    "op://Private",
			resourceID: "//e2e-account.json/notesPlain",
			expect:     "op://Private/e2e-account.json/notesPlain",
		},
		{
			baseURL:    "op://Private",
			resourceID: "",
			hasError:   true,
		},
		{
			baseURL:    "op://",
			resourceID: "/item/field",
			hasError:   true,
		},
	}

	for _, testCase := range testCases {
		actual, err := reference(testCase.baseURL, testCase.resourceID)
		if testCase.hasError {
			assert.Error(t, err, testCase.resourceID)
			continue
		}
		if assert.NoError(t, err, testCase.resourceID) {
			assert.Equal(t, testCase.expect, actual, testCase.resourceID)
		}
	}
}

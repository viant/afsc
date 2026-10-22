package s3

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorager_ExistsUnit(t *testing.T) {
	testCases := []struct {
		name       string
		headStatus int
		listStatus int
		exists     bool
		hasError   bool
	}{
		{name: "existing object", headStatus: http.StatusOK, exists: true},
		{name: "missing object", headStatus: http.StatusNotFound, listStatus: http.StatusOK},
		{name: "access denied", headStatus: http.StatusForbidden, listStatus: http.StatusForbidden, hasError: true},
		{name: "listing denied after missing HEAD", headStatus: http.StatusNotFound, listStatus: http.StatusForbidden, hasError: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodHead {
					return s3UnitResponse(testCase.headStatus, ""), nil
				}
				assert.Equal(t, http.MethodGet, request.Method)
				if testCase.listStatus != http.StatusOK {
					return s3UnitResponse(testCase.listStatus, `<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>`), nil
				}
				return s3UnitResponse(http.StatusOK, s3UnitEmptyList), nil
			})

			exists, err := storager.Exists(context.Background(), "folder/report.csv")
			assert.Equal(t, testCase.exists, exists)
			if testCase.hasError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "AccessDenied")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStorager_ExistsTransportFailureUnit(t *testing.T) {
	transportErr := errors.New("unit transport failure")
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		return nil, transportErr
	})
	exists, err := storager.Exists(context.Background(), "folder/report.csv")
	assert.False(t, exists)
	assert.ErrorIs(t, err, transportErr)
}

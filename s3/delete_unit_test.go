package s3

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs/option"
)

func TestStorager_DeleteFailureUnit(t *testing.T) {
	transportErr := errors.New("unit transport failure")
	testCases := []struct {
		name         string
		status       int
		body         string
		transportErr error
	}{
		{name: "access denied", status: http.StatusForbidden, body: `<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>`},
		{name: "server failure", status: http.StatusInternalServerError, body: `<Error><Code>InternalError</Code><Message>Failed</Message></Error>`},
		{name: "transport failure", transportErr: transportErr},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var methods []string
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				methods = append(methods, request.Method)
				assert.Equal(t, "/"+s3UnitBucket+"/folder/report.csv", request.URL.Path)
				if testCase.transportErr != nil {
					return nil, testCase.transportErr
				}
				return s3UnitResponse(testCase.status, testCase.body), nil
			})

			err := storager.Delete(context.Background(), "/folder/report.csv")
			require.Error(t, err)
			if testCase.transportErr != nil {
				assert.ErrorIs(t, err, testCase.transportErr)
			}
			// A failed delete must not continue to List and discard its error.
			assert.Equal(t, []string{http.MethodDelete}, methods)
		})
	}
}

func TestStorager_DeleteMissingObjectUnit(t *testing.T) {
	deletes := 0
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodDelete {
			deletes++
			// S3 normally acknowledges deletion even when the key is absent.
			return s3UnitResponse(http.StatusNoContent, ""), nil
		}
		assert.Equal(t, http.MethodGet, request.Method)
		return s3UnitResponse(http.StatusOK, s3UnitEmptyList), nil
	})
	assert.NoError(t, storager.Delete(context.Background(), "report.csv"))
	assert.Equal(t, 1, deletes)
}

func TestStorager_DeleteNotFoundFileUnit(t *testing.T) {
	var methods []string
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		methods = append(methods, request.Method)
		return s3UnitResponse(http.StatusNotFound, `<Error><Code>NoSuchKey</Code><Message>Missing</Message></Error>`), nil
	})
	err := storager.Delete(context.Background(), "report.csv", option.NewObjectKind(true))
	require.Error(t, err)
	assert.True(t, isNotFound(err))
	assert.Equal(t, []string{http.MethodDelete}, methods)
}

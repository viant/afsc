package s3

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs/option"
)

type s3UnitContextKey struct{}

func TestStorager_GetContextUnit(t *testing.T) {
	testCases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "caller context", ctx: context.WithValue(context.Background(), s3UnitContextKey{}, "caller")},
		{name: "nil context"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requests := 0
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				requests++
				assert.Equal(t, http.MethodHead, request.Method)
				if testCase.ctx != nil {
					assert.Equal(t, "caller", request.Context().Value(s3UnitContextKey{}))
				}
				return s3UnitResponse(http.StatusOK, ""), nil
			})
			preSign := &option.PreSign{}
			info, err := storager.Get(testCase.ctx, "folder/report.csv", preSign,
				&PresignOptions{TimeToLive: time.Minute}, option.NewObjectKind(true))
			require.NoError(t, err)
			require.NotNil(t, info)
			assert.Equal(t, "report.csv", info.Name())
			require.NotEmpty(t, preSign.URL)
			signedURL, err := url.Parse(preSign.URL)
			require.NoError(t, err)
			assert.Equal(t, "60", signedURL.Query().Get("X-Amz-Expires"))
			assert.Equal(t, 1, requests)
		})
	}
}

func TestStorager_GetCancelledContextUnit(t *testing.T) {
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		return s3UnitResponse(http.StatusOK, ""), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	info, err := storager.Get(ctx, "folder/report.csv", option.NewObjectKind(true))
	assert.Nil(t, info)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestStorager_GetMissingFileUnit(t *testing.T) {
	var methods []string
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		methods = append(methods, request.Method)
		return s3UnitResponse(http.StatusNotFound, ""), nil
	})
	preSign := &option.PreSign{}
	info, err := storager.Get(context.Background(), "report.csv", preSign, option.NewObjectKind(true))
	require.Error(t, err)
	assert.Nil(t, info)
	assert.Empty(t, preSign.URL)
	assert.Equal(t, []string{http.MethodHead}, methods)
}

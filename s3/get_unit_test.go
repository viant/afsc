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
	"github.com/viant/afs/storage"
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

func TestStorager_GetInvalidPresignExpiryUnit(t *testing.T) {
	testCases := []struct {
		name      string
		sharedTTL time.Duration
		settings  *PresignOptions
	}{
		{name: "negative shared expiry", sharedTTL: -time.Second},
		{name: "subsecond shared expiry", sharedTTL: 500 * time.Millisecond},
		{name: "shared expiry above maximum", sharedTTL: 8 * 24 * time.Hour},
		{name: "negative override expiry", settings: &PresignOptions{TimeToLive: -time.Second}},
		{name: "subsecond override expiry", settings: &PresignOptions{TimeToLive: 500 * time.Millisecond}},
		{name: "override expiry above maximum", settings: &PresignOptions{TimeToLive: 8 * 24 * time.Hour}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var methods []string
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				methods = append(methods, request.Method)
				if request.Method == http.MethodHead {
					return s3UnitResponse(http.StatusOK, ""), nil
				}
				// A successful listing of the existing file must not hide the presign error.
				return s3UnitResponse(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>folder/report.csv</Key><LastModified>2026-11-06T00:00:00Z</LastModified><Size>4</Size></Contents></ListBucketResult>`), nil
			})
			preSign := &option.PreSign{TimeToLive: testCase.sharedTTL}
			options := []storage.Option{preSign}
			if testCase.settings != nil {
				options = append(options, testCase.settings)
			}
			info, err := storager.Get(context.Background(), "folder/report.csv", options...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "expiry must be between 1 second and 7 days")
			assert.Nil(t, info)
			assert.Empty(t, preSign.URL)
			assert.Empty(t, preSign.Header)
			assert.Equal(t, []string{http.MethodHead}, methods)
		})
	}
}

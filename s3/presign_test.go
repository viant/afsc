package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

func TestStorager_PresignExpiryUnit(t *testing.T) {
	testCases := []struct {
		name      string
		sharedTTL time.Duration
		settings  *PresignOptions
		expires   string
	}{
		{name: "default expiry", expires: "900"},
		{name: "zero override keeps default", settings: &PresignOptions{}, expires: "900"},
		{name: "custom expiry", settings: &PresignOptions{TimeToLive: time.Minute}, expires: "60"},
		{name: "shared TTL honored", sharedTTL: 5 * time.Minute, expires: "300"},
		{name: "zero override keeps shared TTL", sharedTTL: 5 * time.Minute, settings: &PresignOptions{}, expires: "300"},
		{name: "override with shared TTL", sharedTTL: 5 * time.Minute, settings: &PresignOptions{TimeToLive: time.Minute}, expires: "60"},
		{name: "shared minimum expiry", sharedTTL: time.Second, expires: "1"},
		{name: "shared maximum expiry", sharedTTL: 7 * 24 * time.Hour, expires: "604800"},
		{name: "override minimum expiry", settings: &PresignOptions{TimeToLive: time.Second}, expires: "1"},
		{name: "override maximum expiry", settings: &PresignOptions{TimeToLive: 7 * 24 * time.Hour}, expires: "604800"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requests := 0
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				requests++
				return nil, fmt.Errorf("unexpected HTTP request during presigning")
			})
			preSign := &option.PreSign{TimeToLive: testCase.sharedTTL}
			options := []storage.Option{preSign}
			if testCase.settings != nil {
				options = append(options, testCase.settings)
			}

			require.NoError(t, storager.presign(context.Background(), "folder/report file.csv", options))
			require.NotEmpty(t, preSign.URL)
			signedURL, err := url.Parse(preSign.URL)
			require.NoError(t, err)
			assert.Equal(t, "https", signedURL.Scheme)
			assert.Equal(t, "/"+s3UnitBucket+"/folder/report file.csv", signedURL.Path)
			assert.Equal(t, testCase.expires, signedURL.Query().Get("X-Amz-Expires"))
			assert.NotEmpty(t, signedURL.Query().Get("X-Amz-Signature"))
			assert.NotEmpty(t, preSign.Header)
			assert.Equal(t, 0, requests)
		})
	}
}

func TestStorager_PresignWithoutOutputOptionUnit(t *testing.T) {
	// No signer is needed unless the caller supplies the output holder.
	storager := &Storager{}
	assert.NoError(t, storager.presign(context.Background(), "report.csv", nil))
	assert.NoError(t, storager.presign(context.Background(), "report.csv", []storage.Option{
		&PresignOptions{TimeToLive: time.Minute},
	}))
}

func TestStorager_PresignErrorUnit(t *testing.T) {
	testCases := []struct {
		name      string
		sharedTTL time.Duration
		settings  *PresignOptions
	}{
		{name: "negative shared expiry", sharedTTL: -time.Second},
		{name: "shared expiry below minimum", sharedTTL: time.Second - time.Nanosecond},
		{name: "shared expiry above maximum", sharedTTL: 7*24*time.Hour + time.Nanosecond},
		{name: "negative override expiry", settings: &PresignOptions{TimeToLive: -time.Second}},
		{name: "override expiry below minimum", settings: &PresignOptions{TimeToLive: time.Second - time.Nanosecond}},
		{name: "override expiry above maximum", settings: &PresignOptions{TimeToLive: 7*24*time.Hour + time.Nanosecond}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requests := 0
			storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
				requests++
				return nil, fmt.Errorf("unexpected HTTP request during presigning")
			})
			preSign := &option.PreSign{TimeToLive: testCase.sharedTTL}
			options := []storage.Option{preSign}
			if testCase.settings != nil {
				options = append(options, testCase.settings)
			}
			err := storager.presign(context.Background(), "report.csv", options)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to presign url, s3://"+s3UnitBucket+"/report.csv")
			assert.Contains(t, err.Error(), "expiry must be between 1 second and 7 days")
			assert.Empty(t, preSign.URL)
			assert.Empty(t, preSign.Header)
			assert.Equal(t, 0, requests)
		})
	}
}

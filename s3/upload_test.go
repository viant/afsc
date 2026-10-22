package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/storage"
)

func TestStorager_UploadContentDispositionUnit(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, disposition := range []string{"", `attachment; filename="errors_2026-10-22T12-30-00-000Z_audience.csv"`} {
			t.Run(fmt.Sprintf("streaming=%t/disposition=%t", streaming, disposition != ""), func(t *testing.T) {
				var headers http.Header
				requests := 0
				storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
					requests++
					assert.Equal(t, http.MethodPut, request.Method)
					headers = request.Header.Clone()
					if _, err := io.Copy(io.Discard, request.Body); err != nil {
						return nil, err
					}
					response := s3UnitResponse(http.StatusOK, "")
					response.Header.Set("ETag", `"unit-etag"`)
					return response, nil
				})
				meta := content.NewMeta(content.Type, "text/csv", "report-kind", "validation")
				if disposition != "" {
					meta.Values["Content-Disposition"] = disposition
				}
				options := []storage.Option{meta}
				if streaming {
					options = append(options, option.NewSkipChecksum(true), option.NewStream(5*1024*1024, 0))
				}
				const data = "Invalid email format\n3\n8\n"
				reader := io.LimitReader(strings.NewReader(data), int64(len(data)))
				require.NoError(t, storager.Upload(context.Background(), "report.csv", 0644, reader, options...))
				assert.Equal(t, 1, requests)
				assert.Equal(t, disposition, headers.Get("Content-Disposition"))
				assert.Empty(t, headers.Get("X-Amz-Meta-Content-Disposition"))
				assert.Equal(t, "text/csv", headers.Get("Content-Type"))
				assert.Equal(t, "validation", headers.Get("X-Amz-Meta-Report-Kind"))
			})
		}
	}
}

func TestStorager_UploadContextUnit(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, mode := range []string{"caller", "nil", "cancelled"} {
			t.Run(fmt.Sprintf("streaming=%t/context=%s", streaming, mode), func(t *testing.T) {
				requests := 0
				storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
					requests++
					assert.Equal(t, http.MethodPut, request.Method)
					if mode == "caller" {
						assert.Equal(t, "caller", request.Context().Value(s3UnitContextKey{}))
					}
					if _, err := io.Copy(io.Discard, request.Body); err != nil {
						return nil, err
					}
					return s3UnitResponse(http.StatusOK, ""), nil
				})
				var ctx context.Context
				switch mode {
				case "caller":
					ctx = context.WithValue(context.Background(), s3UnitContextKey{}, "caller")
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(context.Background())
					cancel()
				}
				var options []storage.Option
				if streaming {
					options = append(options, option.NewSkipChecksum(true), option.NewStream(5*1024*1024, 0))
				}
				reader := io.LimitReader(strings.NewReader("3\n8\n"), 4)
				err := storager.Upload(ctx, "report.csv", 0644, reader, options...)
				if mode == "cancelled" {
					assert.ErrorIs(t, err, context.Canceled)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, 1, requests)
				}
			})
		}
	}
}

func TestStorager_UploadMultipartContentDispositionUnit(t *testing.T) {
	const disposition = `attachment; filename="errors_audience.csv"`
	var initiated, parts, completed atomic.Int32
	storager := newS3UnitStorager(func(request *http.Request) (*http.Response, error) {
		if request.Body != nil {
			if _, err := io.Copy(io.Discard, request.Body); err != nil {
				return nil, err
			}
		}
		query := request.URL.Query()
		switch {
		case request.Method == http.MethodPost && query.Has("uploads"):
			initiated.Add(1)
			assert.Equal(t, disposition, request.Header.Get("Content-Disposition"))
			assert.Empty(t, request.Header.Get("X-Amz-Meta-Content-Disposition"))
			return s3UnitResponse(http.StatusOK, `<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>`+s3UnitBucket+`</Bucket><Key>report.csv</Key><UploadId>unit-upload</UploadId></InitiateMultipartUploadResult>`), nil
		case request.Method == http.MethodPut && query.Get("uploadId") == "unit-upload":
			parts.Add(1)
			response := s3UnitResponse(http.StatusOK, "")
			response.Header.Set("ETag", `"unit-etag"`)
			return response, nil
		case request.Method == http.MethodPost && query.Get("uploadId") == "unit-upload":
			completed.Add(1)
			return s3UnitResponse(http.StatusOK, `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>`+s3UnitBucket+`</Bucket><Key>report.csv</Key><ETag>"unit-etag"</ETag></CompleteMultipartUploadResult>`), nil
		case request.Method == http.MethodDelete && query.Get("uploadId") == "unit-upload":
			return s3UnitResponse(http.StatusNoContent, ""), nil
		default:
			return nil, fmt.Errorf("unexpected unit upload request: %s %s", request.Method, request.URL.Path)
		}
	})
	// Exceed the SDK's 16 MiB multipart threshold using a non-seekable reader.
	data := strings.Repeat("1\n", 9*1024*1024)
	reader := io.LimitReader(strings.NewReader(data), int64(len(data)))
	err := storager.Upload(context.Background(), "report.csv", 0644, reader,
		option.NewSkipChecksum(true), option.NewStream(5*1024*1024, 0),
		content.NewMeta("Content-Disposition", disposition))
	require.NoError(t, err)
	assert.EqualValues(t, 1, initiated.Load())
	assert.EqualValues(t, 4, parts.Load())
	assert.EqualValues(t, 1, completed.Load())
}

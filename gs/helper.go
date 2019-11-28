package gs

import (
	"github.com/viant/afs/base"
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
	"time"
)

const notFound = "Not Found"
const storageClassFragment = "storageclass"
const encryptionFragment = "encryption"
const backendError = "backendError"

//isRetryError returns true if backend error
func isRetryError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusServiceUnavailable {
			return true
		}
	}
	message := err.Error()
	return strings.Contains(message, backendError)
}

func isBucketNotFound(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound && strings.Contains(apiError.Message, notFound) {
			return true
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), notFound)
}

func isFallbackError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusBadRequest {
			return true
		}
	}
	errorMessage := strings.ToLower(err.Error())
	return strings.Contains(errorMessage, storageClassFragment) || strings.Contains(errorMessage, encryptionFragment)
}

func sleepBeforeRetry(retry *base.Retry) {
	time.Sleep(retry.Pause())
}

//isRetryError returns true if not found
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusNotFound {
			return true
		}
	}
	return strings.Contains(err.Error(), notFound)
}

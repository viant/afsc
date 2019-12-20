package gs

import (
	"github.com/viant/afs/base"
	"google.golang.org/api/googleapi"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const notFound = "Not Found"
const storageClassFragment = "storageclass"
const encryptionFragment = "encryption"
const backendError = "backendError"
const backendErrorCode = 10
const connectionResetCode = 11

var retryErrors = make(map[int]int)
var mux = &sync.Mutex{}

//isRetryError returns true if backend error
func isRetryError(err error) bool {
	if err == nil {
		return false
	}

	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusServiceUnavailable || apiError.Code == http.StatusGatewayTimeout {
			mux.Lock()
			retryErrors[apiError.Code]++
			mux.Unlock()
			return true
		}
	}
	message := err.Error()
	if strings.Contains(message, "connection reset") {
		mux.Lock()
		retryErrors[connectionResetCode]++
		mux.Unlock()
		return true
	}

	if strings.Contains(message, backendError) {
		mux.Lock()
		retryErrors[backendErrorCode]++
		mux.Unlock()
		return true
	}
	return false
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

func isProxyError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*url.Error)
	if ok {
		return true
	}

	return strings.Contains(err.Error(), "proxy")
}

func GetRetryCodes(reset bool) map[int]int {
	result := retryErrors
	if reset {
		mux.Lock()
		retryErrors = make(map[int]int)
		mux.Unlock()
	}
	return result
}

package gs

import (
	"github.com/viant/afs/base"
	"google.golang.org/api/googleapi"
	"net/http"
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


var errors = make(map[int]int)
var mux = &sync.Mutex{}


//isRetryError returns true if backend error
func isRetryError(err error) bool {
	if err == nil {
		return false
	}

	if apiError, ok := err.(*googleapi.Error); ok {
		mux.Lock()
		errors[apiError.Code]++
		mux.Unlock()
		if apiError.Code == http.StatusServiceUnavailable {
			return true
		}
		if apiError.Code == http.StatusGatewayTimeout {
			return true
		}
	}

	message := err.Error()
	if strings.Contains(message, "connection reset") {
		mux.Lock()
		errors[connectionResetCode]++
		mux.Unlock()
		return true
	}

	if  strings.Contains(message, backendError) {
		mux.Lock()
		errors[connectionResetCode]++
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


func GetRetryCodes(reset bool) map[int]int {
	result :=  errors
	if reset {
		mux.Lock()
		errors = make(map[int]int)
		mux.Unlock()
	}
	return result
}
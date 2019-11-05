package gs

import (
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

const notFound = "Not Found"
const storageClassFragment = "storageclass"
const encryptionFragment = "encryption"

const backendError = "backendError"

//isBackendError returns true if backend error
func isBackendError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	if message == "" {
		return false
	}
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

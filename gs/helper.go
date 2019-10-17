package gs

import (
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

const notFound = "Not Found"
const storageClass = "storageClass"

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


func isStorageClassError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusBadRequest && strings.Contains(apiError.Message, storageClass) {
			return true
		}
	}
	return strings.Contains(err.Error(), storageClass)
}
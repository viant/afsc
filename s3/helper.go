package s3

import (
	"strings"
)

const (
	noSuchBucketMessage  = "NoSuchBucket"
	missingRegionMessage = "MissingRegion"
	badRequestFragment   = "code: 400"
	encryptionFragment   = "encryption"
)

func isBucketNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), noSuchBucketMessage) || strings.Contains(err.Error(), missingRegionMessage)
}

func isFallbackError(err error) bool {
	if err == nil {
		return false
	}
	errorMessage := strings.ToLower(err.Error())
	return strings.Contains(errorMessage, badRequestFragment) || strings.Contains(errorMessage, encryptionFragment)
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), noSuchKeyMessage) || strings.Contains(err.Error(), doesNotExistsMessage)
}

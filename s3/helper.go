package s3

import (
	"strings"
)

const (
	noSuchBucketMessage  = "NoSuchBucket"
	missingRegionMessage = "MissingRegion"
)

func isBucketNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), noSuchBucketMessage) || strings.Contains(err.Error(), missingRegionMessage)
}

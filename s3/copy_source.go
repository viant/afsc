package s3

import "github.com/aws/smithy-go/encoding/httpbinding"

// encodeCopySource accepts a raw source bucket and object key and URI-encodes
// the CopySource value exactly once according to Amazon S3 rules.
func encodeCopySource(bucket, key string) string {
	return httpbinding.EscapePath(bucket+"/"+key, false)
}

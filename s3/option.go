package s3

import "time"

// PresignOptions configures expiry for S3 download URLs requested with option.PreSign.
// These settings optionally override the shared option.PreSign.TimeToLive value.
type PresignOptions struct {
	// TimeToLive overrides the shared TTL when nonzero. Zero keeps the shared TTL or 15-minute default.
	TimeToLive time.Duration
}

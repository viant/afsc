package s3

import "time"

// PresignOptions configures expiry for S3 download URLs requested with option.PreSign.
// These settings are opt-in; legacy option.PreSign.TimeToLive behavior is unchanged.
type PresignOptions struct {
	// TimeToLive overrides the 15-minute default when nonzero.
	TimeToLive time.Duration
}

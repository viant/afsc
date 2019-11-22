package gs

import (
	"context"
	"path"
	"strings"
)

//Delete removes an resource
func (s *storager) Delete(ctx context.Context, location string) (err error) {
	location = strings.Trim(location, "/")
	if location == "" {
		call := s.Buckets.Delete(s.bucket)
		call.Context(ctx)
		err = call.Do()
		return err
	}


	call := s.Objects.Delete(s.bucket, location)
	call.Context(ctx)
	for i := 0; i < maxRetries; i++ {
		err = call.Do()
		if isNotFound(err) {
			notFound := err
			infoList, err := s.List(ctx, location)
			if err != nil {
				return err
			}
			if len(infoList) > 1 {
				for i := 1; i < len(infoList); i++ {
					if err = s.Delete(ctx, path.Join(location, infoList[i].Name())); err != nil {
						return err
					}
				}
				return nil
			} else {
				return notFound
			}
		}
		if !isRetryError(err) {
			return err
		}
		sleepBeforeRetry()
	}
	return err
}

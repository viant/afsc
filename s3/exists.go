package s3

import (
	"context"
	"strings"
)

//Exists returns true if object exists
func (s *storager) Exists(ctx context.Context, location string) (bool, error) {
	location = strings.Trim(location, "/")
	list, err := s.List(ctx, location)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "not found") {
		err = nil
	}
	return len(list) > 0, err

}

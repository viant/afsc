package gs

import (
	"context"
	"strings"
)

//Exists returns true if object exists
func (s *storager) Exists(ctx context.Context, location string) (bool, error) {
	object, err := s.Get(ctx, location)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "not found") {
		err = nil
	}
	return object != nil, err
}

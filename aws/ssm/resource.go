package ssm

import (
	"fmt"
	"strings"
)

var errInvalidFormat = fmt.Errorf("invalid resource format: expected /[region]/parameter/[parameter]")

//Resource represent secret resource
type Resource struct {
	Region string
	Name   string
}

func newResource(resourceID string) (*Resource, error) {
	var result = &Resource{}
	fragments := strings.Split(strings.Trim(resourceID, "/"), "/")
	switch len(fragments) {
	case 3:
		result.Name = fragments[2]
		fallthrough
	case 2:
		if fragments[1] != "parameter" {
			return nil, errInvalidFormat
		}
		result.Region = fragments[0]
	case 0, 1:
		return nil, errInvalidFormat
	default:
		return nil, errInvalidFormat
	}
	return result, nil
}

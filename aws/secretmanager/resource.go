package secretmanager

import (
	"fmt"
	"strings"
)

var errInvalidFormat = fmt.Errorf("invalid resource format: expected /[region]/secret/[secretURI]")

//Resource represent secret resource
type Resource struct {
	Region string
	Secret string
}

func newResource(resourceID string) (*Resource, error) {
	var result = &Resource{}
	fragments := strings.Split(strings.Trim(resourceID, "/"), "/")
	if len(fragments) <= 2 {
		return nil, errInvalidFormat
	}
	result.Region = fragments[0]
	if fragments[1] != "secret" {
		return nil, errInvalidFormat
	}
	result.Secret = strings.Join(fragments[2:], "/")
	return result, nil
}

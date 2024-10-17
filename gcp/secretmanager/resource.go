package secretmanager

import (
	"fmt"
	"strings"
)

var errInvalidFormat = fmt.Errorf("invalid resource format: expected /projects/projectID/secrets/[secret]/versions/[version] ")

// Resource represent secret resource
type Resource struct {
	ProjectID string
	Secret    string
	Version   string
}

// Name returns resource name
func (r *Resource) Name() string {
	return fmt.Sprintf("projects/%v/secrets/%v", r.ProjectID, r.Secret)
}

// VersionedName return versioned name
func (r *Resource) VersionedName() string {
	version := r.Version
	if version == "" {
		version = "latest"
	}
	return fmt.Sprintf("projects/%v/secrets/%v/versions/%v", r.ProjectID, r.Secret, version)
}

func newResource(resourceID string) (*Resource, error) {
	var result = &Resource{}
	fragments := strings.Split(strings.Trim(resourceID, "/"), "/")
	switch len(fragments) {
	case 6:
		result.Version = fragments[5]
		fallthrough
	case 5:
		if fragments[4] != "versions" {
			return nil, errInvalidFormat
		}
		fallthrough
	case 4:
		result.Secret = fragments[3]
		fallthrough
	case 3:
		if fragments[2] != "secrets" {
			return nil, errInvalidFormat
		}
		fallthrough
	case 2:
		result.ProjectID = fragments[1]
		if fragments[0] != "projects" {
			return nil, errInvalidFormat
		}
	case 0, 1:
		return nil, errInvalidFormat
	}
	return result, nil
}

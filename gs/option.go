package gs

import "google.golang.org/api/option"

//ClientOptions represents gcp client options
type ClientOptions []option.ClientOption

//NewClientOptions creates an option slice
func NewClientOptions(options ...option.ClientOption) ClientOptions {
	return options
}

//Project represents a project info
type Project struct {
	ID string
}

//NewProject returns a project option
func NewProject(id string) *Project {
	return &Project{ID: id}
}

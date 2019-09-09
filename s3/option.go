package s3

//Region represents an AWS region option
type Region struct {
	Name string
}

//NewRegion creates a region for specified name
func NewRegion(name string) *Region{
	return &Region{Name:name}
}
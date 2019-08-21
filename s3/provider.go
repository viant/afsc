package s3

import "github.com/viant/afs/storage"

//Provider returns a google storage manager
func Provider(options ...storage.Option) (storage.Manager, error) {
	return New(options...), nil
}

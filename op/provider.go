package op

import "github.com/viant/afs/storage"

// Provider returns a 1Password secret manager.
func Provider(options ...storage.Option) (storage.Manager, error) {
	return New(options...), nil
}

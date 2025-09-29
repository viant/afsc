package openai

import "github.com/viant/afs/storage"

// Provider returns an OpenAI storage manager
func Provider(options ...storage.Option) (storage.Manager, error) {
	return New(options...), nil
}

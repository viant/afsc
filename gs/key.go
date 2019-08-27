package gs

import (
	"github.com/viant/afs/option"
	"net/http"
)

//SetCustomKeyHeader set encryption headers
func SetCustomKeyHeader(key *option.AES256Key, headers http.Header) error {
	err := key.Init()
	if err == nil {
		err = key.Validate()
	}
	if err != nil {
		return err
	}
	headers.Set("x-goog-encryption-algorithm", "AES256")
	headers.Set("x-goog-encryption-key", key.Base64Key)
	headers.Set("x-goog-encryption-key-sha256", key.Base64KeySha256Hash)
	return nil
}

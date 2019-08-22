package gs

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
)

//CustomKey represents AES-256 key
type CustomKey struct {
	Key       []byte
	Base64Key string
}

//SetHeader set encryption headers
func (k CustomKey) SetHeader(headers http.Header) error {
	if len(k.Key) != 32 {
		return fmt.Errorf("%s: not a 32-byte AES-256 key", k.Key)
	}
	headers.Set("x-goog-encryption-algorithm", "AES256")
	headers.Set("x-goog-encryption-key", k.Base64Key)
	keyHash := sha256.Sum256(k.Key)
	headers.Set("x-goog-encryption-key-sha256", base64.StdEncoding.EncodeToString(keyHash[:]))
	return nil
}

//NewCustomKey returns new key
func NewCustomKey(key []byte) *CustomKey {
	return &CustomKey{Key: key, Base64Key: base64.StdEncoding.EncodeToString(key)}
}

//NewBase64CustomKey create a CustomKey from base64 encoded key
func NewBase64CustomKey(base64Key string) (*CustomKey, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, err
	}
	return &CustomKey{
		Key:       key,
		Base64Key: base64Key,
	}, nil
}

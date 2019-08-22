package gs

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
)

//AES256Key represents AES-256 key
type AES256Key struct {
	Key       []byte
	Base64Key string
}

//Set header
func (k AES256Key) SetHeader(headers http.Header) error {
	if len(k.Key) != 32 {
		return fmt.Errorf("%s: not a 32-byte AES-256 key", k.Key)
	}
	headers.Set("x-goog-encryption-algorithm", "AES256")
	headers.Set("x-goog-encryption-key", k.Base64Key)
	keyHash := sha256.Sum256(k.Key)
	headers.Set("x-goog-encryption-key-sha256", base64.StdEncoding.EncodeToString(keyHash[:]))
	return nil
}

//NewKey returns new key
func NewAES256Key(key []byte) *AES256Key {
	return &AES256Key{Key: key, Base64Key: base64.StdEncoding.EncodeToString(key)}
}

//NewBase64AES256Key create a AES256Key from base64 encoded key
func NewBase64AES256Key(base64Key string) (*AES256Key, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, err
	}
	return &AES256Key{
		Key:       key,
		Base64Key: base64Key,
	}, nil
}

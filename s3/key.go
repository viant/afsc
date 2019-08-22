package s3

import (
	"crypto/md5"
	"encoding/base64"
)

//CustomKey represents AES-256 key
type CustomKey struct {
	Key              []byte
	Base64KeyMd5Hash string
}

//NewCustomKey returns new key
func NewCustomKey(key []byte) *CustomKey {
	keyHash := md5.New()
	keyHash.Write(key)
	return &CustomKey{
		Key:              key,
		Base64KeyMd5Hash: base64.StdEncoding.EncodeToString(keyHash.Sum(nil)),
	}
}

//NewBase64CustomKey create a CustomKey from base64 encoded key
func NewBase64CustomKey(base64Key string) (*CustomKey, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, err
	}
	keyHash := md5.New()
	keyHash.Write(key)
	return &CustomKey{
		Key:              key,
		Base64KeyMd5Hash: base64.StdEncoding.EncodeToString(keyHash.Sum(nil)),
	}, nil
}

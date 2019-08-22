package s3

import (
	"crypto/md5"
	"encoding/base64"
)

//AES256Key represents AES-256 key
type AES256Key struct {
	Key           []byte
	Base64KeyHash string
}

//NewKey returns new key
func NewAES256Key(key []byte) *AES256Key {
	keyHash := md5.New()
	keyHash.Write(key)
	return &AES256Key{
		Key:           key,
		Base64KeyHash: base64.StdEncoding.EncodeToString(keyHash.Sum(nil)),
	}
}

//NewBase64AES256Key create a AES256Key from base64 encoded key
func NewBase64AES256Key(base64Key string) (*AES256Key, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, err
	}
	keyHash := md5.New()
	keyHash.Write(key)
	return &AES256Key{
		Key:           key,
		Base64KeyHash: base64.StdEncoding.EncodeToString(keyHash.Sum(nil)),
	}, nil
}

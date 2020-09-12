package s3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"io/ioutil"
	"strings"
	"testing"
)

func TestAES256Key_SetHeader(t *testing.T) {
	authConfig, err := NewTestAuthConfig()
	if err != nil {
		fmt.Printf("skip:%v\n", err)
		t.Skip(err)
		return

	}
	ctx := context.Background()
	var useCases = []struct {
		description string
		URL         string
		location    string
		data        []byte
		key         string
		base64Key   string
	}{

		{
			description: "securing data with key",
			key:         strings.Repeat("xd", 16),
			location:    "custom/header/secret1.txt",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			data:        []byte("this is test 1"),
		},

		{
			description: "securing data with base64key",
			location:    "custom/header/secret2.txt",
			URL:         fmt.Sprintf("s3://%v/", TestBucket),
			data:        []byte("this is test 2"),
			base64Key:   "eGR4ZHhkeGR4ZHhkeGR4ZHhkeGR4ZHhkeGR4ZHhkeGQ=",
		},
	}

	mgr := New(authConfig)

	defer func() {
		_ = mgr.Delete(ctx, fmt.Sprintf("s3://%v/", TestBucket))
	}()
	for _, useCase := range useCases {
		fmt.Printf("%v\n", useCase.description)
		var key *option.AES256Key
		if useCase.key != "" {
			key, err = option.NewAES256Key([]byte(useCase.key))
			assert.Nil(t, err, useCase.description)
		} else {
			key, err = option.NewBase64AES256Key(useCase.base64Key)
			assert.Nil(t, err, useCase.description)
		}

		URL := url.Join(useCase.URL, useCase.location)
		err := mgr.Upload(ctx, URL, 0644, bytes.NewReader(useCase.data), key)
		assert.Nil(t, err, useCase.description)

		reader, err := mgr.OpenURL(ctx, URL, key)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		data, err := ioutil.ReadAll(reader)
		assert.EqualValues(t, useCase.data, data, useCase.description)

		_, err = mgr.OpenURL(ctx, URL)
		assert.NotNil(t, err, useCase.description)

	}

}

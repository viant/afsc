# afsc - Abstract File Storage Connectors

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/afsc)](https://goreportcard.com/report/github.com/viant/afsc)
[![GoDoc](https://godoc.org/github.com/viant/afsc?status.svg)](https://godoc.org/github.com/viant/afsc)


This library is compatible with Go 1.11+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#usage)
- [Connectors](#connectors)
    * [GCP - Google Storage](gs)
    * [AWS - S3](s3)
- [GoCover](#gocover)
- [License](#license)
- [Credits and Acknowledgements](#credits-and-acknowledgements)

This project provides various implementation for [Abstract File Storage](https://github.com/viant/afs)

## Usage

```go

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	"io/ioutil"
	"log"
)

func ExampleNew() {
	service := afs.New()
	ctx := context.Background()
	objects, err := service.List(ctx, "gs://myBucket/folder")
	if err != nil {
		log.Fatal(err)
	}
	for _, object := range objects {
		fmt.Printf("%v %v\n", object.Name(), object.URL())
		if object.IsDir() {
			continue
		}
		reader, err := service.Download(ctx, object)
		if err != nil {
			log.Fatal(err)
		}
		defer reader.Close()
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", data)
	}

	err = service.Copy(ctx, "gs://myBucket/folder", "s3://myBucket/cloned")
	if err != nil {
		log.Fatal(err)
	}
}

```

- Uploading/downloading with secure key

```go
func ExampleNewAES256Key() {
	customKey := s3.NewAES256Key([]byte("secret-key-that-is-32-bytes-long"))
	ctx := context.Background()
	service := afs.New()
	err := service.Upload(ctx, "s3://mybucket/folder/secret1.txt", 0644, strings.NewReader("my secret text"), customKey)
	if err != nil {
		log.Fatal(err)
	}
	reader, err := service.DownloadWithURL(ctx, "s3://mybucket/folder/secret1.txt", customKey)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
}
```

## Connectors 

- [GCP Google Storage](gs)
- [AWS - S3](s3)

## GoCover

[![GoCover](https://gocover.io/github.com/viant/afsc)](https://gocover.io/github.com/viant/afsc)

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Author:** Adrian Witas


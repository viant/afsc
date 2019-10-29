# s3 - Amazon Web Service s3 for Abstract File Storage

## Usage

```go

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/s3"
	"io/ioutil"
	"log"
)

func main() {
	service := afs.New()
	ctx := context.Background()
	objects, err := service.List(ctx, "s3://myBucket/folder")
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
	
	err = service.Copy(ctx, "s3://myBucket/folder", "/tmp")
	if err != nil {
		log.Fatal(err)
	}
}


```

### Auth



- **[AwsConfigProvider](auth.go)**

To use auth provider, provide a type that implements the following interface, you can also use [s3.NewAuthConfig](auth.go)  

```go
type AwsConfigProvider interface {
	AwsConfig() (*aws.Config, error)
}

``` 

_Example:_
```go
    
    authConfig, err := s3.NewAuthConfig(option.NewLocation("credetnialsfile"))
    if err != nil {
		log.Fatal(err)
	}

	service := afs.New()
	reader, err := service.DownloadWithURL(ctx, "s3://my-bucket/myfolder/asset.txt", authConfig)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
	

```

## Options

- [Md5](https://github.com/viant/afs/blob/master/option/md5.go): when uploading content with this option, supplied option is used for Put ContentMD5, otherwise
md5 is computed for supplied content.

- aws.Config: s3 client.

- Region: s3 client region.

- Custom key encryption

```go
   customKey, err := option.NewAES256Key([]byte("secret-key-that-is-32-bytes-long"))
   	if err != nil {
   		log.Fatal(err)
   	}
   	ctx := context.Background()
   	service := afs.New()
   	err = service.Upload(ctx, "s3://mybucket/folder/secret1.txt", 0644, strings.NewReader("my secret text"), customKey)
   	if err != nil {
   		log.Fatal(err)
   	}
   	reader, err := service.DownloadWithURL(ctx, "s3://mybucket/folder/secret1.txt", customKey)
   	data, err := ioutil.ReadAll(reader)
   	if err != nil {
   		log.Fatal(err)
   	}
   	fmt.Printf("data: %s\n", data)
```


- option.Checksum{Skip:true}: checksum (md5) is not computed to stream data in chunks
- option.Stream: download reader reads data with specified stream PartSize 


```go
	_ = os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	ctx := context.Background()
	fs := afs.New()
	sourceURL := "s3://myBucket/path/myasset.gz"
	reader, err := fs.DownloadWithURL(ctx, sourceURL, option.NewStream(64*1024*1024, 0))
	if err != nil {
		log.Fatal(err)
	}
	jwtConfig, err := gs.NewJwtConfig()
	if err != nil {
		log.Fatal(err)
	}
	destURL := "gs://myBucket/path/myasset.gz"
	err = fs.Upload(ctx, destURL, 0644, reader, jwtConfig, &option.Checksum{Skip:true})
	if err != nil {
		log.Fatal(err)
		return
	}


```
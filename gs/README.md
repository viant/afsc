# gs - Google Storage for Abstract File Storage

## Usage

```go

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/gs"
	"io/ioutil"
	"log"
)

func main() {
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
	
	err = service.Copy(ctx, "gs://myBucket/folder", "/tmp")
	if err != nil {
		log.Fatal(err)
	}
}


```

### Auth

- **Default Google Auth**

When this library is used within GCP then owner service account is used to authenticate.  
Otherwise use case use [GOOGLE_APPLICATION_CREDENTIALS](https://cloud.google.com/docs/authentication/production) 



- **JSON Web Token (JWT)**

To use JWT auth, provide a type that implements the following interface, you can also use [gs.NewJwtConfig](jwt.go)  

```go
type JWTProvider interface {

	JWTConfig(scopes ...string) (config *jwt.Config, projectID string, err error)
}

``` 

_Example:_
```go
    secretPath := path.Join(os.Getenv("HOME"), ".secret", TestCredenitlas)
	jwtConfig, err := gs.NewJwtConfig(option.NewLocation(secretPath))
	if err != nil {
		log.Fatal(err)
	}

	service := afs.New()
	reader, err := service.DownloadWithURL(ctx, "gs://my-bucket/myfolder/asset.txt", jwtConfig)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
	

```

## Options

- [Client Options](option.go)

```go

    secretPath := path.Join(os.Getenv("HOME"), ".secret", "gcp-e2e.json")
	jwtConfig, err := gs.NewJwtConfig(option.NewLocation(secretPath))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	JSON, err := json.Marshal(jwtConfig)
	
    //import goption "google.golang.org/api/option"
	jsonAuth := goption.WithCredentialsJSON(JSON)

	service := afs.New()
	reader, err := service.DownloadWithURL(ctx, "gs://my-bucket/myfolder/asset.txt", gs.NewClientOptions(jsonAuth), gs.NewProject("myproject"))
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)

```

- Custom key encryption

```go

  	customKey, err := option.NewAES256Key([]byte("secret-key-that-is-32-bytes-long"))
  	if err != nil {
  		log.Fatal(err)
  	}
  	ctx := context.Background()
  	service := afs.New()
  	err = service.Upload(ctx, "gs://mybucket/folder/secret1.txt", 0644, strings.NewReader("my secret text"), customKey)
  	if err != nil {
  		log.Fatal(err)
  	}
  	reader, err := service.DownloadWithURL(ctx, "gs://mybucket/folder/secret1.txt", customKey)
  	data, err := ioutil.ReadAll(reader)
  	if err != nil {
  		log.Fatal(err)
  	}
  	fmt.Printf("data: %s\n", data)


```


- option.Checksum{Skip:true}: checksum (crc/md5) is not computed to stream data in chunks
- option.Stream: download reader reads data with specified stream PartSize 


```go

    jwtConfig, err := gs.NewJwtConfig()
	if err != nil {
		log.Fatal(err)
	}
	
	
	ctx := context.Background()
	fs := afs.New()
	sourceURL := "gs://myBucket/path/myasset.gz"
	reader, err := fs.DownloadWithURL(ctx, sourceURL, jwtConfig, option.NewStream(64*1024*1024, 0))
	if err != nil {
		log.Fatal(err)
	}
    
	_ = os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	destURL := "s3://myBucket/path/myasset.gz"
	err = fs.Upload(ctx, destURL, 0644, reader, &option.Checksum{Skip:true})
	if err != nil {
		log.Fatal(err)
		return
	}


```
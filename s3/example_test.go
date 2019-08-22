package s3_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afsc/gs"
	"github.com/viant/afsc/s3"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
)

func ExampleAfsService() {
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
}

func ExampleNew() {
	service := gs.New()
	ctx := context.Background()
	reader, err := service.DownloadWithURL(ctx, "s3://my-bucket/folder/asset")
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
}

//Example_Storager storager usage example (uses path rather then URLs)
func Example_Storager() {

	ctx := context.Background()
	service, err := gs.NewStorager(ctx, "s3://myBucket/")
	if err != nil {
		log.Fatal(err)
	}
	location := "/myFolder/myfile"
	err = service.Upload(ctx, location, 0644, []byte("somedata"))
	if err != nil {
		log.Fatal(err)
	}
	reader, err := service.Download(ctx, location)
	if err != nil {
		log.Fatal(err)
	}
	data, err := ioutil.ReadAll(reader)
	fmt.Printf("data: %s\n", data)

	has, _ := service.Exists(ctx, location)
	fmt.Printf("%v %v", location, has)

	files, err := service.List(ctx, location, 0, 3)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		fmt.Printf("file: %v\n", file.Name())
	}

	err = service.Delete(ctx, location)
	if err != nil {
		log.Fatal(err)
	}

}

func ExampleNewAuthConfig() {

	authConfig, err := s3.NewAuthConfig(option.NewLocation(path.Join(os.Getenv("HOME"), ".aws/credentials")))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	//add default import _ "github.com/viant/afsc/s3"

	service := afs.New()
	reader, err := service.DownloadWithURL(ctx, "s3://my-bucket/myfolder/asset.txt", authConfig)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)

}

func ExampleAwsConfig() {
	var awsConfig *aws.Config
	//get config
	ctx := context.Background()
	service := afs.New()
	reader, err := service.DownloadWithURL(ctx, "s3://my-bucket/myfolder/asset.txt", awsConfig)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
}

func ExampleNewCustomKey() {
	customKey := gs.NewCustomKey([]byte("secret-key-that-is-32-bytes-long"))
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

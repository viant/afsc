package gs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afsc/gs"
	goption "google.golang.org/api/option"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
)

func ExampleAfsService() {
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
		reader, err := service.Open(ctx, object)
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
	reader, err := service.OpenURL(ctx, "gs://my-bucket/folder/asset")
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
	service, err := gs.NewStorager(ctx, "gs://myBucket/")
	if err != nil {
		log.Fatal(err)
	}
	location := "/myFolder/myfile"
	err = service.Upload(ctx, location, 0644, strings.NewReader("somedata"))
	if err != nil {
		log.Fatal(err)
	}
	reader, err := service.Open(ctx, location)
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

func ExampleNewJwtConfig() {

	ctx := context.Background()
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "gcp-e2e.json")
	jwtConfig, err := gs.NewJwtConfig(option.NewLocation(secretPath))
	if err != nil {
		log.Fatal(err)

	}
	//add default import _ "github.com/viant/afsc/gs"

	service := afs.New()
	reader, err := service.OpenURL(ctx, "gs://my-bucket/myfolder/asset.txt", jwtConfig)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)

}

func ExampleNewClientOptions() {
	secretPath := path.Join(os.Getenv("HOME"), ".secret", "gcp-e2e.json")
	jwtConfig, err := gs.NewJwtConfig(option.NewLocation(secretPath))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	JSON, err := json.Marshal(jwtConfig)
	jsonAuth := goption.WithCredentialsJSON(JSON)

	service := afs.New()
	reader, err := service.OpenURL(ctx, "gs://my-bucket/myfolder/asset.txt", gs.NewClientOptions(jsonAuth), gs.NewProject("myproject"))
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
}

func ExampleNewCustomKey() {
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
	reader, err := service.OpenURL(ctx, "gs://mybucket/folder/secret1.txt", customKey)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("data: %s\n", data)
}

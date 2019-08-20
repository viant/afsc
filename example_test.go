package afsc

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/gs"
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

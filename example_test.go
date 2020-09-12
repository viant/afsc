package afsc

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
	fs := afs.New()
	ctx := context.Background()
	objects, err := fs.List(ctx, "gs://myBucket/folder")
	if err != nil {
		log.Fatal(err)
	}
	for _, object := range objects {
		fmt.Printf("%v %v\n", object.Name(), object.URL())
		if object.IsDir() {
			continue
		}
		reader, err := fs.Open(ctx, object)
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
	err = fs.Copy(ctx, "gs://myBucket/folder", "s3://myBucket/cloned")
	if err != nil {
		log.Fatal(err)
	}
}

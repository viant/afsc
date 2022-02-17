package ssm_test

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	_ "github.com/viant/afsc/aws"
	"log"
	"strings"
)

func Example_DownloadWithURL() {
	fs := afs.New()
	URL := "aws://ssm/us-west-1/parameter/myParamX"
	err := fs.Upload(context.TODO(), URL, file.DefaultFileOsMode, strings.NewReader("test is super secret"))
	if err != nil {
		log.Fatalf("err: %v\n", err)
	}
	data, err := fs.DownloadWithURL(context.TODO(), URL)
	if err != nil {
		log.Fatalf("err: %v\n", err)
	}
	fmt.Printf("%s %v\n", data, err)
}

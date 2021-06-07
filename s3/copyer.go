package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"sort"
	"strconv"
	"sync"
)

const (
	multiCopyThreadsKey = "AWS_MCOPY_CONCURRENCY"
	multiCopyDebug      = "AWS_MCOPY_DEBUG"
)

type copyer struct {
	*s3.S3
	partSize  int64
	pos       int64 // current reader position
	totalSize int64 // set to -1 if the size is not known
	parts     int64
	uploadID  string
	in        *s3.CopyObjectInput
}

func (c *copyer) copy(ctx context.Context) error {
	err := c.initCopy(ctx)
	if err != nil {
		return err
	}
	debug := os.Getenv(multiCopyDebug) == "true"

	if c.uploadID == "" {
		return fmt.Errorf("invalid upload id: %v", c.uploadID)
	}
	if debug {
		fmt.Printf("multiCopy(%v): %s -> %s/%s\n", c.parts, *c.in.CopySource, *c.in.Bucket, *c.in.Key)
	}

	routines := 10
	if value := os.Getenv(multiCopyThreadsKey); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			routines = intValue
		}
	}
	wg := sync.WaitGroup{}
	var rateLimit = make(chan bool, routines)
	wg.Add(int(c.parts))
	mux := sync.Mutex{}
	parts := CompletedParts{}
	for i := 0; i < int(c.parts); i++ {
		start := i * int(c.partSize)
		finish := ((i + 1) * int(c.partSize)) - 1
		if finish >= int(c.totalSize) {
			finish = int(c.totalSize) - 1
		}
		part := int64(i)

		go func(start, finish int, part int64) {
			rateLimit <- true
			defer func() {
				wg.Done()
				<-rateLimit
			}()
			if debug {
				fmt.Printf("multiCopy[%v]: bytes=%d-%d\n", part, start, finish)
			}
			params := &s3.UploadPartCopyInput{
				Bucket:               c.in.Bucket,
				Key:                  c.in.Key,
				CopySource:           c.in.CopySource,
				CopySourceRange:      aws.String(fmt.Sprintf("bytes=%d-%d", start, finish)),
				UploadId:             &c.uploadID,
				SSECustomerAlgorithm: c.in.SSECustomerAlgorithm,
				SSECustomerKey:       c.in.SSECustomerKey,
				PartNumber:           &part,
			}
			output, e := c.S3.UploadPartCopyWithContext(ctx, params)
			if e != nil {
				if debug {
					fmt.Printf("multiCopy: chunk upload error  %v\n", err)
				}
				err = e

			}
			if e == nil {
				mux.Lock()
				parts = append(parts, &s3.CompletedPart{
					ETag:       output.CopyPartResult.ETag,
					PartNumber: &part,
				})
				mux.Unlock()
			}

		}(start, finish, part+1)
	}
	wg.Wait()
	if err == nil {
		sort.Sort(parts)
		err = c.complete(ctx, parts)
	}
	if debug {
		fmt.Printf("multiCopy: upload completed %v, err: %v\n", c.in.CopySource, err)
	}
	return err
}

func (c *copyer) complete(ctx context.Context, parts CompletedParts) error {
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          c.in.Bucket,
		Key:             c.in.Key,
		UploadId:        &c.uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
	}
	_, err := c.S3.CompleteMultipartUploadWithContext(ctx, params)
	return err
}

func (c *copyer) initCopy(ctx context.Context) error {
	multipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket: c.in.Bucket,
		Key:    c.in.Key,
	}
	multipartUploadOuput, err := c.S3.CreateMultipartUploadWithContext(ctx, multipartUploadInput)
	if err != nil {
		return err
	}
	c.uploadID = *multipartUploadOuput.UploadId
	return nil
}

func newCopyer(client *s3.S3, info os.FileInfo, partSize int64, input *s3.CopyObjectInput) *copyer {
	return &copyer{
		S3:        client,
		in:        input,
		partSize:  partSize,
		totalSize: info.Size(),
		parts:     (info.Size() / partSize) + 1,
	}
}

type CompletedParts []*s3.CompletedPart

func (a CompletedParts) Len() int           { return len(a) }
func (a CompletedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CompletedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

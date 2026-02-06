package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
)

type reader struct {
	from   int64
	size   int
	input  *s3.GetObjectInput
	client *s3.Client
	ctx    context.Context
	writer *Writer
}

func (t *reader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("unsupported whence: %v", whence)
	}
	if int(offset) > t.size {
		return 0, io.EOF
	}
	t.from = offset
	return 0, nil
}

func (t *reader) Read(dest []byte) (int, error) {
	if int(t.from) > t.size {
		return 0, errors.Errorf("index out of bound: %v, size: %v", t.from, t.size)
	}
	from := t.from
	to := int(t.from) + len(dest) - 1
	if to > t.size {
		to = t.size
	}

	rangeLiteral := fmt.Sprintf(base.RangeHeaderTmpl, from, to)
	t.input.Range = &rangeLiteral

	resp, err := t.client.GetObject(t.ctx, t.input)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Read up to len(dest). S3 should return exactly the requested range,
	// but we handle short reads defensively.
	n, readErr := io.ReadFull(resp.Body, dest)
	if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
		// Partial is fine at end-of-object/range.
		readErr = nil
	}
	if readErr != nil {
		return n, readErr
	}

	t.from += int64(n)
	return n, nil
}

// NewReadSeeker create a reader seeker
func NewReadSeeker(ctx context.Context, input *s3.GetObjectInput, client *s3.Client, partSize, size int) io.ReadSeeker {
	return &reader{
		ctx:    ctx,
		writer: NewWriter(partSize),
		input:  input,
		client: client,
		size:   size,
	}
}

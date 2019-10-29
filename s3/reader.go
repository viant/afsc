package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"io"
)

type reader struct {
	from       int64
	size       int
	input      *s3.GetObjectInput
	downloader *s3manager.Downloader
	ctx        context.Context
	writer     *Writer
}

func (t *reader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("usupported whence: %v", whence)
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
	t.writer.Reset()
	_, err := t.downloader.DownloadWithContext(t.ctx, t.writer, t.input)
	if err != nil {
		return 0, err
	}
	copied := copy(dest, t.writer.Bytes())
	return copied, nil
}

//NewReadSeeker create a reader seeker
func NewReadSeeker(ctx context.Context, input *s3.GetObjectInput, downloader *s3manager.Downloader, partSize, size int) io.ReadSeeker {
	return &reader{
		ctx:        ctx,
		writer:     NewWriter(partSize),
		input:      input,
		downloader: downloader,
		size:       size,
	}
}

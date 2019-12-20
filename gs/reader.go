package gs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/base"
	"google.golang.org/api/storage/v1"
	"io"
	"net/http"
)

type reader struct {
	from     int64
	size     int
	call     *storage.ObjectsGetCall
	storager *storager
	ctx      context.Context
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
	t.call.Header().Set(base.RangeHeader, fmt.Sprintf(base.RangeHeaderTmpl, from, to))

	var response *http.Response
	var err error
	err = runWithRetries(t.ctx, func() error {
		response, err = t.call.Download()
		return err
	}, t.storager)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	readSoFar := 0
	for {
		read, err := response.Body.Read(dest[readSoFar:])
		if err != nil {
			return 0, err
		}
		if read == 0 {
			break
		}
		readSoFar += read
		if readSoFar >= len(dest) {
			break
		}
	}
	return readSoFar, nil
}

//NewReadSeeker create a reader seeker
func NewReadSeeker(ctx context.Context, storager *storager, call *storage.ObjectsGetCall, size int) io.ReadSeeker {
	return &reader{
		ctx:      ctx,
		storager: storager,
		call:     call,
		size:     size,
	}
}

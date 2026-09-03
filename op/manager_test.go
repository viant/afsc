package op

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/file"
)

func TestManagerCopyMoveUnsupported(t *testing.T) {
	m := newManager()

	err := m.Copy(context.Background(), "op://Private/a", "op://Private/b")
	assert.ErrorIs(t, err, errUnsupported)

	err = m.Move(context.Background(), "op://Private/a", "op://Private/b")
	assert.ErrorIs(t, err, errUnsupported)
}

func TestUploadCreateUnsupported(t *testing.T) {
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: &stubCLI{}})
	if err != nil {
		t.Fatal(err)
	}

	uploadErr := storager.Upload(context.Background(), "/a", file.DefaultFileOsMode, bytes.NewReader(nil))
	assert.ErrorIs(t, uploadErr, errUnsupported)

	createErr := storager.Create(context.Background(), "/a", file.DefaultFileOsMode, bytes.NewReader(nil), false)
	assert.ErrorIs(t, createErr, errUnsupported)
}

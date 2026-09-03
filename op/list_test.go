package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte("secret-value"),
		},
	}
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	list, err := storager.List(context.Background(), "/e2e-account.json/notesPlain")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "/e2e-account.json/notesPlain", list[0].Name())
	assert.Equal(t, int64(len("secret-value")), list[0].Size())
	assert.False(t, list[0].IsDir())

	_, err = storager.List(context.Background(), "/missing/item/field")
	assert.Error(t, err)
}

func TestGet(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte("secret-value"),
		},
	}
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	info, err := storager.Get(context.Background(), "/e2e-account.json/notesPlain")
	require.NoError(t, err)
	assert.Equal(t, int64(len("secret-value")), info.Size())

	_, err = storager.Get(context.Background(), "/missing/item/field")
	assert.Error(t, err)
}

func TestClose(t *testing.T) {
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: &stubCLI{}})
	require.NoError(t, err)
	assert.NoError(t, storager.Close())
}

func TestDeleteUnsupported(t *testing.T) {
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: &stubCLI{}})
	require.NoError(t, err)

	err = storager.Delete(context.Background(), "/e2e-account.json/notesPlain")
	assert.ErrorIs(t, err, errUnsupported)
}

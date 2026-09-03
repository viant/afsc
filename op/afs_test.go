package op

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
)

func TestAFSDownloadWithURL(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte(`{"type":"service_account"}`),
		},
	}
	fs := afs.New()
	err := fs.Init(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	data, err := fs.DownloadWithURL(context.Background(), "op://Private/e2e-account.json/notesPlain", &CLIOption{CLI: cli})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"service_account"}`, string(data))
}

func TestAFSOpenURL(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte("secret-value"),
		},
	}
	fs := afs.New()
	err := fs.Init(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	reader, err := fs.OpenURL(context.Background(), "op://Private/e2e-account.json/notesPlain", &CLIOption{CLI: cli})
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "secret-value", string(data))
}

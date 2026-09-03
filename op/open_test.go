package op

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubCLI struct {
	secrets map[string][]byte
}

func (s *stubCLI) Read(ctx context.Context, reference string) ([]byte, error) {
	data, ok := s.secrets[reference]
	if !ok {
		return nil, fmt.Errorf("secret not found: %s", reference)
	}
	return data, nil
}

func TestOpen(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte(`{"type":"service_account"}`),
		},
	}
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	reader, err := storager.Open(context.Background(), "/e2e-account.json/notesPlain")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"service_account"}`, string(data))
}

func TestDownloadWithURL(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte("secret-value"),
		},
	}
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	reader, err := storager.Open(context.Background(), "/e2e-account.json/notesPlain")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "secret-value", string(data))
}

func TestExists(t *testing.T) {
	cli := &stubCLI{
		secrets: map[string][]byte{
			"op://Private/e2e-account.json/notesPlain": []byte("secret-value"),
		},
	}
	storager, err := NewStorager(context.Background(), "op://Private", &CLIOption{CLI: cli})
	require.NoError(t, err)

	ok, err := storager.Exists(context.Background(), "/e2e-account.json/notesPlain")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = storager.Exists(context.Background(), "/missing/item/field")
	require.NoError(t, err)
	assert.False(t, ok)
}

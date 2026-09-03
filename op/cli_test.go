package op

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fakeOPPath(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(filename), "testdata", "fake-op.sh")
}

func TestNewCLI_DefaultBinary(t *testing.T) {
	t.Setenv(cliEnvVar, "")
	cli, ok := NewCLI().(*commandCLI)
	require.True(t, ok)
	assert.Equal(t, "op", cli.binary)
}

func TestNewCLI_EnvOverride(t *testing.T) {
	t.Setenv(cliEnvVar, "/custom/path/to/op")
	cli, ok := NewCLI().(*commandCLI)
	require.True(t, ok)
	assert.Equal(t, "/custom/path/to/op", cli.binary)
}

func TestCommandCLI_Read_Success(t *testing.T) {
	t.Setenv(cliEnvVar, fakeOPPath(t))
	cli := NewCLI()

	data, err := cli.Read(context.Background(), "op://Private/e2e-account.json/notesPlain")
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"service_account","project_id":"test"}`, string(data))
}

func TestCommandCLI_Read_TrimsWhitespace(t *testing.T) {
	t.Setenv(cliEnvVar, fakeOPPath(t))
	cli := NewCLI()

	data, err := cli.Read(context.Background(), "op://Private/e2e-account.json/notesPlain")
	require.NoError(t, err)
	// fake-op.sh deliberately wraps its output in leading/trailing newlines to
	// exercise bytes.TrimSpace in commandCLI.Read.
	assert.False(t, len(data) > 0 && (data[0] == '\n' || data[len(data)-1] == '\n'))
}

func TestCommandCLI_Read_Error(t *testing.T) {
	t.Setenv(cliEnvVar, fakeOPPath(t))
	cli := NewCLI()

	_, err := cli.Read(context.Background(), "op://Private/nonexistent/field")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "op read op://Private/nonexistent/field")
	assert.Contains(t, err.Error(), "item not found")
}

func TestCommandCLI_Read_BinaryNotFound(t *testing.T) {
	t.Setenv(cliEnvVar, filepath.Join(os.TempDir(), "afsc-op-does-not-exist"))
	cli := NewCLI()

	_, err := cli.Read(context.Background(), "op://Private/e2e-account.json/notesPlain")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "op read op://Private/e2e-account.json/notesPlain")
}

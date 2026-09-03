//go:build !integration

package op

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	// Ensure accidental NewCLI() use never invokes the real op binary (which
	// triggers 1Password desktop prompts). Unit tests inject stubCLI via
	// CLIOption; this is a fallback for any path that still calls NewCLI().
	// Excluded from the integration build (see integration_test.go) so that
	// build's TestIntegrationDownloadWithURL actually reaches the real op CLI
	// instead of silently getting this fake override too.
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		fakeOP := filepath.Join(filepath.Dir(filename), "testdata", "fake-op.sh")
		_ = os.Setenv("OP_CLI", fakeOP)
	}
	os.Exit(m.Run())
}

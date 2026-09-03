//go:build integration

package op_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs"

	_ "github.com/viant/afsc/op"
)

// TestIntegrationDownloadWithURL exercises a real 1Password secret via the op CLI.
//
// Run manually (not from CI or Cursor):
//
//	op signin
//	export OP_INTEGRATION_REF='op://Private/e2e-account.json/notesPlain'
//	go test ./op/... -tags=integration -run TestIntegrationDownloadWithURL -count=1 -v
func TestIntegrationDownloadWithURL(t *testing.T) {
	ref := os.Getenv("OP_INTEGRATION_REF")
	if ref == "" {
		t.Skip("set OP_INTEGRATION_REF to an op:// secret reference")
	}

	fs := afs.New()
	data, err := fs.DownloadWithURL(context.Background(), ref)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	t.Logf("read %d bytes from %s", len(data), ref)
}

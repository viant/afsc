## 1Password secret store

Read secrets from 1Password using the [`op` CLI](https://developer.1password.com/docs/cli/).

### URL format

```
op://<vault>/<item>/<field>
```

Example:

```
op://Private/e2e-account.json/notesPlain
```

The vault name is parsed as the URL host (`Private`). The item and field path follow the host (`e2e-account.json/notesPlain`).

### Usage

```go
import (
	"context"
	"github.com/viant/afs"
	_ "github.com/viant/afsc/op"
)

func main() {
	fs := afs.New()
	data, err := fs.DownloadWithURL(context.Background(), "op://Private/my-item/password")
	// ...
}
```

### Requirements

- `op` must be installed and on `PATH`, or set `OP_CLI` to the binary location
- The user must be signed in (`op signin`) or otherwise authenticated for CLI access

### Testing

**Unit tests (no 1Password, safe for CI and Cursor):**

```bash
cd ~/project/afsc
go test ./op/... -count=1 -v
```

These tests use an in-memory stub CLI and a `testdata/fake-op.sh` script. They never call your real `op` binary or open a 1Password session.

**Optional integration test (you run locally with 1Password):**

```bash
op signin
export OP_INTEGRATION_REF='op://Private/e2e-account.json/notesPlain'
go test ./op/... -tags=integration -run TestIntegrationDownloadWithURL -count=1 -v
```

Do not run `-tags=integration` from automation that should not access 1Password.

### Notes

- Read-only: upload, create, copy, and move are not supported
- Secret bytes are returned in memory; this connector does not write credentials to disk

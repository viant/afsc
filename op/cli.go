package op

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const cliEnvVar = "OP_CLI"

// CLI reads secrets from 1Password.
type CLI interface {
	Read(ctx context.Context, reference string) ([]byte, error)
}

type commandCLI struct {
	binary string
}

// NewCLI creates a CLI backed by the op binary.
func NewCLI() CLI {
	binary := strings.TrimSpace(os.Getenv(cliEnvVar))
	if binary == "" {
		binary = "op"
	}
	return &commandCLI{binary: binary}
}

func (c *commandCLI) Read(ctx context.Context, reference string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.binary, "read", reference)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			msg := strings.TrimSpace(string(exitErr.Stderr))
			if msg != "" {
				return nil, fmt.Errorf("op read %s: %w: %s", reference, err, msg)
			}
		}
		return nil, fmt.Errorf("op read %s: %w", reference, err)
	}
	return bytes.TrimSpace(out), nil
}

package gs

import (
	"os"

	"github.com/viant/afs"
	"github.com/viant/toolbox"
)

var maxRetries = 4

func init() {
	if maxR := os.Getenv("GS_MAX_RETRIES"); maxR != "" {
		maxRetries = toolbox.AsInt(maxR)
	}
	afs.GetRegistry().Register(Scheme, Provider)
}

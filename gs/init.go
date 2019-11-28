package gs

import (
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"os"
)

var maxRetries = 4

func init() {
	if max := os.Getenv("GS_MAX_RETRIES"); max != "" {
		maxRetries = toolbox.AsInt(max)
	}
	afs.GetRegistry().Register(Scheme, Provider)
}

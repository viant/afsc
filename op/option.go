package op

import (
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
)

// CLIOption injects a 1Password CLI implementation (primarily for tests).
type CLIOption struct {
	CLI CLI
}

// Init validates the option.
func (o *CLIOption) Init() error {
	return nil
}

// Validate validates the option.
func (o *CLIOption) Validate() error {
	return nil
}

func cliFromOptions(options []storage.Option) CLI {
	for _, opt := range options {
		switch o := opt.(type) {
		case *CLIOption:
			if o != nil && o.CLI != nil {
				return o.CLI
			}
		case CLIOption:
			if o.CLI != nil {
				return o.CLI
			}
		}
	}
	var cliOpt CLIOption
	if _, ok := option.Assign(options, &cliOpt); ok && cliOpt.CLI != nil {
		return cliOpt.CLI
	}
	return NewCLI()
}

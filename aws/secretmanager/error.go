package secretmanager

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

func isNotFound(err error) bool {
	if err != nil {
		var rnf *types.ResourceNotFoundException
		if errors.As(err, &rnf) {
			return true
		}
		return false
	}
	return false
}

package gs

import (
	"context"
	"github.com/viant/afs/base"
)

func runWithRetries(ctx context.Context, f func() error, storager *storager) (err error) {
	retry := base.NewRetry()
	for i := 0; i < maxRetries; i++ {
		err = f()
		if err == nil {
			break
		}
		if isProxyError(err) {
			if !storager.client.canProxyFallback {
				return err
			}
			if err = storager.disableProxy(ctx); err != nil {
				return err
			}
			err = f()
			if isProxyError(err) {
				return err
			}
		}
		if !isRetryError(err) {
			return err
		}
		sleepBeforeRetry(retry)
	}
	return err
}

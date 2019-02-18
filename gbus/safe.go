package gbus

import (
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
)

//Safety provides utility methods to safly invoke methods
type Safety struct{}

//SafeWithRetries safely invoke the function with the number of retries
func (s *Safety) SafeWithRetries(funk func() error, retries uint) error {
	action := func(attempts uint) (actionErr error) {
		defer func() {
			if p := recover(); p != nil {
				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				actionErr = errors.New(pncMsg)
			}
		}()
		return funk()
	}

	return retry.Retry(action,
		strategy.Limit(retries),
		strategy.Backoff(backoff.Fibonacci(50*time.Millisecond)))
}

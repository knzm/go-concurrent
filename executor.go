package concurrent

import (
	"context"

	"github.com/pkg/errors"
)

type Result interface{}
type Job func() Result

type Executor interface {
	Execute(ctx context.Context, job Job) (Result, error)
}

type executor struct {
	waiter Waiter
}

func NewExecutor(n int) Executor {
	return &executor{
		waiter: NewWaiter(n),
	}
}

func (executor *executor) Execute(ctx context.Context, job Job) (Result, error) {
	err := executor.waiter.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "No worker is available")
	}

	ch := make(chan interface{})
	go func() {
		defer executor.waiter.Release()
		defer close(ch)
		ch <- job()
	}()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		// Invoke a new goroutine which reads the channel.  It will ensure
		// that the writer goroutine can write on the channel without
		// blocking, and that the waiter will be released properly.
		go func() {
			<-ch
		}()

		return nil, errors.Wrap(ctx.Err(), "job is cancelled")
	}
}

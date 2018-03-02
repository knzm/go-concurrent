package concurrent

import (
	"context"
)

type Waiter interface {
	Acquire(ctx context.Context) error
	Release()
}

type waiter struct {
	sem chan struct{}
}

func NewWaiter(n int) Waiter {
	return &waiter{
		sem: make(chan struct{}, n),
	}
}

func (w *waiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.sem <- struct{}{}:
		return nil
	}
}

func (w *waiter) Release() {
	select {
	case <-w.sem:
	default:
		panic("too much release")
	}
}

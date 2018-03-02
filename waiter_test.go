package concurrent_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/knzm/go-concurrent"
)

type timeoutError struct{}

func (e timeoutError) Error() string   { return "timeout" }
func (e timeoutError) IsTimeout() bool { return true }

func isTimeout(err error) bool {
	terr, ok := err.(timeoutError)
	return ok && terr.IsTimeout()
}

func acquireWithTimeout(ctx context.Context, waiter concurrent.Waiter, timeout time.Duration) error {
	ch := make(chan error)
	defer close(ch)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				// ignore error on closed channel
			}
		}()
		ch <- waiter.Acquire(ctx)
	}()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		return timeoutError{}
	}
}

func releaseWithTimeout(ctx context.Context, waiter concurrent.Waiter, timeout time.Duration) error {
	ch := make(chan struct{})
	defer close(ch)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				// ignore error on closed channel
			}
		}()
		waiter.Release()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return timeoutError{}
	}
}

func TestWaiter(t *testing.T) {
	const timeThresholdForBlocking = time.Millisecond

	t.Run("non blocking", func(t *testing.T) {
		ctx := context.Background()
		waiter := concurrent.NewWaiter(3)
		// waiter.Acquire() should not be blocked
		err := acquireWithTimeout(ctx, waiter, timeThresholdForBlocking)
		if err != nil {
			if isTimeout(err) {
				t.Error("Blocking Acquire()")
			} else {
				t.Errorf("Expected no error, got %v", err)
			}
		}
	})

	t.Run("blocking", func(t *testing.T) {
		ctx := context.Background()
		waiter := concurrent.NewWaiter(3)
		for i := 0; i < 3; i++ {
			err := acquireWithTimeout(ctx, waiter, timeThresholdForBlocking)
			if err != nil {
				if isTimeout(err) {
					t.Errorf("[%d] Blocking Acquire()", i)
				} else {
					t.Errorf("[%d] Expected no error, got %v", i, err)
				}
				return
			}
		}
		// waiter.Acquire() should be blocked
		err := acquireWithTimeout(ctx, waiter, timeThresholdForBlocking)
		if err == nil {
			t.Error("Non-blocking Acquire()")
		} else if !isTimeout(err) {
			t.Errorf("[%d] Expected no error, got %v", 3, err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		waiter := concurrent.NewWaiter(3)
		for i := 0; i < 3; i++ {
			err := acquireWithTimeout(ctx, waiter, timeThresholdForBlocking)
			if err != nil {
				if isTimeout(err) {
					t.Errorf("[%d] Blocking Acquire()", i)
				} else {
					t.Errorf("[%d] Expected no error, got %v", i, err)
				}
				return
			}
		}
		// waiter.Acquire() should be timed out after 100ms
		err := acquireWithTimeout(ctx, waiter, time.Second)
		if err == nil {
			t.Error("Non-blocking Acquire()")
		} else if isTimeout(err) {
			// timeout without context
			t.Error("Expected timeout")
		} else if err.Error() != context.DeadlineExceeded.Error() {
			t.Errorf("[%d] Expected no error, got %v", 3, err)
		}
	})

	t.Run("acquire and release", func(t *testing.T) {
		ctx := context.Background()
		waiter := concurrent.NewWaiter(3)
		for i := 0; i < 5; i++ {
			// waiter.Acquire() should not be blocked
			err := acquireWithTimeout(ctx, waiter, timeThresholdForBlocking)
			if err != nil {
				if isTimeout(err) {
					t.Errorf("[%d] blocking Acquire()", i)
				} else {
					t.Errorf("[%d] Expected no error, got %v", i, err)
				}
				return
			}
			// waiter.Release() should not be blocked
			err = releaseWithTimeout(ctx, waiter, timeThresholdForBlocking)
			if err != nil {
				if isTimeout(err) {
					t.Errorf("[%d] blocking Release()", i)
				} else {
					t.Errorf("[%d] Expected no error, got %v", i, err)
				}
				return
			}
		}
	})

	t.Run("slow worker", func(t *testing.T) {
		var mu sync.Mutex
		var alive, maxAlive int

		workerStarted := func() {
			mu.Lock()
			defer mu.Unlock()

			alive++
			if alive > maxAlive {
				maxAlive = alive
			}
		}

		workerFinished := func() {
			mu.Lock()
			defer mu.Unlock()

			alive--
		}

		worker := func(ctx context.Context, waiter concurrent.Waiter) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			err := waiter.Acquire(ctx)
			if err != nil {
				return err
			}

			ch := make(chan struct{})
			go func() {
				defer waiter.Release()

				workerStarted()
				// do the heavy task
				time.Sleep(100 * time.Millisecond)
				workerFinished()

				ch <- struct{}{}
			}()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
				return nil
			}
		}

		const numThreads = 10
		const numJobs = 100

		waiter := concurrent.NewWaiter(numThreads)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan error)

		for i := 0; i < numJobs; i++ {
			go func() {
				ch <- worker(ctx, waiter)
			}()
		}

		for i := 0; i < numJobs; i++ {
			err := <-ch
			if err != nil {
				t.Errorf("[%d] Expected no error, got %v", i, err)
			}
		}

		if maxAlive > numThreads {
			t.Errorf("maxAlive is greater than expected: %d", maxAlive)
		}
	})
}

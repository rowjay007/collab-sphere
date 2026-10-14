package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryCompaction(t *testing.T) {
	t.Run("transient failures", func(t *testing.T) {
		attempts := 0
		next, err := retryCompaction(make(chan struct{}), func() (uint64, error) {
			attempts++
			if attempts < 3 {
				return 0, errors.New("transient")
			}
			return 100, nil
		})
		if err != nil || next != 100 || attempts != 3 {
			t.Fatalf("next=%d attempts=%d err=%v", next, attempts, err)
		}
	})

	t.Run("close interrupts backoff", func(t *testing.T) {
		closeCh := make(chan struct{})
		attempted := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := retryCompaction(closeCh, func() (uint64, error) {
				select {
				case attempted <- struct{}{}:
				default:
				}
				return 0, errors.New("persistent")
			})
			done <- err
		}()

		<-attempted
		close(closeCh)
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatal("retry backoff did not stop after close")
		}
	})
}

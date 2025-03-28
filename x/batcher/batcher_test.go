package batch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/runreveal/kawa"
	"github.com/stretchr/testify/assert"
)

// func flushTest[T any](c context.Context, msgs []kawa.Message[T]) {
// 	for _, msg := range msgs {
// 		fmt.Println(msg.Value)
// 	}
// 	counter++
// }

func TestBatcher(t *testing.T) {

	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		for _, msg := range msgs {
			fmt.Println(msg.Value)
		}
		return nil
	}

	bat := NewDestination[string](kawa.DestinationFunc[string](ff), Raise[string](), FlushLength(1))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errc := make(chan error)

	go func(c context.Context, ec chan error) {
		ec <- bat.Run(c)
	}(ctx, errc)

	writeMsgs := []kawa.Message[string]{
		{Value: "hi"},
		{Value: "hello"},
		{Value: "bonjour"},
	}

	err := bat.Send(ctx, writeMsgs)
	assert.NoError(t, err)
	cancel()
	assert.NoError(t, <-errc)
}

func TestBatchFlushTimeout(t *testing.T) {
	var hMu sync.Mutex
	handled := false

	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		for _, msg := range msgs {
			fmt.Println(msg.Value)
		}
		hMu.Lock()
		handled = true
		hMu.Unlock()
		return nil
	}

	bat := NewDestination[string](
		kawa.DestinationFunc[string](ff),
		Raise[string](),
		FlushFrequency(1*time.Millisecond),
		FlushLength(2),
		StopTimeout(10*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	errc := make(chan error)

	go func(c context.Context, ec chan error) {
		ec <- bat.Run(c)
	}(ctx, errc)

	err := bat.Send(ctx, []kawa.Message[string]{{Value: "hi"}})
	assert.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	hMu.Lock()
	assert.True(t, handled, "value should have been set!")
	hMu.Unlock()

	cancel()
	assert.NoError(t, <-errc)
}

func TestBatcherErrors(t *testing.T) {
	flushErr := errors.New("flush error")
	var ff = func(c context.Context, msgs []kawa.Message[string]) error {
		return flushErr
	}

	t.Run("flush errors return from run", func(t *testing.T) {
		bat := NewDestination[string](kawa.DestinationFunc[string](ff), Raise[string](), FlushLength(1))
		errc := make(chan error)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			{Value: "hi"},
		}

		err := bat.Send(ctx, writeMsgs)
		assert.ErrorIs(t, err, flushErr)

		cancel()
		assert.EqualError(t, <-errc, "flush error")
	})

	t.Run("cancellation works", func(t *testing.T) {
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			return nil
		}
		bat := NewDestination[string](kawa.DestinationFunc[string](ff), Raise[string](), FlushLength(1))
		errc := make(chan error)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		cancel()
		err := <-errc
		assert.ErrorIs(t, err, nil, "should return nil since no errors in flush")
	})

	t.Run("deadlock cancellation", func(t *testing.T) {

		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			<-c.Done()
			return nil
		}
		bat := NewDestination[string](kawa.DestinationFunc[string](ff), Raise[string](), FlushLength(1), StopTimeout(10*time.Millisecond))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		errc := make(chan error)

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		err := bat.Send(ctx, writeMsgs)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		cancel()
		assert.ErrorIs(t, <-errc, errDeadlock, "should return deadlock error")
	})

	t.Run("handle errors when errors returned from flush", func(t *testing.T) {
		// This test deadlocks in failure
		// Should figure out how to write it better

		flushErr := errors.New("flush error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(110 * time.Millisecond)
			return flushErr
		}
		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			assert.ErrorIs(t, err, flushErr)
			return err
		})
		bat := NewDestination[string](
			kawa.DestinationFunc[string](ff),
			errHandler,
			FlushLength(2),
			FlushParallelism(2),
			StopTimeout(90*time.Millisecond),
		)
		errc := make(chan error)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		err := bat.Send(ctx, writeMsgs)
		assert.NoError(t, err, "errors aren't returned from Send")

		// parallelism is 2, so max processing time is 220ms (110ms for the first
		// two msgs in parallel, and another 110ms for the third)
		// stop timeout of 90ms means we'll see the deadlock error
		cancel()
		assert.ErrorIs(t, <-errc, errDeadlock)
	})

	t.Run("handle errors when errors returned from flush", func(t *testing.T) {
		// This test deadlocks in failure
		// Should figure out how to write it better

		flushErr := errors.New("flush error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(110 * time.Millisecond)
			return flushErr
		}
		var errHandler = ErrorFunc[string](func(c context.Context, err error, msgs []kawa.Message[string]) error {
			assert.ErrorIs(t, err, flushErr)
			return err
		})
		bat := NewDestination[string](
			kawa.DestinationFunc[string](ff),
			errHandler,
			FlushLength(2),
			FlushParallelism(2),
			StopTimeout(90*time.Millisecond),
		)
		errc := make(chan error)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		err := bat.Send(ctx, writeMsgs)
		assert.NoError(t, err, "errors aren't returned from Send")

		// parallelism is 2, so max processing time is 220ms (110ms for the first
		// two msgs in parallel, and another 110ms for the third)
		// stop timeout of 90ms means we'll see the deadlock error
		cancel()
		assert.ErrorIs(t, <-errc, errDeadlock)
	})

	t.Run("dont deadlock on errors returned from flush with length 1", func(t *testing.T) {

		// This test deadlocks in failure
		// Should figure out how to write it better

		flushErr := errors.New("flush error")
		var ff = func(c context.Context, msgs []kawa.Message[string]) error {
			time.Sleep(5 * time.Millisecond)
			return flushErr
		}
		bat := NewDestination[string](kawa.DestinationFunc[string](ff), Raise[string](), FlushLength(1), FlushParallelism(2),
			StopTimeout(100*time.Millisecond))
		errc := make(chan error)

		ctx := context.Background()

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		writeMsgs := []kawa.Message[string]{
			// will be blocked flushing
			{Value: "hi"},
			// will be stuck waiting for flush slot
			{Value: "hello"},
			// will be stuck waiting to write to msgs in Send
			{Value: "bonjour"},
		}

		err := bat.Send(ctx, writeMsgs)
		assert.NoError(t, err)

		assert.ErrorIs(t, <-errc, flushErr)
	})

	t.Run("Don't ack messages if flush handler returns ErrDontAck", func(t *testing.T) {
		var retryHandler = func(ctx context.Context, err error, msgs []kawa.Message[string]) error {
			return ErrDontAck
		}
		bat := NewDestination[string](
			kawa.DestinationFunc[string](ff),
			ErrorFunc[string](retryHandler),
			FlushLength(1),
			FlushParallelism(1),
		)
		errc := make(chan error)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)

		go func(c context.Context, ec chan error) {
			ec <- bat.Run(c)
		}(ctx, errc)

		messages := []kawa.Message[string]{
			{Value: "one"},
			{Value: "two"},
			{Value: "three"},
			{Value: "ten"},
		}

		err := bat.Send(ctx, messages)
		assert.ErrorIs(t, err, flushErr)
		time.Sleep(50 * time.Millisecond)

		cancel()
		assert.ErrorIs(t, <-errc, nil)
	})
}

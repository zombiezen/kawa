package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"log/slog"

	"github.com/runreveal/kawa"
	"github.com/segmentio/ksuid"
)

var _ kawa.Destination[struct{}] = (*Destination[struct{}])(nil)

// ErrDontAck should be returned by ErrorHandlers when they wish to
// signal to the batcher to skip acking a message as delivered, but
// continue to process.  For example, if an error is retryable and
// will be retried upstream at the source if an ack is not received
// before some timeout.
var ErrDontAck = errors.New("Destination encountered a retryable error")

type ErrorHandler[T any] interface {
	HandleError(context.Context, error, []kawa.Message[T]) error
}

type ErrorFunc[T any] func(context.Context, error, []kawa.Message[T]) error

func (ef ErrorFunc[T]) HandleError(c context.Context, err error, msgs []kawa.Message[T]) error {
	return ef(c, err, msgs)
}

// Destination is a batching destination that will buffer messages until the
// FlushLength limit is reached or the FlushFrequency timer fires, whichever
// comes first.
//
// `Destination.Run` must be called after calling `New` before events will be
// processed in this destination. Not calling `Run` will likely end in a
// deadlock as the internal channel being written to by `Send` will not be
// getting read.
type Destination[T any] struct {
	flusher         kawa.Destination[T]
	flushq          chan struct{}
	flushlen        int
	flushfreq       time.Duration
	flushcan        map[string]context.CancelFunc
	flushTimeout    time.Duration
	stopTimeout     time.Duration
	watchdogTimeout time.Duration

	errorHandler ErrorHandler[T]
	flusherr     chan error

	messages chan pendingMessage[T]
	buf      []pendingMessage[T]

	count   int
	running bool
	syncMu  sync.Mutex
}

type OptFunc func(*Opts)

type Opts struct {
	FlushLength      int
	FlushFrequency   time.Duration
	FlushTimeout     time.Duration
	FlushParallelism int
	StopTimeout      time.Duration
	WatchdogTimeout  time.Duration
}

func FlushFrequency(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushFrequency = d
	}
}

func FlushLength(size int) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushLength = size
	}
}

func FlushParallelism(n int) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushParallelism = n
	}
}

func FlushTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.FlushTimeout = d
	}
}

func WatchdogTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.WatchdogTimeout = d
	}
}

func StopTimeout(d time.Duration) func(*Opts) {
	return func(opts *Opts) {
		opts.StopTimeout = d
	}
}

func DiscardHandler[T any]() ErrorHandler[T] {
	return ErrorFunc[T](func(context.Context, error, []kawa.Message[T]) error { return nil })
}

func Raise[T any]() ErrorHandler[T] {
	return ErrorFunc[T](func(_ context.Context, err error, _ []kawa.Message[T]) error { return err })
}

// NewDestination instantiates a new batcher.
func NewDestination[T any](f kawa.Destination[T], e ErrorHandler[T], opts ...OptFunc) *Destination[T] {
	cfg := Opts{
		FlushLength:      100,
		FlushFrequency:   1 * time.Second,
		FlushParallelism: 2,
		StopTimeout:      5 * time.Second,
	}

	for _, o := range opts {
		o(&cfg)
	}

	// TODO: validate here
	if cfg.FlushParallelism < 1 {
		panic("FlushParallelism must be greater than or equal to 1")
	}
	if e == nil {
		panic("ErrorHandler must not be nil")
	}
	if cfg.StopTimeout < 0 {
		cfg.StopTimeout = 0
	}
	if cfg.WatchdogTimeout < 0 {
		cfg.WatchdogTimeout = 0
	}
	if cfg.FlushTimeout < 0 {
		cfg.FlushTimeout = 0
	}

	d := &Destination[T]{
		flushlen:        cfg.FlushLength,
		flushq:          make(chan struct{}, cfg.FlushParallelism),
		flusher:         f,
		flushcan:        make(map[string]context.CancelFunc),
		flushfreq:       cfg.FlushFrequency,
		flushTimeout:    cfg.FlushTimeout,
		stopTimeout:     cfg.StopTimeout,
		watchdogTimeout: cfg.WatchdogTimeout,

		errorHandler: e,
		flusherr:     make(chan error, cfg.FlushParallelism),

		messages: make(chan pendingMessage[T]),
	}

	return d
}

type pendingMessage[T any] struct {
	msg       kawa.Message[T]
	errorChan chan<- error
}

// Send satisfies the [kawa.Destination] interface and accepts messages to be
// buffered for flushing after the FlushLength limit is reached or the
// FlushFrequency timer fires, whichever comes first.
// Send will wait until all the messages have been sent (or failed to send),
// or ctx.Done() is closed, whichever comes first,
// and returns the first error encountered.
func (d *Destination[T]) Send(ctx context.Context, msgs []kawa.Message[T]) error {
	if len(msgs) < 1 {
		return nil
	}

	// Buffer so we don't block the batching goroutine
	// in case we bail early.
	ch := make(chan error, len(msgs))

	var firstError error
	for _, m := range msgs {
		select {
		case d.messages <- pendingMessage[T]{msg: m, errorChan: ch}: // Here
		case <-ctx.Done():
			// TODO: one more flush?
			return ctx.Err()
		}
	}

	for range msgs {
		select {
		case err := <-ch:
			if firstError == nil {
				firstError = err
			}
		case <-ctx.Done():
			if firstError == nil {
				firstError = ctx.Err()
			}
			return firstError
		}
	}

	return firstError
}

// Run starts the batching destination.  It must be called before messages will
// be processed and written to the underlying Flusher.
// Run will block until the context is canceled.
// Upon cancellation, Run will flush any remaining messages in the buffer and
// return any flush errors that occur
func (d *Destination[T]) Run(ctx context.Context) error {
	var epoch uint64
	epochC := make(chan uint64)
	setTimer := true

	d.syncMu.Lock()
	if d.running {
		panic("already running")
	} else {
		d.running = true
	}
	d.syncMu.Unlock()

	var wdChan <-chan time.Time
	var wdTimer *time.Timer
	if d.watchdogTimeout > 0 {
		wdTimer = time.NewTimer(d.watchdogTimeout)
		wdChan = wdTimer.C
	}

	var err error
loop:
	for {
		select {
		case <-wdChan:
			return errDeadlock

		case msg := <-d.messages: // Here
			d.count++
			if setTimer {
				// copy the epoch to send on the chan after the timer fires
				epc := epoch
				time.AfterFunc(d.flushfreq, func() {
					epochC <- epc // Here
				})

				if wdTimer != nil {
					if !wdTimer.Stop() {
						<-wdTimer.C
					}
					wdTimer.Reset(d.watchdogTimeout)
				}

				setTimer = false
			}
			d.buf = append(d.buf, msg)
			if len(d.buf) >= d.flushlen {
				epoch++
				d.flush(ctx)
				setTimer = true
			}
		case tEpoch := <-epochC:
			// if we haven't flushed yet this epoch, then flush, otherwise ignore
			if tEpoch == epoch {
				epoch++
				d.flush(ctx)
				setTimer = true
			}
		case <-ctx.Done():
			// on shutdown, don't attempt final flush even if buffer is not empty
			break loop
		case err = <-d.flusherr:
			break loop
		}
	}

	// we're done, no flushes in flight
	if len(d.flushq) == 0 {
		return err
	}

	slog.Info("stopping batcher. waiting for remaining flushes to finish.", "len", len(d.flushq))
	for i := 10 * time.Millisecond; i < d.stopTimeout; i = i + 10*time.Millisecond {
		if len(d.flushq) == 0 {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	// flushes still active after timeout
	// cancel them.
	d.syncMu.Lock()
	for k, v := range d.flushcan {
		v()
		fmt.Println("timeout cancel for id", k)
	}
	d.syncMu.Unlock()
	return errDeadlock
}

var errDeadlock = errors.New("batcher: flushes timed out")

func (d *Destination[T]) flush(ctx context.Context) {
	// We make a new context here so that we can cancel the flush if the parent
	// context is canceled. It's important to use context.Background() here because
	// we don't want to propagate the parent context's cancelation to the flusher.
	// If we did, then the flusher would likely be canceled before it could
	// finish flushing.
	flctx, cancel := context.WithCancel(context.Background())

	id := ksuid.New().String()
	d.syncMu.Lock()
	d.flushcan[id] = cancel
	d.syncMu.Unlock()

	// block until a slot is available, or until a timeout is reached in the
	// parent context
	select {
	case d.flushq <- struct{}{}:
	case <-ctx.Done():
		cancel()
		return
	}

	// Have to make a copy so these don't get overwritten
	msgs := make([]kawa.Message[T], len(d.buf))
	channels := make([]chan<- error, len(d.buf))
	for i, m := range d.buf {
		msgs[i] = m.msg
		channels[i] = m.errorChan
	}
	// Clear the buffer for the next batch
	d.buf = d.buf[:0]

	go func() {
		defer cancel()

		if err := d.doflush(flctx, msgs, channels); err != nil {
			d.flusherr <- err
		}
		// clear flush slot
		<-d.flushq
		// clear cancel
		d.syncMu.Lock()
		delete(d.flushcan, id)
		d.syncMu.Unlock()
	}()
}

func (d *Destination[T]) doflush(ctx context.Context, msgs []kawa.Message[T], channels []chan<- error) error {
	if d.flushTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.flushTimeout)
		defer cancel()
	}

	err := d.flusher.Send(ctx, msgs)
	var flushError error
	if err != nil {
		slog.Debug("flush err", "error", err)
		flushError = d.errorHandler.HandleError(ctx, err, msgs)
		// If error handler returns ErrDontAck, this means we want the
		// batcher to continue running, but to skip acknowledging the delivery
		// of the affected messages
		if errors.Is(flushError, ErrDontAck) {
			flushError = nil
		}
	}

	// All channels are appropriately buffered, so they will not block.
	for _, ch := range channels {
		ch <- err
	}

	return flushError
}

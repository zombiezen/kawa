package multi

import (
	"context"

	"github.com/runreveal/kawa"
)

var _ kawa.Destination[struct{}] = MultiDestination[struct{}]{}

type MultiDestination[T any] struct {
	wrapped []kawa.Destination[T]
}

func NewMultiDestination[T any](dests []kawa.Destination[T]) MultiDestination[T] {
	return MultiDestination[T]{
		wrapped: dests,
	}
}

func (md MultiDestination[T]) Send(ctx context.Context, msgs []kawa.Message[T]) error {
	for _, d := range md.wrapped {
		if err := d.Send(ctx, msgs); err != nil {
			return err
		}
	}
	return nil
}

package printer

import (
	"context"
	"io"

	"github.com/runreveal/kawa"
)

var _ kawa.Destination[[]byte] = (*Printer)(nil)

type Printer struct {
	writer io.Writer
	delim  []byte
}

func WithDelim(delim []byte) func(*Printer) {
	return func(s *Printer) {
		s.delim = delim
	}
}

func NewPrinter(writer io.Writer, opts ...func(*Printer)) *Printer {
	ret := &Printer{
		writer: writer,
		delim:  []byte("\n"),
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (p *Printer) Send(ctx context.Context, msgs []kawa.Message[[]byte]) error {
	for _, m := range msgs {
		toSend := append(m.Value, []byte(p.delim)...)

		_, err := p.writer.Write(toSend)
		if err != nil {
			return err
		}
	}
	return nil
}

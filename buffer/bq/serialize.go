package bq

import (
	"fmt"
	"sync"
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"
)

type RowBuffer struct {
	Rows   [][]byte
	Buf    []byte // 9MB of data
	Length int
}

func (b *RowBuffer) Reset() {
	b.Rows = b.Rows[:0]
	b.Length = 0
}

type RowSerializer struct { // We can write until we fill the channel or we reach the end of the buffer
	freeBuffers chan *RowBuffer
	fullBuffers chan *RowBuffer // Read will usually just take a full buffer
	buf         *RowBuffer
	mu          sync.Mutex // Used for working on buf. Sometimes the reader will need to read directly from the in use buffer.
}

func NewRowSerializer(buffers int) *RowSerializer {
	rs := &RowSerializer{
		freeBuffers: make(chan *RowBuffer, buffers),
		fullBuffers: make(chan *RowBuffer, buffers),
	}

	for i := 0; i < buffers; i++ {
		rs.freeBuffers <- &RowBuffer{
			Rows: make([][]byte, 0, 5_000),
			Buf:  make([]byte, 1024*1024*9),
		}
	}
	return rs
}

// Serialize is used by the same routine that does Append (the cmdhandler routine).
func (s *RowSerializer) Serialize(row *v1.Row) {
	rowSize := row.SizeVT()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.buf == nil {
		s.buf = <-s.freeBuffers
	}

	if s.buf.Length+int(rowSize) > len(s.buf.Buf) {
		s.fullBuffers <- s.buf
		s.buf = <-s.freeBuffers
	}

	data := s.buf.Buf[s.buf.Length : s.buf.Length+int(rowSize)]
	s.buf.Rows = append(s.buf.Rows, data)
	s.buf.Length += int(rowSize)

	if _, err := row.MarshalToSizedBufferVT(data); err != nil {
		panic(fmt.Errorf("failed to serialize row: %w", err))
	}
}

// Rows is used by another routine than Serialize.
func (s *RowSerializer) Rows(fn func(rows [][]byte)) (nrRows int) {
	for {
		select {
		case buf := <-s.fullBuffers:
			nrRows = len(buf.Rows)
			fn(buf.Rows)
			buf.Reset()
			s.freeBuffers <- buf
			return
		case <-time.After(10 * time.Millisecond):
			s.mu.Lock()
			if s.buf != nil {
				s.fullBuffers <- s.buf
				s.buf = nil
			}
			s.mu.Unlock()
		}
	}
}

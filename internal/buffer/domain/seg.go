package domain

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ntail-io/streams/internal/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/internal/core/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	ErrSegmentFull    = errors.New("segment is full")
	ErrChunkFull      = errors.New("chunk is full")
	ErrTooLargeData   = errors.New("data is too large to be pushed")
	ErrOutOfHashRange = errors.New("hash is out of range")
)

type PollResult struct {
	HashRange types.HashRange
	From      time.Time
	To        time.Time
	LastMsgId uuid.UUID
	Data      []byte
	EOF       bool
}

type Segment struct {
	Topic            types.Topic
	HashRange        types.HashRange
	From             uuid.Time
	Finished         bool
	Closed           bool
	MaxKeysKV        int
	MaxSegmentChunks int
	// MaxUncommitted   uint32               // Max number of uncommitted messages before back pressure kicks in
	kv          map[types.Key][]byte //map[[20]byte][]byte
	allocator   *ChunkAllocator
	chunks      int
	head        *Chunk
	tail        *Chunk
	uncommitted int32
	// uncommittedFullCh chan struct{} // unbuffered. Sent on append if channel is full, will be consumed after commit if message exist.
}

type SegmentOption func(*Segment)

func WithMaxKeysKV(maxKeysKV int) SegmentOption {
	return func(s *Segment) {
		s.MaxKeysKV = maxKeysKV
	}
}

func WithInitialKeysCapacityKV(capacity int) SegmentOption {
	return func(s *Segment) {
		s.kv = make(map[types.Key][]byte, capacity) //make(map[[20]byte][]byte, capacity)
	}
}

func WithMaxSegmentChunks(maxSegmentChunks int) SegmentOption {
	return func(s *Segment) {
		s.MaxSegmentChunks = maxSegmentChunks
	}
}

// func WithMaxUncommitted(maxUncommitted uint32) SegmentOption {
// 	return func(s *Segment) {
// 		s.MaxUncommitted = maxUncommitted
// 	}
// }

const (
	DefaultMaxKeysKV            = 10_000_000
	DefaultInitialKeyCapacityKV = 100_000
	DefaultMaxSegmentChunks     = 100
	DefaultMaxUncommitted       = 50_000
)

func NewSegment(topic types.Topic, hashRange types.HashRange, from uuid.Time, allocator *ChunkAllocator, options ...SegmentOption) Segment {
	s := Segment{
		Topic:     topic,
		HashRange: hashRange,
		From:      from,
		allocator: allocator,
		// uncommittedFullCh: make(chan struct{}),
	}

	for _, option := range options {
		option(&s)
	}

	if s.MaxSegmentChunks == 0 {
		s.MaxSegmentChunks = DefaultMaxSegmentChunks
	}

	if s.MaxKeysKV == 0 {
		s.MaxKeysKV = DefaultMaxKeysKV
	}

	if s.kv == nil {
		s.kv = make(map[types.Key][]byte, DefaultInitialKeyCapacityKV) //make(map[[20]byte][]byte, DefaultInitialKeyCapacityKV)
	}
	return s
}

// Wait until everything is committed, should only be called on same routine that does Append.
func (s *Segment) WaitCommitted() {
	for atomic.LoadInt32(&s.uncommitted) == 0 {
		time.Sleep(10 * time.Microsecond)
	}
}

// Close returns the chunks to the allocator.
func (s *Segment) Close() {
	freed := 0
	for c := s.popChunk(); c != nil; c = s.popChunk() {
		s.allocator.free(c)
		freed++
	}
	log.Infof("closing segment: freed %d chunks", freed)
	s.kv = nil
	s.Closed = true
}

// func (s *Segment) putKey(key types.Key, data []byte) (oldValue []byte) {
// 	if len(s.kv) >= s.MaxKeysKV {
// 		panic("too many keys")
// 	}

// 	//k := sha1.Sum([]byte(key)) // TODO Check for key collisions?
// 	//var k [20]byte
// 	oldValue = s.kv[key]
// 	if data == nil {
// 		delete(s.kv, key)
// 	} else {
// 		if s.kv == nil {
// 			panic(fmt.Errorf("WTF: %s %d", s.HashRange.String(), s.From))
// 		}
// 		s.kv[key] = data
// 	}
// 	return
// }

func (s *Segment) Get(key types.Key) (msgId uuid.UUID, data []byte, ok bool) {
	bytes, ok := s.kv[key] //sha1.Sum([]byte(key))]
	if !ok {
		return
	}

	msgId, err := uuid.FromBytes(bytes[:16])
	if err != nil {
		panic("could not parse uuid bytes")
	}

	data = make([]byte, len(bytes[20:]))
	copy(data, bytes[20:])

	return
}

func (s *Segment) Commit(n int) {
	atomic.AddInt32(&s.uncommitted, int32(-n))

	// if len(s.uncommittedFullCh) > 0 && n > 0 {
	// 	<-s.uncommittedFullCh // Notify waiting appender that there is room for more
	// }
}

func (s *Segment) Append(key types.Key, data []byte) (id uuid.UUID, err error) {
	var appendedData []byte
	// // Wait if uncommitted max is reached
	// for uncommitted := atomic.LoadInt32(&s.uncommitted); uncommitted >= int32(s.MaxUncommitted); uncommitted = atomic.LoadInt32(&s.uncommitted) {
	// 	log.Warnf("uncommitted max (%d) reached on segment [%v], this is a sign that hash range size change might be needed. Waiting for commit...", s.MaxUncommitted, s)
	// 	s.uncommittedFullCh <- struct{}{}
	// }

	hash := key.Hash()

	if !s.HashRange.Contains(hash) {
		return id, ErrOutOfHashRange
	}

	if s.MaxKeysKV <= len(s.kv) {
		return id, ErrSegmentFull
	}

	if s.tail != nil {
		id, appendedData, err = s.tail.append(hash, data)
		if err == ErrTooLargeData {
			panic(err)
		}
	}

	if s.tail == nil || err != nil {
		if s.chunks >= s.MaxSegmentChunks {
			return id, ErrSegmentFull
		}

		chunk := s.allocator.alloc()
		s.pushChunk(chunk)
		id, appendedData, err = s.tail.append(hash, data)
		if err != nil {
			panic(fmt.Errorf("could not append to empty chunk :%w", err))
		}
	}
	_ = appendedData

	// status.oldValue = s.putKey(key, appendedData)

	atomic.AddInt32(&s.uncommitted, 1)

	return id, nil
}

func (s *Segment) Poll(from time.Time, fromMsgId uuid.UUID) (res PollResult, err error) {
	res.HashRange = s.HashRange
	res.From = from
	if s.tail == nil {
		res.EOF = s.Finished
		return
	}

	data, lastMsgId := s.tail.poll(uuidutil.FromTime(from), fromMsgId, int(atomic.LoadInt32(&s.uncommitted)))
	if data == nil {
		res.EOF = s.Finished
		return res, nil
	}

	// This is really shitty code, we need to copy the data because the chunk might be freed and the data overwritten
	// before the response has been sent.
	// A better way here would be to simply return the data to the SegmentService (zero copy) and let it copy the data once
	// to the grpc response. Optimal would be if we used something like a flatbuffer for GRPC to only do a single
	// (zero alloc) copy. But the second best solution would be to let the SegmentService copy the data to a response
	// buffer that is later on marshalled to protobuf.
	dst := make([]byte, len(data))
	copy(dst, data)

	return PollResult{
		HashRange: s.HashRange,
		From:      from,
		To:        uuidutil.ToTime(lastMsgId.Time()),
		Data:      dst,
		LastMsgId: lastMsgId,
		EOF:       s.Finished && (s.tail.msgLen == 0 || lastMsgId == *s.tail.lastMsgId()),
	}, nil
}

func (s *Segment) pushChunk(c *Chunk) {
	defer s.gagueChunks()
	if s.head == nil {
		s.head = c
	}
	c.next = s.tail
	s.tail = c
	s.chunks++
}

func (s *Segment) popChunk() *Chunk {
	defer s.gagueChunks()
	chunk := s.tail
	if chunk == nil {
		s.head = nil
		return nil
	}
	if s.head == chunk {
		s.head = nil
	}
	s.tail = chunk.next
	s.chunks--
	return chunk
}

func (s *Segment) String() string {
	return fmt.Sprintf("topic: %s, hash range: %v, from: %d", s.Topic, s.HashRange, s.From)
}

var (
	gagueSegments = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "segment_chunks"}, []string{"topic", "hash_range", "from"})
)

func (s *Segment) gagueChunks() {
	gagueSegments.WithLabelValues(string(s.Topic), s.HashRange.String(), strconv.Itoa(int(s.From))).Set(float64(s.chunks))
}

package domain

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/core/types"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Append(t *testing.T) {
	allocator := NewChunkAllocator()
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
	)
	_, err := s.Append("key-1", []byte("hello"))
	assert.NoError(t, err)
}

func TestSegment_Append_Too_Big(t *testing.T) {
	allocator := NewChunkAllocator()
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
	)

	assert.Panics(t, func() {
		_, _ = s.Append("key-1", make([]byte, 1024*1024*10))
	})
}

func TestSegment_Append_Many(t *testing.T) {
	allocator := NewChunkAllocator(WithAllocatorSize(24 * 1024 * 1024))
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
		WithMaxSegmentChunks(2),
	)
	data := make([]byte, 1024*1024*3)
	_, err := s.Append("key-1", data)
	assert.NoError(t, err)
	assert.Nil(t, s.tail.next)
	_, err = s.Append("key-1", data)
	assert.NoError(t, err)
	assert.Nil(t, s.tail.next)
	_, err = s.Append("key-1", data)
	assert.NoError(t, err)
	assert.NotNil(t, s.tail.next)
	_, err = s.Append("key-1", data)
	assert.NoError(t, err)
	_, err = s.Append("key-1", data)
	assert.ErrorIs(t, err, ErrSegmentFull)
}

func TestSegment_Poll(t *testing.T) {
	allocator := NewChunkAllocator(WithAllocatorSize(9 * 1024 * 1024))
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
	)
	from := time.Now()
	id, err := s.Append("key-1", []byte("hello"))
	assert.NoError(t, err)
	s.Commit(1)
	res, err := s.Poll(from, uuid.Nil)
	assert.NoError(t, err)
	assert.True(t, res.To.After(from))
	assert.Equal(t, types.HashRange{From: 0, To: types.MaxHash}, res.HashRange)
	assert.Len(t, res.Data, 25)
	assert.Equal(t, id[:], res.Data[:16])
	id2, err := uuid.FromBytes(res.Data[:16])
	assert.NoError(t, err)
	assert.Equal(t, uuid.Version(byte(1)), id2.Version())
	assert.Equal(t, []byte("hello"), res.Data[20:])
}

func TestSegment_Poll_EOF(t *testing.T) {
	allocator := NewChunkAllocator(WithAllocatorSize(9 * 1024 * 1024))
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
	)
	s.Finished = true
	res, err := s.Poll(time.Now(), uuid.Nil)
	assert.NoError(t, err)
	assert.True(t, res.EOF)
}

// func TestSegment_Get(t *testing.T) {
// 	allocator := NewChunkAllocator(WithAllocatorSize(9 * 1024 * 1024))
// 	var s = NewSegment(
// 		"a-topic",
// 		types.HashRange{From: 0, To: types.MaxHash},
// 		uuidutil.Now(),
// 		allocator,
// 		WithInitialKeysCapacityKV(3),
// 		WithMaxKeysKV(3),
// 	)
// 	data := []byte("hello")
// 	id, err := s.Append("key-1", data)
// 	assert.NoError(t, err)
// 	msgId, res, ok := s.Get("key-1")
// 	assert.True(t, ok)
// 	assert.Equal(t, id, msgId)
// 	assert.Equal(t, data, res)
// 	assert.True(t, &data != &res, "should be a copy")
// }

func BenchmarkSegment_Append(b *testing.B) {
	allocator := NewChunkAllocator(WithAllocatorSize(2 << 30))
	var s = NewSegment(
		"a-topic",
		types.HashRange{From: 0, To: types.MaxHash},
		uuidutil.Now(),
		allocator,
		WithMaxSegmentChunks(1_000),
	)
	data := make([]byte, 32)
	_ = data
	_ = s

	runtime.GC()
	b.ResetTimer()

	//keyNr := 10_000

	for i := 0; i < b.N; i++ {
		keyNr := i % 1000
		key := types.Key(strconv.Itoa(keyNr))
		_ = string(key)
		_, err := s.Append(key, data)
		if err != nil {
			b.Fatal(err)
		}
		if keyNr == 0 {
			s.Commit(1000)
		}
	}
}

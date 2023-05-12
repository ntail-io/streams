package domain

import (
	"testing"
	"time"
	"unsafe"

	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/core/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestChunk_Append(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, err := chunk.append(types.Hash(0), make([]byte, 5))
	assert.NoError(t, err)
	assert.Equal(t, 5+4+16, chunk.msgLen)
	assert.Len(t, chunk.index, 1)
}

func TestChunk_Append_Correct_Index(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	now := uuid.Must(uuid.NewUUID()).Time()

	_, _, err := chunk.append(types.Hash(0), []byte("hello"))
	assert.NoError(t, err)
	id := chunk.index[0]

	expected := (uintptr)(unsafe.Pointer(id))
	actual := (uintptr)(unsafe.Pointer(&chunk.data[0]))

	assert.GreaterOrEqual(t, id.Time(), now, "index entry id should be after now")
	assert.Equal(t, expected, actual)
}

func TestChunk_Append_Many_Correct_Index(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	now, _, _ := uuid.GetTime()
	id, _, _ := chunk.append(types.Hash(0), []byte("hello"))

	assert.Len(t, chunk.index, 4)

	assert.Equal(t, id, *chunk.index[0])
	assert.GreaterOrEqual(t, id.Time(), now, "index entry id should be after now")
}

func TestChunk_Append_Full(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 16+4+5+4),
	}

	_, _, err := chunk.append(types.Hash(0), []byte("hello"))
	assert.Error(t, err, ErrChunkFull)
}

func TestChunk_Append_Many_Full(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, (4+5+32)*3),
	}

	_, _, err := chunk.append(types.Hash(0), []byte("hello"))
	assert.NoError(t, err)
	_, _, err = chunk.append(types.Hash(0), []byte("hello"))
	assert.NoError(t, err)
	_, _, err = chunk.append(types.Hash(0), []byte("hello"))
	assert.NoError(t, err)
	_, _, err = chunk.append(types.Hash(0), []byte("hello"))
	assert.Error(t, err, ErrChunkFull)
}

func TestChunk_Poll(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))

	data, _ := chunk.poll(0, uuid.Nil, 0)

	assert.Len(t, data, (16+4+5)*3)
}

func TestChunk_Poll_Uncomitted_Chunk(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	// 50 bytes written here
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))

	data, _ := chunk.poll(0, uuid.Nil, 2)

	assert.Equal(t, 75, len(data), "should be 50 bytes")
}

func TestChunk_Poll_Mid_Chunk(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	from := uuidutil.Now()
	time.Sleep(time.Microsecond)
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))

	data, _ := chunk.poll(from, uuid.Nil, 0)

	assert.Equal(t, 50, len(data), "should be 50 bytes")
}

func TestChunk_Poll_Mid_Chunk_FromMsgId(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))

	data, _ := chunk.poll(0, *chunk.index[2], 0)

	assert.Equal(t, (16+4+5)*2, len(data))
}

func TestChunk_Poll_From_Now_Nil(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))

	data, _ := chunk.poll(uuidutil.Now(), uuid.Nil, 0)
	assert.Nil(t, data)
}

func TestChunk_Poll_Many_Chunks_Next(t *testing.T) {
	chunk1 := Chunk{
		data: make([]byte, 1000),
	}
	chunk2 := Chunk{
		data: make([]byte, 1000),
		next: &chunk1,
	}

	from := uuidutil.Now() - 1

	// append 25 bytes (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk1.append(types.Hash(0), []byte("hello"))

	// append 50 bytes 2 * (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello"))

	data, _ := chunk2.poll(from, uuid.Nil, 0) // Data returned should be from chunk1
	assert.Equal(t, 25, len(data), "should be 25 bytes")
}

func TestChunk_Poll_Many_Chunks_Next_FromMsgId(t *testing.T) {
	chunk0 := Chunk{
		data: make([]byte, 1000),
	}
	chunk1 := Chunk{
		data: make([]byte, 1000),
		next: &chunk0,
	}
	chunk2 := Chunk{
		data: make([]byte, 1000),
		next: &chunk1,
	}
	// append 26 bytes (16 for uuid, 4 for len prefix, 6 payload)
	_, _, _ = chunk0.append(types.Hash(0), []byte("hello0"))

	// append 25 bytes (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk1.append(types.Hash(0), []byte("hello"))

	// append 52 bytes 2 * (16 for uuid, 4 for len prefix, 6 payload)
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello2"))
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello2"))

	data, _ := chunk2.poll(0, *chunk0.lastMsgId(), 0) // Data returned should be from chunk1
	assert.Len(t, data, 25)
}

func TestChunk_Poll_Many_Chunks_Next_FromMsgId_Nil(t *testing.T) {
	chunk1 := Chunk{
		data: make([]byte, 1000),
	}
	chunk2 := Chunk{
		data: make([]byte, 1000),
		next: &chunk1,
	}
	// append 25 bytes (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk1.append(types.Hash(0), []byte("hello"))

	// append 52 bytes 2 * (16 for uuid, 4 for len prefix, 6 payload)
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello2"))
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello2"))

	data, _ := chunk2.poll(0, uuid.Nil, 0) // Data returned should be from chunk1
	assert.Len(t, data, 25)
}

func TestChunk_Poll_Many_Chunks_Last(t *testing.T) {
	chunk1 := Chunk{
		data: make([]byte, 1000),
	}
	chunk2 := Chunk{
		data: make([]byte, 1000),
		next: &chunk1,
	}

	// append 25 bytes (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk1.append(types.Hash(0), []byte("hello"))
	from := uuidutil.Now()
	time.Sleep(time.Microsecond)
	// append 50 bytes 2 * (16 for uuid, 4 for len prefix, 5 payload)
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk2.append(types.Hash(0), []byte("hello"))

	data, _ := chunk2.poll(from, uuid.Nil, 0) // Data returned should be from chunk1
	assert.Equal(t, 50, len(data), "should be 50 bytes")
}

func TestChunk_pop(t *testing.T) {
	chunk := Chunk{
		data: make([]byte, 1000),
	}

	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	_, _, _ = chunk.append(types.Hash(0), []byte("hello"))
	assert.Len(t, chunk.index, 2)
	chunk.pop()
	assert.Len(t, chunk.index, 1)
}

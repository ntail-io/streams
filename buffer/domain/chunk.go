package domain

import (
	"encoding/binary"
	"sort"
	"unsafe"

	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/core/types"

	"github.com/google/uuid"
)

const indexEntrySize = int(unsafe.Sizeof(uintptr(0)))

type Chunk struct {
	msgLen int
	data   []byte
	next   *Chunk
	index  []*uuid.UUID
}

func (c *Chunk) lastMsgId() *uuid.UUID {
	if len(c.index) == 0 {
		return nil
	}
	return c.index[0]
}

func (c *Chunk) firstMsgId() *uuid.UUID {
	if len(c.index) == 0 {
		return nil
	}
	return c.index[len(c.index)-1]
}

func (c *Chunk) from() uuid.Time {
	if c.firstMsgId() == nil {
		return 0
	}
	return c.firstMsgId().Time()
}

func (c *Chunk) to() uuid.Time {
	if c.lastMsgId() == nil {
		return 0
	}
	return c.lastMsgId().Time()
}

// poll will recursively poll the earliest chunk that is after the given id.
// It will only select data in the chunks that is after the given id.
// If rangeFilter is not nil, it will only select data that is in the given range.
// If the chunk is not found, nil is returned.
func (c *Chunk) poll(from uuid.Time, fromMsgId uuid.UUID, uncommmitted int) (data []byte, lastMsgId uuid.UUID) {
	// Skip chunk early if we know it's not in range.
	if len(c.index) == 0 || len(c.index) <= int(uncommmitted) {
		return
	}

	if c.to() > 0 && c.to() < from {
		return
	}
	if uuidutil.Cmp(*c.lastMsgId(), fromMsgId) <= 0 {
		return
	}

	if c.next != nil {
		nextUncommitted := uncommmitted - len(c.index)
		if nextUncommitted < 0 {
			nextUncommitted = 0
		}
		data, lastMsgId = c.next.poll(from, fromMsgId, nextUncommitted)
		if data != nil {
			return
		}
	}

	// If we can return the entire chunk do so early
	if uncommmitted == 0 && from < c.from() && (uuidutil.Before(fromMsgId, *c.firstMsgId())) {
		return c.data[:c.msgLen], *c.lastMsgId()
	}

	// Exclude uncommitted messages from index
	index := c.index[uncommmitted:]

	// i is the last index to exclude.
	i := sort.Search(len(index), func(i int) bool {
		return !(index[i].Time() > from && uuidutil.After(*index[i], fromMsgId))
	}) // Reverse predict because index is reversed
	if i <= 0 { // Exclude everything
		return
	}

	// Hack: pointer arithmetic to get the data slice from index
	{
		startOfLastMsg := (uintptr)(unsafe.Pointer(index[0]))
		lastMsgLen := (*uint32)(unsafe.Pointer(startOfLastMsg + 16))     // +16 because of the msg id
		endOfLastMsg := startOfLastMsg + 16 + 4 + uintptr(*lastMsgLen)   // +16 for msg id, +4 for the uint32 length prefix
		sliceLen := endOfLastMsg - (uintptr)(unsafe.Pointer(index[i-1])) // End of last msg - beginning of first msg
		data = unsafe.Slice((*byte)(unsafe.Pointer(index[i-1])), sliceLen)
	}

	return data, *index[0]
}

func (c *Chunk) reset() {
	c.msgLen = 0
	c.next = nil
	c.index = nil
}

func (c *Chunk) indexSize() int {
	return len(c.index) * indexEntrySize
}

// appendIndex prepends a pointer to the back end of the chunk that points to the next message.
// This can be used to do binary search on the messages.
func (c *Chunk) appendIndex() {
	indexStartPos := len(c.data) - indexEntrySize*(len(c.index)+1)
	e := (**uuid.UUID)(unsafe.Pointer(&c.data[indexStartPos]))
	c.index = unsafe.Slice(e, len(c.index)+1)
	c.index[0] = (*uuid.UUID)(unsafe.Pointer(&c.data[c.msgLen]))
}

func (c *Chunk) freeSpace() int {
	return len(c.data) - c.msgLen - c.indexSize()
}

func (c *Chunk) append(_ types.Hash, data []byte) (id uuid.UUID, appendedData []byte, _ error) {
	lengthToAdd := len(id) + 4 + len(data) // 16 bytes for the msg id, 4 bytes for the length prefix
	totalLengthToAdd := lengthToAdd + indexEntrySize

	if totalLengthToAdd > len(c.data) {
		return id, nil, ErrTooLargeData
	}

	if c.freeSpace() < totalLengthToAdd {
		return id, nil, ErrChunkFull
	}

	id, _ = uuid.NewUUID()

	c.appendIndex()

	appendedData = c.data[c.msgLen : c.msgLen+lengthToAdd : c.msgLen+lengthToAdd]

	copy(c.data[c.msgLen:], id[:])
	c.msgLen += len(id)

	binary.LittleEndian.PutUint32(c.data[c.msgLen:], uint32(len(data)))
	c.msgLen += 4
	copy(c.data[c.msgLen:], data)
	c.msgLen += len(data)

	return
}

// pop removes last message from the chunk.
func (c *Chunk) pop() {
	c.msgLen = c.lenAt(c.index[0])
	c.index = c.index[1:]
}

// lenAt getes the length of the chunk at a certain id reference. The id must point to a position in the chunk.
func (c *Chunk) lenAt(id *uuid.UUID) int {
	return int(uintptr(unsafe.Pointer(id)) - uintptr(unsafe.Pointer(&c.data[0])))
}

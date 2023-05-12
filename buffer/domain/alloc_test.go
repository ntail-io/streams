package domain

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAllocator_Alloc(t *testing.T) {
	a := NewChunkAllocator(WithAllocatorSize(10 * 9 * 1024 * 1024))
	var c *Chunk
	for i := 0; i < 10; i++ {
		c = a.alloc()
		assert.NotNil(t, c, "alloc returned nil chunk")
	}
	go func() {
		<-a.FullCh
		a.free(c)
	}()
	c = a.alloc()
}

func TestAllocator_Free(t *testing.T) {
	a := NewChunkAllocator()
	c := a.alloc()
	ptr := unsafe.Pointer(c)
	a.free(c)
	c = a.alloc()
	assert.Equal(t, ptr, unsafe.Pointer(c), "alloc should return the same chunk as before")
}

func TestAllocator_HasFree(t *testing.T) {
	a := NewChunkAllocator(WithAllocatorSize(10 * 9 * 1024 * 1024))
	for i := 0; i < 10; i++ {
		assert.True(t, a.HasFree(), "HasFree should return true")
		a.alloc()
	}
	assert.False(t, a.HasFree(), "HasFree should return false")
}

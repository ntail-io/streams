package domain

import (
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var ErrOutOfChunks = errors.New("out of chunks")

const (
	DefaultAllocatorSize   = 100 * 1024 * 1024 // 100 MB
	DefaultFullChThreshold = 0
)

type ChunkAllocatorOption func(*ChunkAllocator)

func WithAllocatorSize(size int) ChunkAllocatorOption {
	return func(a *ChunkAllocator) {
		a.data = make([]byte, size)
	}
}

func WithFullChThreshold(threshold int) ChunkAllocatorOption {
	return func(a *ChunkAllocator) {
		a.FullChThreshold = threshold
	}
}

const ChunkSize = 9 * 1024 * 1024 // 9 MB

type ChunkAllocator struct {
	FullCh          chan struct{}
	data            []byte
	chunks          []Chunk
	allocatedChunks int
	freeChunk       *Chunk
	mu              sync.Mutex
	FullChThreshold int // Number of free chunks to trigger FullCh, should be high in production but can be low in dev and teesting.
}

func NewChunkAllocator(options ...ChunkAllocatorOption) *ChunkAllocator {
	a := &ChunkAllocator{
		FullCh:          make(chan struct{}, 1),
		FullChThreshold: DefaultFullChThreshold,
	}

	for _, option := range options {
		option(a)
	}

	if a.data == nil {
		a.data = make([]byte, DefaultAllocatorSize)
	}

	if a.chunks == nil {
		a.chunks = make([]Chunk, len(a.data)/ChunkSize)
	}

	for i := range a.chunks {
		c := &a.chunks[i]
		c.data = a.data[i*ChunkSize : (i+1)*ChunkSize : (i+1)*ChunkSize]
		if i > 0 {
			c.next = &a.chunks[i-1]
		}
	}
	a.freeChunk = &a.chunks[len(a.chunks)-1]
	return a
}

func (a *ChunkAllocator) HasFree() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.freeChunk != nil
}

func (a *ChunkAllocator) FreePercent() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return float64(len(a.chunks)-a.allocatedChunks) / float64(len(a.chunks))
}

func (a *ChunkAllocator) alloc() *Chunk {
	defer a.gague()
	a.mu.Lock()

	if len(a.chunks)-a.allocatedChunks <= a.FullChThreshold {
		a.mu.Unlock()
		a.FullCh <- struct{}{} // Request for more chunks
		a.mu.Lock()
	}

	if a.freeChunk == nil {
		a.mu.Unlock()
		log.Warn("out of chunks in chunk allocator, probably due to a lot of concurrent calls to alloc(). Will sleep for 1ms and try to alloc again")
		time.Sleep(time.Millisecond)
		return a.alloc()
	}

	a.allocatedChunks++

	c := a.freeChunk
	a.freeChunk = c.next
	c.next = nil
	a.mu.Unlock()
	return c
}

func (a *ChunkAllocator) free(c *Chunk) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c.reset()
	c.next = a.freeChunk
	a.freeChunk = c
	a.allocatedChunks--
}

// Gagues for prometheus metrics
var (
	gaugeAllocator               = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "chunk_allocator"}, []string{"type"})
	gaugeAllocatorFreePercent    = gaugeAllocator.WithLabelValues("free_percent")
	gaugeAllocatorAllocated      = gaugeAllocator.WithLabelValues("allocated")
	gaugeAllocatorFree           = gaugeAllocator.WithLabelValues("free")
	gaugeAllocatorFreeBytes      = gaugeAllocator.WithLabelValues("free_bytes")
	gaugeAllocatorAllocatedBytes = gaugeAllocator.WithLabelValues("allocted_bytes")
)

func (a *ChunkAllocator) gague() {
	gaugeAllocatorFreePercent.Set(a.FreePercent())
	gaugeAllocatorAllocated.Set(float64(a.allocatedChunks))
	gaugeAllocatorFree.Set(float64(len(a.chunks) - a.allocatedChunks))
	gaugeAllocatorFreeBytes.Set(float64((len(a.chunks) - a.allocatedChunks) * ChunkSize))
	gaugeAllocatorAllocatedBytes.Set(float64(a.allocatedChunks * ChunkSize))
}

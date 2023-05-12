package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ntail-io/streams/buffer/buftypes"
	"github.com/ntail-io/streams/buffer/commands"
	"github.com/ntail-io/streams/buffer/domain"
	"github.com/ntail-io/streams/buffer/etcd/repo"
	"github.com/ntail-io/streams/core/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/google/uuid"
)

var (
	ErrNotFound                 = errors.New("not found")
	ErrNotFoundInconsistentData = errors.New("not found, might be missing data")
	ErrInvalidSegmentAddress    = errors.New("invalid segment address")
)

// GW
// Route to a lease based on segment hash range
// Route to a lease based on leased  hash range
// When a lease is lost close leases (and segment)
// Create a lease on demand

// Lease Handler
// Wraps the SegmentHandler with leasing logic
// If a msg is within lease range but out of segment range, recreate segment.
// If a msg is within lease range and segment range, append.

type Router struct {
	Ctx                context.Context
	Allocator          *domain.ChunkAllocator
	SegmentRepo        *repo.SegmentRepo
	unfinishedSegments map[types.Topic]map[types.HashRange]chan commands.Command
	finishedSegmentsCh chan *buftypes.SegmentAddress
	hashRangeSizes     map[types.Topic]types.HashRangeSize
	segments           map[buftypes.SegmentAddress]chan commands.Command
	segmentsByTime     map[types.Topic][]buftypes.SegmentAddress
	mu                 sync.RWMutex
}

func NewRouter(ctx context.Context, segmentRepo *repo.SegmentRepo, allocator *domain.ChunkAllocator, finishedSegmentsCh chan *buftypes.SegmentAddress) *Router {
	return &Router{
		Ctx:                ctx,
		Allocator:          allocator,
		segments:           make(map[buftypes.SegmentAddress]chan commands.Command),
		segmentsByTime:     make(map[types.Topic][]buftypes.SegmentAddress),
		unfinishedSegments: make(map[types.Topic]map[types.HashRange]chan commands.Command),
		finishedSegmentsCh: finishedSegmentsCh,
		hashRangeSizes:     make(map[types.Topic]types.HashRangeSize),
		SegmentRepo:        segmentRepo,
	}
}

func (r *Router) FinishSegments(topic types.Topic) {
	defer r.gagueSegments()
	r.mu.Lock()

	unfinishedSegments, ok := r.unfinishedSegments[topic]
	if !ok {
		r.mu.Unlock()
		return
	}

	ch := make(chan *commands.Finish, len(unfinishedSegments))
	for _, cmdCh := range unfinishedSegments {
		cmdCh <- &commands.Finish{
			ResCh: ch,
		}
	}
	r.mu.Unlock()
	for i := 0; i < cap(ch); i++ {
		<-ch
	}
}

func (r *Router) PutHashRangeSize(topic types.Topic, size types.HashRangeSize) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hashRangeSizes[topic] = size
}

func (r *Router) DeleteTopic(topic types.Topic, size types.HashRangeSize) {
	r.mu.Lock()
	defer r.mu.Unlock()
	panic("implement me")
}

func (r *Router) Get(ctx context.Context, cmd *commands.Get) (err error) {
	var earliestFrom uuid.Time

	defer func() {
		if err == ErrNotFound && (earliestFrom == 0 || earliestFrom > cmd.From) {
			err = ErrNotFoundInconsistentData
		}
	}()

	r.mu.RLock()

	segs, ok := r.segmentsByTime[cmd.Topic]
	if !ok {
		r.mu.RUnlock()
		return ErrNotFound
	}

	channels := make([]chan commands.Command, 0, 16)

	for i := len(segs) - 1; i >= 0; i-- {
		seg := &segs[i]
		earliestFrom = seg.From

		if segs[i].From >= cmd.From && seg.HashRange.Contains(cmd.Hash) {
			channels = append(channels, r.segments[*seg])
		}
	}
	r.mu.RUnlock()

	for _, ch := range channels {
		ch <- cmd
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cmd.ResCh:
			if cmd.Err != nil {
				return cmd.Err
			}
			if cmd.Ok {
				return nil
			}
		}
	}

	return ErrNotFound
}

func (r *Router) Append(cmd *commands.Append) (err error) {
	var cmdCh chan commands.Command
	for try := 0; try < 3; try++ {
		cmdCh, err = r.apiOfHash(cmd.Topic, cmd.Hash)
		if err != nil {
			if try < 2 {
				time.Sleep(time.Millisecond * 1)
				continue
			}
			return
		}
		break
	}

	cmdCh <- cmd
	return nil
}

func (r *Router) CmdCh(addr buftypes.SegmentAddress) (chan commands.Command, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	api, ok := r.segments[addr]
	if !ok {
		return api, ErrInvalidSegmentAddress
	}
	return api, nil
}

func (r *Router) Poll(ctx context.Context, cmd *commands.Poll) error {
	ch, err := r.CmdCh(cmd.Addr)
	if err != nil {
		return err
	}
	ch <- cmd

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cmd.ResCh:
		return cmd.Err
	}
}

func (r *Router) CloseSegment(addr buftypes.SegmentAddress) error {
	defer r.gagueSegments()
	r.mu.Lock()
	defer r.mu.Unlock()
	cmdCh, ok := r.segments[addr]
	if !ok {
		return ErrNotFound
	}
	delete(r.segments, addr)
	for i := range r.segmentsByTime[addr.Topic] {
		// Remove segment
		if r.segmentsByTime[addr.Topic][i] == addr {
			r.segmentsByTime[addr.Topic] = append(r.segmentsByTime[addr.Topic][:i], r.segmentsByTime[addr.Topic][i+1:]...)
			break
		}
	}
	close(cmdCh)
	return nil
}

func (r *Router) PutSegment(addr buftypes.SegmentAddress, cmdCh chan commands.Command) {
	defer r.gagueSegments()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segments[addr] = cmdCh

	if _, ok := r.unfinishedSegments[addr.Topic]; !ok {
		r.unfinishedSegments[addr.Topic] = make(map[types.HashRange]chan commands.Command)
	}

	r.unfinishedSegments[addr.Topic][addr.HashRange] = cmdCh
	r.segmentsByTime[addr.Topic] = append(r.segmentsByTime[addr.Topic], addr)
}

func (r *Router) SegmentFinished(topic types.Topic, hr types.HashRange, from uuid.Time) {
	defer r.gagueSegments()
	r.mu.Lock()
	defer r.mu.Unlock()

	topicLeases, ok := r.unfinishedSegments[topic]
	if !ok {
		return
	}
	delete(topicLeases, hr)

	r.finishedSegmentsCh <- &buftypes.SegmentAddress{
		Topic:     topic,
		HashRange: hr,
		From:      from,
	}
}

func (r *Router) apiOfHash(topic types.Topic, hash types.Hash) (chan commands.Command, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	hr := hash.HashRange(r.hashRangeSizes[topic])

	m, ok := r.unfinishedSegments[topic]
	if !ok {
		return nil, ErrInvalidSegmentAddress
	}
	cmdCh, ok := m[hr]
	if !ok {
		return nil, ErrInvalidSegmentAddress
	}
	return cmdCh, nil
}

var (
	gaugeSegments         = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "segments"}, []string{"type"})
	gagueSegmentsTotal    = gaugeSegments.WithLabelValues("total")
	gagueSegmentsFinished = gaugeSegments.WithLabelValues("finished")
)

func (r *Router) gagueSegments() {
	gagueSegmentsTotal.Set(float64(len(r.segments)))
	gagueSegmentsFinished.Set(float64(len(r.unfinishedSegments)))
}

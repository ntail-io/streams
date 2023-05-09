package domain

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ntail-io/streams/internal/core/types"
)

var (
	ErrBufferAddressNotFound = errors.New("buffer address not found")
	ErrInvalidSegmentId      = errors.New("invalid segment id")
	ErrNoBookmarksAvailable  = errors.New("no bookmarks available")
	ErrSubscriptionNotFound  = errors.New("subscription not found")
)

type BufferAddress string

type AdressType uint32

const (
	BufferAddressType AdressType = iota
	BigQueryAddressType
)

type SegmentAddress struct {
	SegmentId SegmentId
	Type      AdressType
	Address   string
}

type Topic struct {
	Name            types.Topic
	hashRangeSize   types.HashRangeSize
	head            []*Segment
	segments        []*Segment
	bufferAddresses map[types.HashRange]BufferAddress
	segM            map[SegmentId]*Segment
	subscriptions   map[types.SubscriptionId]*Subscription
	mu              sync.RWMutex
}

func NewTopic(name types.Topic, size types.HashRangeSize) *Topic {
	return &Topic{
		Name:            name,
		hashRangeSize:   size,
		bufferAddresses: make(map[types.HashRange]BufferAddress),
		segM:            make(map[SegmentId]*Segment),
		subscriptions:   make(map[types.SubscriptionId]*Subscription),
	}
}

func (t *Topic) HashRangeSize(hrs types.HashRangeSize) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hashRangeSize = hrs
}

func (t *Topic) Heads() (s []SegmentId) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s = make([]SegmentId, len(t.head))
	for i := range t.head {
		s[i] = t.head[i].Id
	}
	return s
}

func (t *Topic) Subscriptions() (s []types.SubscriptionId) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	s = make([]types.SubscriptionId, 0, len(t.subscriptions))
	for _, sub := range t.subscriptions {
		s = append(s, sub.Id)
	}
	return s
}

func (t *Topic) AcquireBookmark(subscriptionId types.SubscriptionId) (bm *Bookmark, addr *SegmentAddress, new []*Bookmark, deletions []*Bookmark, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	sub, ok := t.subscriptions[subscriptionId]
	if !ok {
		err = ErrSubscriptionNotFound
		return
	}

	new, deletions, ok = sub.acquireBookmark()
	if !ok {
		err = ErrNoBookmarksAvailable
		return
	}

	bm = new[0]
	new = new[1:]
	addr = &bm.segment.Address
	return
}

func (t *Topic) AddressOfHash(hash types.Hash) (BufferAddress, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	hr := hash.HashRange(t.hashRangeSize)
	addr, ok := t.bufferAddresses[hr]
	if !ok {
		return "", ErrBufferAddressNotFound
	}
	return addr, nil
}

func (t *Topic) UpdateSubscription(s *Subscription) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.subscriptions[s.Id]; ok {
		return
	}

	t.subscriptions[s.Id] = s
}

func (t *Topic) UpdateBookmarkLeased(subId types.SubscriptionId, segId SegmentId, leased bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	sub, ok := t.subscriptions[subId]
	if !ok {
		panic(fmt.Sprintf("subscription not found: %s", subId))
	}

	b, ok := sub.BookmarksMap[segId]
	if !ok {
		panic(fmt.Sprintf("bookmark not found: %s", segId.String()))
	}

	b.Leased = leased
}

func (t *Topic) UpdateBookmark(subscriptionId types.SubscriptionId, segmentId SegmentId, msgId uuid.UUID, finished bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	sub, ok := t.subscriptions[subscriptionId]
	if !ok {
		panic("subscription not found")
	}

	segment, ok := t.segM[segmentId]
	if !ok {
		panic("segment not found")
	}

	if bm, ok := sub.BookmarksMap[segmentId]; ok {
		bm.MsgId = msgId
		bm.Finished = finished
		return
	}

	b := &Bookmark{
		MsgId:          msgId,
		Finished:       finished,
		SegmentId:      segment.Id,
		SubscriptionId: subscriptionId,
		segment:        segment,
	}

	sub.Bookmarks = append(sub.Bookmarks, b)
	sub.BookmarksMap[segmentId] = b
}

func (t *Topic) DeleteBookmark(subscriptionId types.SubscriptionId, segmentId SegmentId) {
	t.mu.Lock()
	defer t.mu.Unlock()

	sub, ok := t.subscriptions[subscriptionId]
	if !ok {
		panic("subscription not found")
	}

	bm, ok := sub.BookmarksMap[segmentId]
	if !ok {
		return
	}

	// TODO SLow!
	for i := range sub.Bookmarks {
		if sub.Bookmarks[i] == bm {
			sub.Bookmarks = append(sub.Bookmarks[:i], sub.Bookmarks[i+1:]...)
			delete(sub.BookmarksMap, segmentId)
			return
		}
	}
}

func (t *Topic) CloseSegment(id SegmentId) {
	t.mu.Lock()
	defer t.mu.Unlock()

	seg, ok := t.segM[id]
	if !ok {
		return
	}

	seg.Address = SegmentAddress{
		SegmentId: id,
		Type:      BigQueryAddressType,
		Address:   fmt.Sprintf("%s.%s", id.Topic, id.BigQueryTable()),
	}
}

func (t *Topic) PutSegment(s *Segment, override bool) (head bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.segM[s.Id]; ok {
		if override {
			old.Address = s.Address
		}
		return old.Prev == nil
	}

	if s.Prev != nil || s.Next != nil {
		panic("prev and next should be nil")
	}

	remainingSize := s.Id.HashRange.Size()
	for i := len(t.segments) - 1; i >= 0; i-- {
		seg := t.segments[i]

		if !seg.isPrev(s) {
			continue
		}

		s.Prev = append(s.Prev, seg)
		seg.Next = append(seg.Next, s)

		remainingSize -= seg.Id.HashRange.Size()
		if remainingSize <= 0 {
			break
		}
	}

	t.segments = append(t.segments, s)
	s = t.segments[len(t.segments)-1]

	t.segM[s.Id] = s

	if s.Prev == nil {
		t.head = append(t.head, s)
	}

	return s.Prev == nil
}

func (t *Topic) PutBufferAddress(hr types.HashRange, address BufferAddress) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.bufferAddresses[hr] = address
}

func (t *Topic) DeleteBufferAddress(hr types.HashRange) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.bufferAddresses[hr]; !ok {
		return
	}

	delete(t.bufferAddresses, hr)
}

type SegmentId struct {
	Topic     types.Topic
	HashRange types.HashRange
	TimeFrom  uuid.Time
}

func ParseSegmentId(topic types.Topic, str string) (id SegmentId, err error) {
	if _, err = fmt.Sscanf(str, "%d_%d-%d", &id.TimeFrom, &id.HashRange.From, &id.HashRange.To); err != nil {
		return id, ErrInvalidSegmentId
	}
	id.Topic = topic
	return
}

func (id *SegmentId) String() string {
	return fmt.Sprintf("%d_%d-%d", id.TimeFrom, id.HashRange.From, id.HashRange.To)
}

func (id *SegmentId) BigQueryTable() string {
	return fmt.Sprintf("seg_%d_%d_%d", id.TimeFrom, id.HashRange.From, id.HashRange.To)
}

type Segment struct {
	Id      SegmentId
	Address SegmentAddress
	Prev    []*Segment
	Next    []*Segment
}

func NewBigQuerySegment(id SegmentId) *Segment {
	return &Segment{
		Id: id,
		Address: SegmentAddress{
			SegmentId: id,
			Type:      BigQueryAddressType,
			Address:   fmt.Sprintf("%s.%s", id.Topic, id.BigQueryTable()),
		},
	}
}

func NewBufferSegment(id SegmentId, addr BufferAddress) *Segment {
	return &Segment{
		Id: id,
		Address: SegmentAddress{
			SegmentId: id,
			Type:      BufferAddressType,
			Address:   string(addr),
		},
	}
}

func (s *Segment) isPrev(next *Segment) bool {
	return s.Id.TimeFrom < next.Id.TimeFrom && s.Id.HashRange.Overlaps(next.Id.HashRange)
}

type Bookmark struct {
	MsgId          uuid.UUID
	Leased         bool
	SegmentId      SegmentId
	SubscriptionId types.SubscriptionId
	Finished       bool
	segment        *Segment // segment is a pointer should not be used outside the domain (with mutex), therefore it is private.
}

type Subscription struct {
	Id           types.SubscriptionId
	Topic        types.Topic
	BookmarksMap map[SegmentId]*Bookmark
	Bookmarks    []*Bookmark
}

func NewSubscription(id types.SubscriptionId, topic types.Topic) *Subscription {
	return &Subscription{
		Id:           id,
		Topic:        topic,
		BookmarksMap: make(map[SegmentId]*Bookmark),
	}
}

func (s *Subscription) acquireBookmark() (bookmarks []*Bookmark, deletions []*Bookmark, ok bool) {
	for _, b := range s.Bookmarks {
		if b.Leased {
			continue
		}
		if b.Finished {
			if bookmarks, deletions, ok = s.next(b); !ok {
				continue
			}
			return
		}
		bookmarks = make([]*Bookmark, 1)
		bookmarks[0] = b
		return bookmarks, deletions, true
	}
	return
}

func (s *Subscription) next(bm *Bookmark) (_ []*Bookmark, prev []*Bookmark, ok bool) {
	bookmarks, prev, ok := next(bm, s.BookmarksMap)
	if !ok {
		return
	}

	return bookmarks, prev, true
}

func next(b *Bookmark, bookmarks map[SegmentId]*Bookmark) (bms []*Bookmark, prev []*Bookmark, ok bool) {
	if !b.Finished {
		panic("bookmark not finished")
	}
	if len(b.segment.Next) == 0 {
		return
	}

	bms = make([]*Bookmark, len(b.segment.Next))

	for i, next := range b.segment.Next {
		prev = make([]*Bookmark, 0, len(next.Prev))
		for _, seg := range next.Prev {
			bm, ok2 := bookmarks[seg.Id]
			if !ok2 || !bm.Finished {
				prev = nil
				return
			}
			prev = append(prev, bm)
		}
		bms[i] = &Bookmark{
			MsgId:          uuid.Nil,
			segment:        next,
			SubscriptionId: b.SubscriptionId,
			SegmentId:      next.Id,
		}
	}

	return bms, prev, true
}

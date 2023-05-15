package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway"
	"github.com/ntail-io/streams/gateway/domain"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	prefixTopics          = "/topic_range_sizes/"
	prefixBufferSegments  = "/buffer_segments/"
	prefixSegments        = "/segments/"
	prefixSubs            = "/subscriptions/"
	prefixBookmarks       = "/bookmarks/"
	prefixBookmarkLeases  = "/bookmark_leases/"
	prefixBufferAddresses = "/hash_range_leases/"
)

type Watcher struct {
	client       *clientv3.Client
	topicService *gateway.TopicService
}

func NewWatcher(client *clientv3.Client, service *gateway.TopicService) *Watcher {
	return &Watcher{
		client:       client,
		topicService: service,
	}
}

func (w *Watcher) Run(ctx context.Context, errCh chan error, restoredCh chan struct{}) {
	var err error
	defer func() {
		log.Info("closing etcd watcher")
		if err != nil {
			errCh <- err
		}
	}()
	ctx, cnl := context.WithCancel(ctx)
	defer cnl()
	ch := w.client.Watch(ctx, "/", clientv3.WithPrefix(), clientv3.WithCreatedNotify())

	for {
		select {
		case <-ctx.Done():
			return
		case res := <-ch:
			if err = res.Err(); err != nil {
				return
			}
			if res.Created {
				if err = w.restoreTopics(ctx); err != nil {
					return
				}
				if err = w.restoreBufferSegments(ctx); err != nil {
					return
				}
				if err = w.restoreSegments(ctx); err != nil {
					return
				}
				if err = w.restoreBufferAddresses(ctx); err != nil {
					return
				}
				if err = w.restoreSubscriptions(ctx); err != nil {
					return
				}
				if err = w.restoreBookmarks(ctx); err != nil {
					return
				}
				if err = w.restoreBookmarkLeases(ctx); err != nil {
					return
				}
				restoredCh <- struct{}{}
				continue
			}
			for _, ev := range res.Events {
				prefix := string(ev.Kv.Key)
				switch ev.Type {
				case mvccpb.PUT:
					switch {
					case strings.HasPrefix(prefix, prefixTopics):
						err = w.handleTopicKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixSegments):
						err = w.handleSegmentKv(ctx, ev.Kv)
					case strings.HasPrefix(prefix, prefixBufferSegments):
						err = w.handleBufferSegmentKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixBufferAddresses):
						err = w.handleBufferAddressKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixSubs):
						err = w.handleSubscriptionKv(ctx, ev.Kv)
					case strings.HasPrefix(prefix, prefixBookmarks):
						err = w.handleBookmarkKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixBookmarkLeases):
						err = w.handleBookmarkLeaseKv(ev.Kv)
					}
				case mvccpb.DELETE:
					switch {
					case strings.HasPrefix(prefix, prefixBufferSegments):
						err = w.deleteBufferSegmentKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixBookmarkLeases):
						err = w.handleBookmarkLeaseKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixBufferAddresses):
						err = w.deleteBufferAddressKv(ev.Kv)
					case strings.HasPrefix(prefix, prefixBookmarks):
						err = w.deleteBookmarkKv(ev.Kv)
					default:
						panic("delete not implemented")
					}
				}
				if err != nil {
					return
				}
			}
		}
	}
}

func (w *Watcher) restoreTopics(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixTopics, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get topics: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleTopicKv(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreBufferSegments(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixBufferSegments, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get open segments: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleBufferSegmentKv(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreSegments(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixSegments, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleSegmentKv(ctx, kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreBufferAddresses(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixBufferAddresses, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get buffer addresses: %w", err)
	}
	for _, kv := range res.Kvs {
		if err = w.handleBufferAddressKv(kv); err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreSubscriptions(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixSubs, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get subscriptions: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleSubscriptionKv(ctx, kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreBookmarks(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixBookmarks, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get bookmarks: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleBookmarkKv(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) restoreBookmarkLeases(ctx context.Context) error {
	res, err := w.client.Get(ctx, prefixBookmarkLeases, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get bookmark leasess: %w", err)
	}
	for _, kv := range res.Kvs {
		err = w.handleBookmarkLeaseKv(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) handleTopicKv(kv *mvccpb.KeyValue) (err error) {
	name, err := parseTopicKey(string(kv.Key))
	if err != nil {
		return
	}

	size, err := types.ParseHashRangeSize(string(kv.Value))
	if err != nil {
		return
	}

	w.topicService.Add(name, size)
	return
}

func (w *Watcher) handleBufferSegmentKv(kv *mvccpb.KeyValue) (err error) {
	segId, err := parseBufferSegmentKey(string(kv.Key))
	if err != nil {
		return err
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	seg := domain.NewBufferSegment(segId, domain.BufferAddress(kv.Value))
	t.PutSegment(seg, true)
	return
}

func (w *Watcher) deleteBufferSegmentKv(kv *mvccpb.KeyValue) (err error) {
	segId, err := parseBufferSegmentKey(string(kv.Key))
	if err != nil {
		return err
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	t.CloseSegment(segId)
	return
}

func (w *Watcher) handleSegmentKv(ctx context.Context, kv *mvccpb.KeyValue) (err error) {
	segId, err := parseSegmentKey(string(kv.Key))
	if err != nil {
		return err
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	seg := domain.NewBigQuerySegment(segId)
	if head := t.PutSegment(seg, false); head {
		for _, subId := range t.Subscriptions() {
			bm := domain.Bookmark{
				MsgId:          uuid.Nil,
				SegmentId:      seg.Id,
				SubscriptionId: subId,
			}
			if err = saveHeadBookmark(ctx, w.client, &bm); err != nil {
				return err
			}
		}
	}

	return
}

func (w *Watcher) handleBufferAddressKv(kv *mvccpb.KeyValue) (err error) {
	topic, hr, err := parseBufferAddressKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(topic)
	if err != nil {
		return
	}

	addr := domain.BufferAddress(kv.Value)
	t.PutBufferAddress(hr, addr)

	return
}

func (w *Watcher) handleSubscriptionKv(ctx context.Context, kv *mvccpb.KeyValue) (err error) {
	topic, id, err := parseSubscriptionKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(topic)
	if err != nil {
		return
	}

	sub := domain.NewSubscription(id, topic)
	t.UpdateSubscription(sub)

	for _, segId := range t.Heads() {
		bm := domain.Bookmark{
			MsgId:          uuid.Nil,
			SegmentId:      segId,
			SubscriptionId: id,
		}
		if err = saveHeadBookmark(ctx, w.client, &bm); err != nil {
			return
		}
	}

	return
}

func (w *Watcher) handleBookmarkKv(kv *mvccpb.KeyValue) (err error) {
	subId, segId, err := parseBookmarkKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	val := bookmarkData{}
	if err = json.Unmarshal(kv.Value, &val); err != nil {
		return
	}

	msgId, err := uuid.Parse(val.MsgId)
	if err != nil {
		return
	}

	t.UpdateBookmark(subId, segId, msgId, val.Finished)
	return
}

func (w *Watcher) handleBookmarkLeaseKv(kv *mvccpb.KeyValue) (err error) {
	subId, segId, err := parseBookmarkLeaseKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	t.UpdateBookmarkLeased(subId, segId, kv.Version > 0)
	return
}

func (w *Watcher) deleteBufferAddressKv(kv *mvccpb.KeyValue) (err error) {
	topic, hr, err := parseBufferAddressKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(topic)
	if err != nil {
		return
	}

	t.DeleteBufferAddress(hr)
	return
}

func (w *Watcher) deleteBookmarkKv(kv *mvccpb.KeyValue) (err error) {
	subId, segId, err := parseBookmarkKey(string(kv.Key))
	if err != nil {
		return
	}

	t, err := w.topicService.Get(segId.Topic)
	if err != nil {
		return
	}

	t.DeleteBookmark(subId, segId)
	return
}

func parseTopicKey(key string) (types.Topic, error) {
	split := strings.Split(key, "/")
	if len(split) != 3 {
		return "", fmt.Errorf("invalid topic key %s", key)
	}
	return types.Topic(split[2]), nil
}

func parseBufferSegmentKey(key string) (id domain.SegmentId, err error) {
	split := strings.SplitN(key, "/", 4)
	if len(split) != 4 {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	id.Topic = types.Topic(split[2])

	split = strings.SplitN(split[3], "_", 2)
	if len(split) != 2 {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	timeFrom, err := strconv.ParseInt(split[0], 10, 64)
	if err != nil {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	id.TimeFrom = uuid.Time(timeFrom)
	id.HashRange, err = types.ParseHashRange(split[1])
	return
}

func parseSegmentKey(key string) (id domain.SegmentId, err error) {
	split := strings.SplitN(key, "/", 4)
	if len(split) != 4 {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	id.Topic = types.Topic(split[2])

	split = strings.SplitN(split[3], "_", 2)
	if len(split) != 2 {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	timeFrom, err := strconv.ParseInt(split[0], 10, 64)
	if err != nil {
		return id, fmt.Errorf("invalid segment key %s", key)
	}
	id.TimeFrom = uuid.Time(timeFrom)
	id.HashRange, err = types.ParseHashRange(split[1])
	return
}

func parseBufferAddressKey(key string) (topic types.Topic, hr types.HashRange, err error) {
	split := strings.Split(key, "/")
	if len(split) != 4 {
		err = fmt.Errorf("invalid buffer address key %s", key)
		return
	}

	topic = types.Topic(split[2])
	hr, err = types.ParseHashRange(split[3])

	return
}

func parseSubscriptionKey(key string) (topic types.Topic, id types.SubscriptionId, err error) {
	split := strings.Split(key, "/") // /subscriptions/{topic}/{subscriptionId}
	if len(split) != 4 {
		err = fmt.Errorf("invalid subscription key %s", key)
		return
	}

	topic = types.Topic(split[2])
	id = types.SubscriptionId(split[3])

	return
}

func parseBookmarkKey(key string) (subId types.SubscriptionId, segId domain.SegmentId, err error) {
	split := strings.Split(key, "/") // /bookmarks/{topic}/{subscriptionId}/{segmentId}
	if len(split) != 5 {
		err = fmt.Errorf("invalid bookmark key %s", key)
		return
	}

	topic := types.Topic(split[2])

	subId = types.SubscriptionId(split[3])
	segId, err = domain.ParseSegmentId(topic, split[4])

	return
}

func parseBookmarkLeaseKey(key string) (subId types.SubscriptionId, segId domain.SegmentId, err error) {
	split := strings.Split(key, "/") // /bookmark_leases/{topic}/{subscriptionId}/{segmentId}
	if len(split) != 5 {
		err = fmt.Errorf("invalid bookmark key %s", key)
		return
	}

	topic := types.Topic(split[2])
	subId = types.SubscriptionId(split[3])
	segId, err = domain.ParseSegmentId(topic, split[4])

	return
}

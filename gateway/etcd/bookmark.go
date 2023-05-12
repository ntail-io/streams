package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/ntail-io/streams/gateway/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func bookmarkKey(b *domain.Bookmark) string {
	// /bookmarks/{topic}/{subscriptionId}/{segmentId}
	return fmt.Sprintf("%s%s/%s/%s", prefixBookmarks, b.SegmentId.Topic, b.SubscriptionId, b.SegmentId.String())
}

func bookmarkLeaseKey(b *domain.Bookmark) string {
	// /bookmarks/{topic}/{subscriptionId}/{segmentId}
	return fmt.Sprintf("%s%s/%s/%s", prefixBookmarkLeases, b.SegmentId.Topic, b.SubscriptionId, b.SegmentId.String())
}

func headBookmark(b *domain.Bookmark) string {
	// /head_bookmarks/{topic}/{subscriptionId}/{segmentId}
	return fmt.Sprintf("/head_bookmarks/%s/%s/%s", b.SegmentId.Topic, b.SubscriptionId, b.SegmentId.String())
}

type bookmarkData struct {
	MsgId    string
	Finished bool
}

func saveBookmark(ctx context.Context, sess *concurrency.Session, b *domain.Bookmark) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to save bookmark: %w", err)
		}
	}()

	val := bookmarkData{
		MsgId:    b.MsgId.String(),
		Finished: b.Finished,
	}

	bytes, err := json.Marshal(val)

	res, err := sess.Client().Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(bookmarkLeaseKey(b)), "=", sess.Lease())).
		Then(clientv3.OpPut(bookmarkKey(b), string(bytes))).
		Commit()

	if !res.Succeeded {
		return ErrMissingLease
	}

	return
}

var nilBookmark, _ = json.Marshal(bookmarkData{MsgId: uuid.Nil.String()})

func saveHeadBookmark(ctx context.Context, client *clientv3.Client, b *domain.Bookmark) error {
	// head bookmarks need to be stored in etcd to ensure we do not have race conditions.
	_, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(headBookmark(b)), "=", 0)).
		Then(
			clientv3.OpPut(headBookmark(b), ""),
			clientv3.OpPut(bookmarkKey(b), string(nilBookmark)),
		).
		Commit()
	return err
}

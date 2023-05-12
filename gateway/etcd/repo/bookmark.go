package repo

import (
	"context"
	"fmt"

	"github.com/ntail-io/streams/gateway/domain"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type BookmarkRepo struct {
	client *clientv3.Client
}

func NewBookmarkRepo(client *clientv3.Client) *BookmarkRepo {
	return &BookmarkRepo{
		client: client,
	}
}

func (r *BookmarkRepo) Save(ctx context.Context, bookmark *domain.Bookmark, sub *domain.Subscription) error {
	str, err := bookmark.MsgId.MarshalText()
	if err != nil {
		return err
	}

	_, err = r.client.Put(ctx, bookmarkKey(bookmark, sub), string(str))
	return err
}

func bookmarkKey(bookmark *domain.Bookmark, sub *domain.Subscription) string {
	// /bookmarks/{topic}/{subscriptionId}/{segmentId}
	return fmt.Sprintf("/bookmarks/%s/%s/%v", sub.Topic, sub.Id, bookmark.SegmentId)
}

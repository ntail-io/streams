package repo

import (
	"context"
	"fmt"

	"github.com/ntail-io/streams/internal/gateway/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type SubscriptionRepo struct {
	client *clientv3.Client
}

func NewSubscriptionRepo(client *clientv3.Client) *SubscriptionRepo {
	return &SubscriptionRepo{
		client: client,
	}
}

func (r *SubscriptionRepo) Save(ctx context.Context, subscription *domain.Subscription) error {
	_, err := r.client.Put(ctx, subscriptionKey(subscription, subscription), "1")
	return err
}

func subscriptionKey(subscription *domain.Subscription, sub *domain.Subscription) string {
	// /subscriptions/{topic}/{subscriptionId}
	return fmt.Sprintf("/subscriptions/%s/%s", sub.Topic, sub.Id)
}

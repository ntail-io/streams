package etcd

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ntail-io/streams/core/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrAlreadyExist = errors.New("already exists")
	ErrNotFound     = errors.New("not found")
)

func CreateTopic(ctx context.Context, client *clientv3.Client, topic types.Topic, size types.HashRangeSize) error {
	key := fmt.Sprintf("%s%s", prefixTopics, topic)
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, strconv.Itoa(int(size)))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("topic %s already exists: %w", topic, ErrAlreadyExist)
	}
	return nil
}

func CreateSubscription(ctx context.Context, client *clientv3.Client, topic types.Topic, id types.SubscriptionId) error {
	topicKey := fmt.Sprintf("%s%s", prefixTopics, topic)
	subKey := fmt.Sprintf("%s%s/%s", prefixSubs, topic, id)

	// Check if topic exists before creating subscription, this is only to get better error message
	resp, err := client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Version(topicKey), ">", 0),
		).
		//Then().
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("topic %s does not exist: %w", topic, ErrNotFound)
	}

	resp, err = client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Version(topicKey), ">", 0),
			clientv3.Compare(clientv3.Version(subKey), "=", 0),
		).
		Then(clientv3.OpPut(subKey, "")).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("subscription %s already exists: %w", id, ErrAlreadyExist)
	}
	return nil
}

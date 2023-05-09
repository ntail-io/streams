package etcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ntail-io/streams/internal/buffer"
	"github.com/ntail-io/streams/internal/buffer/cmdhandler"
	"github.com/ntail-io/streams/internal/core/types"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaseService struct {
	Sess     *concurrency.Session
	Addr     string
	Router   *buffer.Router
	SegProps cmdhandler.SegmentServiceProps
}

func NewLeaseService(ctx context.Context, addr string, sess *concurrency.Session, segProps cmdhandler.SegmentServiceProps) *LeaseService {
	service := &LeaseService{
		Addr:     addr,
		Sess:     sess,
		Router:   segProps.Router,
		SegProps: segProps,
	}

	readyCh := make(chan struct{})
	go service.hashRangeSizeWatcher(ctx, readyCh)
	go service.leaseWatcher(ctx)
	<-readyCh

	return service
}

func (s *LeaseService) Close() error {
	return s.Sess.Close()
}

func (s *LeaseService) closeLeases(ctx context.Context, topic types.Topic) {
	log.Infof("closing leases for topic [%s]", topic)
	s.Router.FinishSegments(topic)

	resp, err := s.Sess.Client().Get(ctx, fmt.Sprintf("/hash_range_leases/%s", topic), clientv3.WithPrefix())
	if err != nil {
		// TODO Retry
		log.WithError(err).Errorf("failed to get hash range leases for topic [%s]", topic)
		return
	}

	cmps := make([]clientv3.Cmp, 0, 16)
	ops := make([]clientv3.Op, 0, 16)
	for _, kv := range resp.Kvs {
		if kv.Lease != int64(s.Sess.Lease()) {
			continue
		}
		cmps = append(cmps, clientv3.Compare(clientv3.LeaseValue(string(kv.Key)), "=", s.Sess.Lease()))
		ops = append(ops, clientv3.OpDelete(string(kv.Key)))
	}
	if _, err := s.Sess.Client().Txn(ctx).If(cmps...).Then(ops...).Commit(); err != nil {
		log.WithError(err).Errorf("failed to delete hash range leases for topic [%s]", topic)
		return
	}
	log.Infof("removed leases on topic [%s]", topic)
}

func (s *LeaseService) tryLease(ctx context.Context, topic types.Topic) error {
	// TODO Implement check so that we're not leasing more than the allowed number of segments per buffer

	rangeSizeKey := fmt.Sprintf("/topic_range_sizes/%s", topic)
	res, err := s.Sess.Client().Get(ctx, rangeSizeKey)
	if err != nil {
		return err
	}
	if res.Count == 0 {
		return fmt.Errorf("topic %s does not exist", topic) // TODO Make sure everything for this topic is closed!
	}

	rangeSizeStr := string(res.Kvs[0].Value)
	rangeSize, err := types.ParseHashRangeSize(rangeSizeStr)
	if err != nil {
		panic(fmt.Errorf("failed to convert range size to int: %w", err))
	}

	res, err = s.Sess.Client().Get(ctx, fmt.Sprintf("/hash_range_leases/%s/", topic), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	missingRanges := rangeSize.HashRanges()
	for _, kv := range res.Kvs {
		hashRange, err := parseHashRangeKey(string(kv.Key))
		if err != nil {
			panic(fmt.Errorf("failed to parse hash range key: %w", err))
		}
		// TODO Check if there are any blocking hash ranges
		if rangeSize.ValidHashRange(hashRange) {
			for i := 0; i < len(missingRanges); i++ {
				if missingRanges[i] == hashRange {
					missingRanges = append(missingRanges[:i], missingRanges[i+1:]...)
					break
				}
			}
		} else {
			panic("oh no!") // temporary for debugging
		}
	}

	if len(missingRanges) == 0 {
		return nil
	}

	key := fmt.Sprintf("/hash_range_leases/%s/%s", topic, missingRanges[0].String())
	txnRes, err := s.Sess.Client().Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(rangeSizeKey), "=", rangeSizeStr),
			clientv3.Compare(clientv3.CreateRevision(key), "=", 0), // TODO Put correct buffer address based on pod address
		).
		Then(clientv3.OpPut(key, s.Addr, clientv3.WithLease(s.Sess.Lease()))).
		Commit()
	if err != nil {
		return err
	}
	if txnRes.Succeeded {
		segService := cmdhandler.NewSegmentService(topic, missingRanges[0], s.SegProps)
		handler := cmdhandler.NewHandler(
			s.Router.Ctx,
			segService,
		)
		go handler.Run()
	}
	return s.tryLease(ctx, topic)
}

const hashRangeSizesPrefix = "/topic_range_sizes/"

func (s *LeaseService) hashRangeSizeWatcher(ctx context.Context, readyCh chan struct{}) {
	w := clientv3.NewWatcher(s.Sess.Client())
	defer func() {
		if err := w.Close(); err != nil {
			log.WithError(err).Errorf("failed to close lease watcher")
		}
		if err := s.Close(); err != nil {
			log.WithError(err).Errorf("failed to close etcd session")
		}
	}()
	ch := w.Watch(ctx, hashRangeSizesPrefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	for {
		select {
		case <-ctx.Done():
			return
		case res := <-ch:
			if res.Canceled {
				log.Fatalf("watcher canceled: %v", res.Err())
				return
			}

			if res.Created {
				resp, err := s.Sess.Client().Get(ctx, hashRangeSizesPrefix, clientv3.WithPrefix())
				if err != nil {
					log.WithError(err).Fatalf("failed to get hash range sizes")
					return
				}
				for _, kv := range resp.Kvs {
					s.handleHashRangeSizeChange(ctx, kv)
				}
				readyCh <- struct{}{}
			}

			if res.Err() != nil {
				log.WithError(res.Err()).Fatalf("watcher error")
				return
			}

			for _, ev := range res.Events {
				s.handleHashRangeSizeChange(ctx, ev.Kv)
			}
		}
	}
}

func (s *LeaseService) handleHashRangeSizeChange(ctx context.Context, kv *mvccpb.KeyValue) {
	topic, err := parseTopicFromKey(string(kv.Key), 2)
	log.Infof("watcher recieved change in hash range size for topic: %s", topic)
	if err != nil {
		panic(fmt.Errorf("could not parse topic from key: %s", kv.Key))
	}

	rangeSize, err := types.ParseHashRangeSize(string(kv.Value))
	if err != nil {
		panic(fmt.Errorf("failed to parse hash range size: %w", err))
	}
	s.Router.PutHashRangeSize(topic, rangeSize)

	s.closeLeases(ctx, topic)

	for {
		if err := s.tryLease(ctx, topic); err != nil {
			log.WithError(err).Errorf("failed to try and lease, trying again")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

func (s *LeaseService) leaseWatcher(ctx context.Context) {
	w := clientv3.NewWatcher(s.Sess.Client())
	defer func() {
		if err := w.Close(); err != nil {
			log.WithError(err).Error("failed to close lease watcher")
		}
		if err := s.Close(); err != nil {
			log.WithError(err).Error("failed to close lease watcher")
		}
	}()
	ch := w.Watch(ctx, "/hash_range_leases/", clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case res := <-ch:
			if res.Canceled {
				log.Fatalf("lease watcher canceled: %v", res.Err())
				return
			}

			if res.Err() != nil {
				log.Fatalf("lease watcher error: %v", res.Err())
				return
			}

			for _, ev := range res.Events {
				topic, _ := parseTopicFromKey(string(ev.Kv.Key), 2)
				for {
					if err := s.tryLease(ctx, topic); err != nil {
						log.WithError(err).Errorf("failed when trying to lease hash range")
						time.Sleep(100 * time.Millisecond)
						continue
					}
					break
				}
			}
		}
	}
}

func parseHashRangeKey(key string) (types.HashRange, error) {
	split := strings.Split(key, "/")
	str := split[len(split)-1]
	return types.ParseHashRange(str)
}

func parseTopicFromKey(key string, pos int) (types.Topic, error) {
	split := strings.Split(key, "/")
	if len(split) < pos+1 {
		return "", fmt.Errorf("invalid pos for parsing topic")
	}
	return types.Topic(split[pos]), nil
}

package repo

import (
	"context"
	"fmt"

	"github.com/ntail-io/streams/buffer/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type SegmentRepo struct {
	sess *concurrency.Session
	addr string
}

func NewSegmentRepo(sess *concurrency.Session, bufferAddr string) *SegmentRepo {
	return &SegmentRepo{
		sess: sess,
		addr: bufferAddr,
	}
}

func (r *SegmentRepo) Save(ctx context.Context, segment *domain.Segment) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to save segment: %w", err)
		}
	}()

	if segment.Closed {
		_, err = r.sess.Client().Delete(ctx, bufferSegmentKey(segment))
		return
	}

	_, err = r.sess.Client().Txn(ctx).
		If(clientv3.Compare(clientv3.Version(segmentKey(segment)), "=", 0)).
		Then(
			clientv3.OpPut(segmentKey(segment), "1"),
			clientv3.OpPut(bufferSegmentKey(segment), r.addr, clientv3.WithLease(r.sess.Lease())),
		).
		Commit()
	return err
}

func segmentKey(seg *domain.Segment) string {
	// /segments/{topic}/{time}_{hashFrom}-{hashTo}
	return fmt.Sprintf("/segments/%s/%d_%d-%d", seg.Topic, seg.From, seg.HashRange.From, seg.HashRange.To)
}

func bufferSegmentKey(seg *domain.Segment) string {
	// /buffer_segments/{topic}/{time}_{hashFrom}-{hashTo}
	return fmt.Sprintf("/buffer_segments/%s/%d_%d-%d", seg.Topic, seg.From, seg.HashRange.From, seg.HashRange.To)
}

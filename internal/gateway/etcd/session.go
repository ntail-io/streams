package etcd

import (
	"context"

	"github.com/ntail-io/streams/internal/gateway/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func NewTailSession(client *clientv3.Client) (*TailSession, error) {
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		return nil, err
	}
	return &TailSession{
		Sess: sess,
	}, nil
}

type TailSession struct {
	Sess *concurrency.Session
}

func (s *TailSession) Commit(ctx context.Context, b *domain.Bookmark) error {
	return saveBookmark(ctx, s.Sess, b)
}

func (s *TailSession) RevokeBookmark(ctx context.Context, b *domain.Bookmark) error {
	res, err := s.Sess.Client().Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(bookmarkLeaseKey(b)), "=", s.Sess.Lease())).
		Then(clientv3.OpDelete(bookmarkLeaseKey(b))).
		Commit()

	if !res.Succeeded {
		return ErrMissingLease
	}
	return err
}

func (s *TailSession) LeaseBookmark(ctx context.Context, b *domain.Bookmark, new []*domain.Bookmark, deletions []*domain.Bookmark) error {
	cmps := make([]clientv3.Cmp, 0, len(deletions)+len(new)*2+1)

	// If we have deletions they must exist.
	for i := range deletions {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(bookmarkKey(deletions[i])), ">", 0))
	}

	// If we have new bookmarks they must not exist.
	for i := range new {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(bookmarkKey(new[i])), "=", 0))
	}

	// b must not exist if we have deletions.
	if len(deletions) > 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(bookmarkKey(b)), "=", 0))
	} else {
		// b must exist if we have no deletions.
		cmps = append(cmps, clientv3.Compare(clientv3.Version(bookmarkKey(b)), ">", 0))
	}

	ops := make([]clientv3.Op, 0, len(deletions)+len(new)+1)
	for i := range deletions {
		ops = append(ops, clientv3.OpDelete(bookmarkKey(deletions[i])))
	}
	for i := range new {
		ops = append(ops, clientv3.OpPut(bookmarkKey(new[i]), string(nilBookmark)))
	}
	ops = append(ops, clientv3.OpPut(bookmarkKey(b), string(nilBookmark)))
	ops = append(ops, clientv3.OpPut(bookmarkLeaseKey(b), "1", clientv3.WithLease(s.Sess.Lease())))

	res, err := s.Sess.Client().Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()

	if err != nil {
		return err
	}

	if !res.Succeeded {
		return ErrInconsistentState
	}
	return err
}

func (s *TailSession) Close() error {
	return s.Sess.Close()
}

package tail

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/bigquery"
	"github.com/ntail-io/streams/gateway/domain"
	"github.com/ntail-io/streams/gateway/etcd"
	v1 "github.com/ntail-io/streams/proto/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type TailSessionOptions struct {
	BQReaderService *bigquery.ReaderService
	Sess            *etcd.TailSession
	Topic           *domain.Topic
	Subscription    types.SubscriptionId
}

type TailSession struct {
	TailSessionOptions
	BQBuf    []byte
	BQSess   *bigquery.ReaderSession
	Data     []byte
	Addr     *domain.SegmentAddress
	Bookmark *domain.Bookmark
	EOF      bool
	Req      *v1.TailRequest
}

func (t *TailSession) Done() <-chan struct{} {
	return t.Sess.Sess.Done()
}

func (t *TailSession) Poll(ctx context.Context) (data []byte, err error) {
L:
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-t.Done():
			return nil, status.Error(codes.Internal, "session expired")
		default:
			// Commit before polling if this is not the first poll.
			if t.Bookmark != nil {
				if err = t.Sess.Commit(ctx, t.Bookmark); err != nil {
					err = status.Error(codes.Internal, "could not bookmark")
					return
				}
			}

			// Get next segment if we're finished with the existing one or if this is the first poll
			if t.Addr == nil || t.Bookmark.Finished { // TODO Check max segment lag with current segment lag, switch if too high
				err := t.newSegment(ctx) // TODO Maybe create a context with deadline based on parameter?
				if err != nil {
					return nil, err
				}
			}

			switch t.Addr.Type {
			case domain.BufferAddressType:
				if data, err = t.pollBuffer(ctx); err != nil {
					log.WithError(err).Error("failed to poll buffer")
					time.Sleep(50 * time.Millisecond)
					t.Addr = nil
				}
				if t.Bookmark.Finished {
					t.Addr = nil
				} else if data == nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				break L

			case domain.BigQueryAddressType:
				t.BQBuf = t.BQBuf[:0]

				if t.BQSess == nil {
					t.BQSess, err = t.BQReaderService.NewSession(ctx, t.Addr, t.Bookmark.MsgId)
					// If table could not be found we should jump to next segment.
					if status.Code(err) == codes.NotFound {
						log.Debug("table not found, skipping segment")
						t.Bookmark.Finished = true
						t.Addr = nil
						t.BQSess = nil
						continue
					}
					if err != nil {
						log.WithError(err).Error("failed to create bigquery session")
						return nil, status.Error(codes.Internal, "could not create bigquery session")
					}
				}
				var lastMsgId uuid.UUID
				t.BQBuf, lastMsgId, err = t.BQSess.Poll(ctx, t.BQBuf)
				if err == io.EOF { // do not set eof, eof will always be from a buffer.
					t.Bookmark.Finished = true
					t.Addr = nil
					t.BQSess = nil
					continue
				} else if err != nil {
					return nil, status.Error(codes.Unavailable, "could not poll bigquery")
				} else {
					data = t.BQBuf
					t.Bookmark.MsgId = lastMsgId
					break L
				}
			}
		}
	}

	return data, nil
}

func (t *TailSession) Commit(ctx context.Context, msgId uuid.UUID) error {
	{ // msgId validation
		if msgId.Time() < t.Addr.SegmentId.TimeFrom {
			return status.Error(codes.InvalidArgument, "invalid message id, cannot be before segment.")
		}
		if uuidutil.After(t.Bookmark.MsgId, msgId) {
			return status.Error(codes.InvalidArgument, "invalid message id, must be older or the same as the last polled msg.")
		}
	}

	bm := domain.Bookmark{
		MsgId:          msgId,
		Leased:         true,
		SegmentId:      t.Addr.SegmentId,
		SubscriptionId: t.Subscription,
		Finished:       t.Bookmark.Finished && msgId == t.Bookmark.MsgId,
	}

	if err := t.Sess.Commit(ctx, &bm); err != nil {
		return status.Error(codes.Internal, "could not bookmark")
	}
	return nil
}

// pollBuffer polls from the buffer, also mutates the cursor.
func (t *TailSession) pollBuffer(ctx context.Context) (data []byte, err error) {
	client, err := t.bufferClient(t.Addr)
	if err != nil {
		return
	}

	res, err := client.Poll(ctx, &v1.PollRequest{
		Addr: &v1.SegmentAddress{
			Topic:         string(t.Addr.SegmentId.Topic),
			HashRangeFrom: int32(t.Addr.SegmentId.HashRange.From),
			HashRangeTo:   int32(t.Addr.SegmentId.HashRange.To),
			FromTime:      int64(t.Addr.SegmentId.TimeFrom),
		},
		//FromTime: 0,
		Bookmark: &v1.Bookmark{
			MsgId: t.Bookmark.MsgId.String(),
		},
	})
	if err != nil {
		return
	}
	data = res.Data
	if data != nil {
		t.Bookmark.MsgId, err = uuid.Parse(res.GetLastMsgId())
		t.Bookmark.Finished = res.GetEof()
	}
	return
}

func (t *TailSession) bufferClient(addr *domain.SegmentAddress) (client v1.BufferServiceClient, err error) {
	conn, err := grpc.Dial(string(addr.Address), grpc.WithTransportCredentials(insecure.NewCredentials())) // TODO use TLS
	if err != nil {
		return
	}
	return v1.NewBufferServiceClient(conn), nil
}

func (t *TailSession) newSegment(ctx context.Context) (err error) {
	if t.Bookmark != nil {
		if err = t.Sess.RevokeBookmark(ctx, t.Bookmark); err != nil {
			return status.Error(codes.Internal, "could not revoke bookmark")
		}
	}

	var (
		deletions    []*domain.Bookmark
		newBookmarks []*domain.Bookmark
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if t.Bookmark, t.Addr, newBookmarks, deletions, err = t.Topic.AcquireBookmark(t.Subscription); err != nil {
				if err == domain.ErrSubscriptionNotFound {
					return status.Error(codes.NotFound, "subscription not found")
				}

				if ctx.Err() != nil {
					return ctx.Err()
				}

				time.Sleep(100 * time.Millisecond)
				log.Info("failed to acquire bookmark. Retrying...")
				continue
			}

			err := t.Sess.LeaseBookmark(ctx, t.Bookmark, newBookmarks, deletions)
			if err == etcd.ErrInconsistentState {
				continue
			}
			if err != nil {
				return status.Error(codes.Internal, "could not lease bookmark")
			}

			t.EOF = t.Bookmark.Finished
			return nil
		}
	}
}

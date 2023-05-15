package grpcio

import (
	"context"
	"io"
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/bigquery"
	"github.com/ntail-io/streams/gateway/domain"
	"github.com/ntail-io/streams/gateway/etcd"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type cursor struct {
	msgId    uuid.UUID
	finished bool
}

// Tail will attempt to stream and entire segment to the client.
// If the client disconnects, the segment will be released and the client will be disconnected.
// If the segment is on the buffer, and the buffer drops the segment the client will be disconnected.
// If the segment reaches EOF the client will be disconnected.
// It is expected that the client will open a new stream to continue reading.
func (s *GatewayService) Tail(server v1.GatewayService_TailServer) (err error) {
	defer func() {
		if err != nil {
			log.WithError(err).Error("failed to tail")
		}
	}()
	var (
		initialized  bool
		curs         cursor // TODO cursor is not needed anymore, we can use the bookmark directly
		topic        *domain.Topic
		bqBuf        []byte
		data         []byte
		bqSess       *bigquery.ReaderSession
		topicName    types.Topic
		subscription types.SubscriptionId
		addr         *domain.SegmentAddress
		bm           *domain.Bookmark
		req          = &v1.TailRequest{}
	)

	sess, err := etcd.NewTailSession(s.etcdClient)
	if err != nil {
		return status.Error(codes.Internal, "could not create session")
	}
	defer func() {
		log.Debug("closing tail etcd session")
		err := sess.Close()
		if err != nil {
			log.WithError(err).Error("failed to close etcd session")
		}
	}()

	newSegment := func(ctx context.Context) (err error) {
		if bm != nil {
			if err = sess.RevokeBookmark(ctx, bm); err != nil {
				return status.Error(codes.Internal, "could not revoke bookmark")
			}
		}

		curs = cursor{}
		var (
			deletions    []*domain.Bookmark
			newBookmarks []*domain.Bookmark
		)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if bm, addr, newBookmarks, deletions, err = topic.AcquireBookmark(subscription); err != nil {
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

				err := sess.LeaseBookmark(server.Context(), bm, newBookmarks, deletions)
				if err == etcd.ErrInconsistentState {
					continue
				}
				if err != nil {
					return status.Error(codes.Internal, "could not lease bookmark")
				}

				curs.finished = bm.Finished
				curs.msgId = bm.MsgId
				return nil
			}
		}
	}

	res := &v1.TailResponse{}

	for {
		select {
		case <-server.Context().Done():
			return nil
		case <-sess.Sess.Done():
			return status.Error(codes.Internal, "session expired")
		default:
			res.Reset()
			err := server.RecvMsg(req)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return status.Error(codes.InvalidArgument, "could not read request")
			}

			switch cmd := req.GetCommand().(type) {
			case *v1.TailRequest_Init:
				if initialized {
					return status.Error(codes.InvalidArgument, "already initialized")
				}

				init := cmd.Init
				initialized = true

				topicName = types.Topic(init.GetTopic())
				subscription = types.SubscriptionId(init.GetSubscription())
				// fromFilter := ...

				topic, err = s.topicService.Get(topicName)
				if err != nil {
					return status.Error(codes.NotFound, "topic not found")
				}
				res.Reset()
			case *v1.TailRequest_Poll:
				if !initialized {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

			L:
				for {
					select {
					case <-server.Context().Done():
						return nil
					case <-sess.Sess.Done():
						return status.Error(codes.Internal, "session expired")
					default:
						// Commit before polling if this is not the first poll.
						if bm != nil {
							bm.Finished = curs.finished
							bm.MsgId = curs.msgId
							if err = sess.Commit(server.Context(), bm); err != nil {
								return status.Error(codes.Internal, "could not bookmark")
							}
						}

						// Get next segment if we're finished with the existing one or if this is the first poll
						if addr == nil || curs.finished { // TODO Check max segment lag with current segment lag, switch if too high
							err := newSegment(server.Context()) // TODO Maybe create a context with deadline based on parameter?
							if err != nil {
								return err
							}
						}

						switch addr.Type {
						case domain.BufferAddressType:
							if data, err = s.pollBuffer(server.Context(), addr, &curs); err != nil {
								log.WithError(err).Error("failed to poll buffer")
								time.Sleep(50 * time.Millisecond)
								addr = nil
							}
							if curs.finished {
								addr = nil
								res.Eof = true
							} else if data == nil {
								time.Sleep(5 * time.Millisecond)
								continue
							}

							break L

						case domain.BigQueryAddressType:
							bqBuf = bqBuf[:0]

							if bqSess == nil {
								bqSess, err = s.bqReaderService.NewSession(server.Context(), addr, curs.msgId)
								// If table could not be found we should jump to next segment.
								if status.Code(err) == codes.NotFound {
									log.Debug("table not found, skipping segment")
									curs.finished = true
									addr = nil
									bqSess = nil
									continue
								}
								if err != nil {
									log.WithError(err).Error("failed to create bigquery session")
									return status.Error(codes.Internal, "could not create bigquery session")
								}
							}
							var lastMsgId uuid.UUID
							bqBuf, lastMsgId, err = bqSess.Poll(server.Context(), bqBuf)
							if err == io.EOF {
								curs.finished = true
								addr = nil
								bqSess = nil
								continue
							} else if err != nil {
								return status.Error(codes.Unavailable, "could not poll bigquery")
							} else {
								data = bqBuf
								curs.msgId = lastMsgId
								break L
							}
						}
					}
				}

				res.Data = data

			case *v1.TailRequest_Commit:
				if !initialized {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

				msgId, err := uuid.Parse(cmd.Commit.GetMsgId())
				if err != nil {
					return status.Error(codes.InvalidArgument, "invalid message id")
				}

				//if bytes.Equal(curs.msgId.NodeID(), msgId.NodeID()) {
				//	return status.Error(codes.InvalidArgument, "invalid message id, must be from the same session.")
				//}

				if msgId.Time() < addr.SegmentId.TimeFrom {
					return status.Error(codes.InvalidArgument, "invalid message id, cannot be before segment.")
				}

				if uuidutil.After(curs.msgId, msgId) {
					return status.Error(codes.InvalidArgument, "invalid message id, must be older or the same as the last polled msg.")
				}

				bm := domain.Bookmark{
					MsgId:          msgId,
					Leased:         true,
					SegmentId:      addr.SegmentId,
					SubscriptionId: subscription,
					Finished:       curs.finished && msgId == curs.msgId,
				}

				if err = sess.Commit(server.Context(), &bm); err != nil {
					return status.Error(codes.Internal, "could not bookmark")
				}
			}
			if err = server.Send(res); err != nil {
				return status.Error(codes.Internal, "could not send response")
			}
		}
	}
}

// pollBuffer polls from the buffer, also mutates the cursor.
func (s *GatewayService) pollBuffer(ctx context.Context, addr *domain.SegmentAddress, c *cursor) (data []byte, err error) {
	client, err := s.bufferClient(addr)
	if err != nil {
		return
	}

	res, err := client.Poll(ctx, &v1.PollRequest{
		Addr: &v1.SegmentAddress{
			Topic:         string(addr.SegmentId.Topic),
			HashRangeFrom: int32(addr.SegmentId.HashRange.From),
			HashRangeTo:   int32(addr.SegmentId.HashRange.To),
			FromTime:      int64(addr.SegmentId.TimeFrom),
		},
		//FromTime: 0,
		Bookmark: &v1.Bookmark{
			MsgId: c.msgId.String(),
		},
	})
	if err != nil {
		return
	}
	data = res.Data
	if data != nil {
		c.msgId, err = uuid.Parse(res.GetLastMsgId())
		c.finished = res.GetEof()
	}
	return
}

func (s *GatewayService) bufferClient(addr *domain.SegmentAddress) (client v1.BufferServiceClient, err error) {
	conn, err := grpc.Dial(string(addr.Address), grpc.WithTransportCredentials(insecure.NewCredentials())) // TODO use TLS
	if err != nil {
		return
	}
	return v1.NewBufferServiceClient(conn), nil

	sess, err := etcd.NewTailSession(s.etcdClient)
	if err != nil {
		return status.Error(codes.Internal, "could not create session")
	}
	defer func() {
		log.Debug("closing tail etcd session")
		err := sess.Close()
		if err != nil {
			log.WithError(err).Error("failed to close etcd session")
		}
	}()

	newSegment := func(ctx context.Context) (err error) {
		if bm != nil {
			if err = sess.RevokeBookmark(ctx, bm); err != nil {
				return status.Error(codes.Internal, "could not revoke bookmark")
			}
		}

		curs = cursor{}
		var (
			deletions    []*domain.Bookmark
			newBookmarks []*domain.Bookmark
		)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if bm, addr, newBookmarks, deletions, err = topic.AcquireBookmark(subscription); err != nil {
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

				err := sess.LeaseBookmark(server.Context(), bm, newBookmarks, deletions)
				if err == etcd.ErrInconsistentState {
					continue
				}
				if err != nil {
					return status.Error(codes.Internal, "could not lease bookmark")
				}

				curs.finished = bm.Finished
				curs.msgId = bm.MsgId
				return nil
			}
		}
	}

	res := &v1.TailResponse{}

	for {
		select {
		case <-server.Context().Done():
			return nil
		case <-sess.Sess.Done():
			return status.Error(codes.Internal, "session expired")
		default:
			res.Reset()
			err := server.RecvMsg(req)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return status.Error(codes.InvalidArgument, "could not read request")
			}

			switch cmd := req.GetCommand().(type) {
			case *v1.TailRequest_Init:
				if initialized {
					return status.Error(codes.InvalidArgument, "already initialized")
				}

				init := cmd.Init
				initialized = true

				topicName = types.Topic(init.GetTopic())
				subscription = types.SubscriptionId(init.GetSubscription())
				// fromFilter := ...

				topic, err = s.topicService.Get(topicName)
				if err != nil {
					return status.Error(codes.NotFound, "topic not found")
				}
				res.Reset()
			case *v1.TailRequest_Poll:
				if !initialized {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

			L:
				for {
					select {
					case <-server.Context().Done():
						return nil
					case <-sess.Sess.Done():
						return status.Error(codes.Internal, "session expired")
					default:
						// Commit before polling if this is not the first poll.
						if bm != nil {
							bm.Finished = curs.finished
							bm.MsgId = curs.msgId
							if err = sess.Commit(server.Context(), bm); err != nil {
								return status.Error(codes.Internal, "could not bookmark")
							}
						}

						// Get next segment if we're finished with the existing one or if this is the first poll
						if addr == nil || curs.finished { // TODO Check max segment lag with current segment lag, switch if too high
							err := newSegment(server.Context()) // TODO Maybe create a context with deadline based on parameter?
							if err != nil {
								return err
							}
						}

						switch addr.Type {
						case domain.BufferAddressType:
							if data, err = s.pollBuffer(server.Context(), addr, &curs); err != nil {
								log.WithError(err).Error("failed to poll buffer")
								time.Sleep(50 * time.Millisecond)
								addr = nil
							}
							if curs.finished {
								addr = nil
								res.Eof = true
							} else if data == nil {
								time.Sleep(5 * time.Millisecond)
								continue
							}

							break L

						case domain.BigQueryAddressType:
							bqBuf = bqBuf[:0]

							if bqSess == nil {
								bqSess, err = s.bqReaderService.NewSession(server.Context(), addr, curs.msgId)
								// If table could not be found we should jump to next segment.
								if status.Code(err) == codes.NotFound {
									log.Debug("table not found, skipping segment")
									curs.finished = true
									addr = nil
									bqSess = nil
									continue
								}
								if err != nil {
									log.WithError(err).Error("failed to create bigquery session")
									return status.Error(codes.Internal, "could not create bigquery session")
								}
							}
							var lastMsgId uuid.UUID
							bqBuf, lastMsgId, err = bqSess.Poll(server.Context(), bqBuf)
							if err == io.EOF {
								curs.finished = true
								addr = nil
								bqSess = nil
								continue
							} else if err != nil {
								return status.Error(codes.Unavailable, "could not poll bigquery")
							} else {
								data = bqBuf
								curs.msgId = lastMsgId
								break L
							}
						}
					}
				}

				res.Data = data

			case *v1.TailRequest_Commit:
				if !initialized {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

				msgId, err := uuid.Parse(cmd.Commit.GetMsgId())
				if err != nil {
					return status.Error(codes.InvalidArgument, "invalid message id")
				}

				//if bytes.Equal(curs.msgId.NodeID(), msgId.NodeID()) {
				//	return status.Error(codes.InvalidArgument, "invalid message id, must be from the same session.")
				//}

				if msgId.Time() < addr.SegmentId.TimeFrom {
					return status.Error(codes.InvalidArgument, "invalid message id, cannot be before segment.")
				}

				if uuidutil.After(curs.msgId, msgId) {
					return status.Error(codes.InvalidArgument, "invalid message id, must be older or the same as the last polled msg.")
				}

				bm := domain.Bookmark{
					MsgId:          msgId,
					Leased:         true,
					SegmentId:      addr.SegmentId,
					SubscriptionId: subscription,
					Finished:       curs.finished && msgId == curs.msgId,
				}

				if err = sess.Commit(server.Context(), &bm); err != nil {
					return status.Error(codes.Internal, "could not bookmark")
				}
			}
			if err = server.Send(res); err != nil {
				return status.Error(codes.Internal, "could not send response")
			}
		}
	}
}

// pollBuffer polls from the buffer, also mutates the cursor.
func (s *GatewayService) pollBuffer(ctx context.Context, addr *domain.SegmentAddress, c *cursor) (data []byte, err error) {
	client, err := s.bufferClient(addr)
	if err != nil {
		return
	}

	res, err := client.Poll(ctx, &v1.PollRequest{
		Addr: &v1.SegmentAddress{
			Topic:         string(addr.SegmentId.Topic),
			HashRangeFrom: int32(addr.SegmentId.HashRange.From),
			HashRangeTo:   int32(addr.SegmentId.HashRange.To),
			FromTime:      int64(addr.SegmentId.TimeFrom),
		},
		//FromTime: 0,
		Bookmark: &v1.Bookmark{
			MsgId: c.msgId.String(),
		},
	})
	if err != nil {
		return
	}
	data = res.Data
	if data != nil {
		c.msgId, err = uuid.Parse(res.GetLastMsgId())
		c.finished = res.GetEof()
	}
	return
}

func (s *GatewayService) bufferClient(addr *domain.SegmentAddress) (client v1.BufferServiceClient, err error) {
	conn, err := grpc.Dial(string(addr.Address), grpc.WithTransportCredentials(insecure.NewCredentials())) // TODO use TLS
	if err != nil {
		return
	}
	return v1.NewBufferServiceClient(conn), nil
}

package grpcio

import (
	"io"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/etcd"
	"github.com/ntail-io/streams/gateway/tail"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	etcdSess, err := etcd.NewTailSession(s.etcdClient)
	if err != nil {
		return status.Error(codes.Internal, "could not create session")
	}

	var (
		sess         *tail.TailSession
		topicName    types.Topic
		subscription types.SubscriptionId
		res          = &v1.TailResponse{}
		req          = &v1.TailRequest{}
	)

	for {
		select {
		case <-server.Context().Done():
			return nil
		case <-sess.Done():
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
				if sess != nil {
					return status.Error(codes.InvalidArgument, "already initialized")
				}

				init := cmd.Init

				topicName = types.Topic(init.GetTopic())
				subscription = types.SubscriptionId(init.GetSubscription())
				// fromFilter := ...

				topic, err := s.topicService.Get(topicName)
				if err != nil {
					return status.Error(codes.NotFound, "topic not found")
				}

				sess = &tail.TailSession{
					TailSessionOptions: tail.TailSessionOptions{
						BQReaderService: s.bqReaderService,
						Sess:            etcdSess,
						Topic:           topic,
						Subscription:    subscription,
					},
				}

				if err != nil {
					return status.Error(codes.Internal, "could not create session")
				}
			case *v1.TailRequest_Poll:
				if sess != nil {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

				res.Data, err = sess.Poll(server.Context())
				res.Eof = sess.EOF
				if err != nil {
					return status.Error(codes.Internal, "could not poll")
				}
			case *v1.TailRequest_Commit:
				if sess != nil {
					return status.Error(codes.FailedPrecondition, "not initialized")
				}

				msgId, err := uuid.Parse(cmd.Commit.GetMsgId())
				if err != nil {
					return status.Error(codes.InvalidArgument, "invalid message id")
				}

				if err = sess.Commit(server.Context(), msgId); err != nil {
					return status.Error(codes.Internal, "could not bookmark")
				}
			}
			if err = server.Send(res); err != nil {
				return status.Error(codes.Internal, "could not send response")
			}
		}
	}
}

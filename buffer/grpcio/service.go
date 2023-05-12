package grpcio

import (
	"context"
	"errors"
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/buffer"
	"github.com/ntail-io/streams/buffer/buftypes"
	"github.com/ntail-io/streams/buffer/commands"
	"github.com/ntail-io/streams/core/types"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BufferService struct {
	v1.UnimplementedBufferServiceServer
	router *buffer.Router
}

func NewBufferService(router *buffer.Router) *BufferService {
	return &BufferService{
		router: router,
	}
}

func (s *BufferService) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	topic := types.Topic(req.GetTopic())
	if err := topic.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	key := types.Key(req.GetKey())

	cmd := commands.Get{
		Topic: types.Topic(req.GetTopic()),
		Key:   types.Key(req.GetKey()),
		Hash:  key.Hash(),
		From:  uuid.Time(req.GetFromTime()),
		ResCh: make(chan *commands.Get, 1),
	}
	err := s.router.Get(ctx, &cmd)
	if err == buffer.ErrNotFoundInconsistentData {
		return nil, status.Error(codes.NotFound, "not found (inconsistent data)")
	}
	if err == buffer.ErrNotFound {
		return nil, status.Error(codes.NotFound, "not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &v1.GetResponse{
		MsgId: cmd.MsgId.String(),
		Data:  cmd.Data,
	}, nil
}

func (s *BufferService) Append(server v1.BufferService_AppendServer) error {
	cmds := make([]commands.Append, 50_000)
	resCh := make(chan *commands.Append, 50_000)
	free := make(chan *commands.Append, 50_000)
	errCh := make(chan error)

	// Each connection have a pool of append commands to can be reused
	for i := range cmds {
		cmds[i] = commands.Append{
			ResCh: resCh,
		}
		free <- &cmds[i]
	}

	// Request routine
	go func() {
		for {
			select {
			case <-server.Context().Done():
				return
			case cmd := <-free:
				// default:
				// cmd := &commands.Append{
				// 	Req: &v1.AppendRequest{},
				// 	Res: &v1.AppendResponse{},
				// 	Ch:  resCh,
				// }
				// cmd.Res.ReqId = cmd.Req.ReqId
				req, err := server.Recv()
				// if err == io.EOF {
				// 	errCh <- err
				// 	return
				// 	//time.Sleep(10 * time.Millisecond)
				// 	//continue
				// }
				if err != nil {
					errCh <- err
					log.WithError(err).Error("could not read request")
					return
				}

				key := types.Key(req.GetKey())
				*cmd = commands.Append{
					Id:    req.ReqId,
					Topic: types.Topic(req.GetTopic()),
					Key:   key,
					Hash:  key.Hash(),
					Data:  req.GetData(),
					ResCh: resCh,
				}

				if err := cmd.Topic.Validate(); err != nil {
					cmd.Err = status.Error(codes.InvalidArgument, "invalid topic name")
					cmd.ResCh <- cmd
					return
				}

				if err := s.router.Append(cmd); err != nil {
					cmd.Err = err
					cmd.ResCh <- cmd
				}
			}
		}
	}()

	for {
		select {
		case <-server.Context().Done():
			return nil
		case cmd := <-resCh:

			res := &v1.AppendResponse{
				ReqId: cmd.Id,
			}

			if cmd.Err != nil {
				log.WithError(cmd.Err).Warnf("could not append, sending error to client")
				// TODO Handle error better here
				res.Result = &v1.AppendResponse_ErrorCode{
					ErrorCode: int32(codes.Internal),
				}
			} else {
				res.Result = &v1.AppendResponse_MsgId{
					MsgId: cmd.MsgId.String(),
				}
			}

			if err := server.Send(res); err != nil {
				log.WithError(err).Error("could not send response")
				return err
			}
			free <- cmd
		case err := <-errCh:
			log.WithError(err).Error("could not read request")
			return err
		}
	}
}

func (s *BufferService) Poll(ctx context.Context, req *v1.PollRequest) (_ *v1.PollResponse, err error) {
	defer func() {
		if err != nil {
			log.WithError(err).Error("could not poll")
		}
	}()

	if req.Addr == nil {
		return nil, status.Error(codes.InvalidArgument, "segment address is required")
	}

	topic := types.Topic(req.Addr.GetTopic())
	if err := topic.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}

	if req.Addr.HashRangeFrom > 65535 {
		return nil, status.Error(codes.InvalidArgument, "hash range from is invalid")
	}
	if req.Addr.HashRangeTo > 65535 {
		return nil, status.Error(codes.InvalidArgument, "hash range to is invalid")
	}

	var msgId uuid.UUID
	if req.GetBookmark() != nil {
		if msgId, err = uuid.Parse(req.GetBookmark().GetMsgId()); err != nil {
			return nil, status.Error(codes.InvalidArgument, "msgId is invalid")
		}
	}

	cmd := commands.Poll{
		Addr: buftypes.SegmentAddress{
			Topic: topic,
			HashRange: types.HashRange{
				From: types.Hash(req.Addr.HashRangeFrom),
				To:   types.Hash(req.Addr.HashRangeTo),
			},
			From: uuid.Time(req.Addr.FromTime),
		},
		FromTime:  time.UnixMicro(req.FromTime),
		FromMsgId: msgId,
	}

	if err := s.router.Poll(ctx, &cmd); err != nil {
		if !errors.Is(err, buffer.ErrInvalidSegmentAddress) {
			panic("unexpected error")
		}
		return nil, status.Error(codes.NotFound, "segment address not found")
	}
	return &v1.PollResponse{
		LastMsgId: cmd.Result.LastMsgId.String(),
		Data:      cmd.Result.Data,
		FromTime:  cmd.Result.From.UnixMicro(),
		ToTime:    cmd.Result.To.UnixMicro(),
		Eof:       cmd.Result.EOF,
	}, nil
}

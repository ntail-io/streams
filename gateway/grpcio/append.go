package grpcio

import (
	"io"
	"sync"
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/domain"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (s *GatewayService) Append(server v1.GatewayService_AppendServer) error {
	// Each connection will have a pool of requests and responses
	//cmds := make([]commands.Append, 1_000)
	//for i := range cmds {
	//	cmds[i].Req = &v1.AppendRequest{}
	//	cmds[i].Res = &v1.AppendResponse{}
	//}
	//free := make(chan *commands.Append, 1_000)
	//for i := range cmds {
	//	free <- &cmds[i]
	//}

	resCh := make(chan *v1.AppendResponse, 100)
	closeCh := make(chan error)
	streams := make(map[domain.BufferAddress]v1.GatewayService_AppendClient)
	var muStreams sync.RWMutex

	errResp := func(reqId int64, code codes.Code) *v1.AppendResponse {
		return &v1.AppendResponse{
			ReqId:  reqId,
			Result: &v1.AppendResponse_ErrorCode{ErrorCode: int32(code)},
		}
	}

	bufferReceiver := func(stream v1.GatewayService_AppendClient, addr domain.BufferAddress) {
		defer func() {
			muStreams.Lock()
			delete(streams, addr)
			err := stream.CloseSend()
			muStreams.Unlock()
			if err != nil {
				log.WithError(err).Error("could not close stream")
			}
		}()

		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.WithError(err).Error("could not read response from buffer. Closing client connection.")
				closeCh <- err
				return
			}
			resCh <- res
		}
	}

	// Request routine
	go func() {
		for {
			select {
			case <-server.Context().Done():
				return
			default:
				req, err := server.Recv()
				if err == io.EOF {
					closeCh <- nil
					return
				}
				if err != nil {
					closeCh <- err
					log.WithError(err).Error("could not read request")
					return
				}
				topic, err := s.topicService.Get(types.Topic(req.GetTopic()))
				if err != nil {
					resCh <- errResp(req.GetReqId(), codes.InvalidArgument)
					continue
				}
				key, err := types.NewKey(req.GetKey())
				if err != nil {
					log.WithError(err).Error("invalid key from client")
					resCh <- errResp(req.GetReqId(), codes.InvalidArgument)
					continue
				}

				var addr domain.BufferAddress
				for i := 0; i < 3; i++ {
					addr, err = topic.AddressOfHash(key.Hash())
					// retry getting address
					if err == nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil {
					if err != nil {
						log.WithError(err).Warn("could not get address")
						resCh <- errResp(req.GetReqId(), codes.Unavailable)
						return
					}
				}

				muStreams.RLock()
				stream, ok := streams[addr]
				if !ok {
					// Switch to write lock
					muStreams.RUnlock()
					muStreams.Lock()
					conn, err := grpc.Dial(string(addr), grpc.WithTransportCredentials(insecure.NewCredentials())) // TODO Use TLS
					if err != nil {
						log.WithError(err).Error("could not dial buffer")
						resCh <- errResp(req.GetReqId(), codes.Unavailable)
						return
					}
					// This error is not of interest, the only error that can be returned is saying that it has no connections.
					defer func() { _ = conn.Close() }()

					client := v1.NewBufferServiceClient(conn)
					if stream, err = client.Append(server.Context(), grpc.WaitForReady(true)); err != nil {
						log.WithError(err).Error("could not create stream")
						resCh <- errResp(req.GetReqId(), codes.Unavailable)
						return
					}

					streams[addr] = stream
					go bufferReceiver(stream, addr)
					muStreams.Unlock()
					muStreams.RLock()
				}

				if err = stream.SendMsg(req); err != nil {
					log.WithError(err).Error("could not send request")
					resCh <- errResp(req.GetReqId(), codes.Unavailable)
					return
				}
				muStreams.RUnlock()
			}
		}
	}()

	for {
		select {
		case <-server.Context().Done():
			return nil
		case res := <-resCh:
			if err := server.SendMsg(res); err != nil {
				log.WithError(err).Error("could not send response")
				return status.Error(codes.Internal, "internal error")
			}
		case err := <-closeCh:
			if err == nil {
				return nil
			}
			log.WithError(err).Error("closing stream due to error")
			if _, ok := status.FromError(err); !ok {
				return status.Error(codes.Internal, "internal error")
			}
			return err
		}
	}
}

//func (s *GatewayService) Tail(request *v1.TailRequest, server v1.GatewayService_TailServer) error {
//	//TODO implement me
//	panic("implement me")
//}
//
//
//func (s *BufferService) Poll(ctx context.Context, req *v1.PollRequest) (*v1.PollResponse, error) {
//	if req.Addr == nil {
//		return nil, status.Error(codes.InvalidArgument, "segment address is required")
//	}
//
//	poll := &buffer.Poll{
//		Req: req,
//		Res: &v1.PollResponse{},
//		Ch:  make(chan *buffer.Poll),
//	}
//
//	pollRes, err := s.gw.Poll(ctx, poll.Address(), time.UnixMicro(req.FromTime))
//	if err == domain.EOF {
//		return nil, status.Error(codes.OutOfRange, "no more data")
//	}
//	if err != nil {
//		if err == buffer.ErrInvalidSegmentAddress {
//			return nil, status.Error(codes.NotFound, "segment address not found")
//		}
//		return nil, status.Error(codes.Internal, "internal error")
//	}
//	return &v1.PollResponse{
//		Data:     pollRes.Data,
//		FromTime: pollRes.From.UnixMicro(),
//		ToTime:   pollRes.To.UnixMicro(),
//	}, nil
//}

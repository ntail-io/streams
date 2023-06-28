package grpcio

import (
	"errors"
	"io"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/gateway/bufclient"
	"github.com/ntail-io/streams/gateway/domain"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (s *GatewayService) Append(server v1.GatewayService_AppendServer) error {
	pool := bufclient.NewAppendStreamPool(server.Context(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	resCh := pool.ResCh

	errResp := func(reqId int64, code codes.Code) *v1.AppendResponse {
		return &v1.AppendResponse{
			ReqId:  reqId,
			Result: &v1.AppendResponse_ErrorCode{ErrorCode: int32(code)},
		}
	}

	log.Info("append stream opened") // TODO remove

	// Request routine
	go func() {
		for {
			select {
			case <-server.Context().Done():
				return
			default:
				req, err := server.Recv()
				if err == io.EOF {
					return // TODO Handle EOF better, wait for all responses to be sent and close the stream
				}
				if err != nil {
					log.WithError(err).Error("could not read request")
					return
				}

				err = bufclient.Append(pool, s.topicService, req)
				if errors.Is(err, domain.ErrBufferAddressNotFound) {
					resCh <- errResp(req.GetReqId(), codes.Unavailable)
					continue
				}
				if err != nil {
					resCh <- errResp(req.GetReqId(), codes.InvalidArgument)
					continue
				}
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
		}
	}
}

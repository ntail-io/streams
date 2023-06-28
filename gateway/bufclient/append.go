package bufclient

import (
	"context"
	"io"
	"sync"

	"github.com/ntail-io/streams/gateway/domain"
	v1 "github.com/ntail-io/streams/proto/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

type AppendChan = chan *v1.AppendRequest

type AppendStreamPool struct {
	Ctx         context.Context // Every stream will have the same context
	ResCh       chan *v1.AppendResponse
	Streams     map[domain.BufferAddress]AppendChan
	DialOptions []grpc.DialOption
	mu          sync.RWMutex
}

func NewAppendStreamPool(ctx context.Context, opts ...grpc.DialOption) *AppendStreamPool {
	return &AppendStreamPool{
		Ctx:         ctx,
		ResCh:       make(chan *v1.AppendResponse, 10_000),
		Streams:     make(map[domain.BufferAddress]AppendChan),
		DialOptions: opts,
	}
}

func (p *AppendStreamPool) Append(addr domain.BufferAddress, req *v1.AppendRequest) {
	reqCh, err := p.open(addr)
	if err != nil {
		log.WithError(err).Warnf("could not open buffer stream: %s", addr)
		p.ResCh <- &v1.AppendResponse{
			ReqId:  req.ReqId,
			Result: &v1.AppendResponse_ErrorCode{ErrorCode: int32(codes.Unavailable)},
		}
		return
	}
	reqCh <- req
}

func (p *AppendStreamPool) open(addr domain.BufferAddress) (AppendChan, error) {
	{ // Stream already exists
		p.mu.RLock()
		stream, ok := p.Streams[addr]
		p.mu.RUnlock()
		if ok {
			return stream, nil
		}
	}

	p.mu.Lock() // Lock the pool for writing
	defer p.mu.Unlock()
	// Check again if the stream exists (it might have been created by another goroutine) whilst the lock was released.
	if stream, ok := p.Streams[addr]; ok {
		return stream, nil
	}

	// Create a new stream
	ctx, cnl := context.WithCancel(p.Ctx)
	reqCh, err := newAppendStream(ctx, cnl, addr, p.ResCh)
	if err != nil {
		return nil, err
	}

	p.Streams[addr] = reqCh

	// close the stream when the context is done
	// This will be done when the stream is closed or on shutdown
	go func() {
		<-ctx.Done()
		p.mu.Lock()
		defer p.mu.Unlock()

		delete(p.Streams, addr)
	}()

	return reqCh, nil
}

type AppendStream struct {
	Stream v1.BufferService_AppendClient
	ReqCh  AppendChan
}

func newAppendStream(ctx context.Context, cancel context.CancelFunc,
	addr domain.BufferAddress, resCh chan *v1.AppendResponse) (reqCh AppendChan, err error) {

	conn, err := grpc.Dial(string(addr), grpc.WithTransportCredentials(insecure.NewCredentials())) // TODO Use TLS/ALTS
	if err != nil {
		return
	}
	client := v1.NewBufferServiceClient(conn)
	stream, err := client.Append(ctx)
	if err != nil {
		return
	}

	reqCh = make(AppendChan, 10_000)

	go func() {
		for {
			select {
			case <-ctx.Done():

				// Drain reqCh
				for len(reqCh) > 0 {
					req := <-reqCh
					resCh <- &v1.AppendResponse{
						ReqId:  req.ReqId,
						Result: &v1.AppendResponse_ErrorCode{ErrorCode: int32(codes.Canceled)},
					}
				}

				return
			case req := <-reqCh:
				// TODO Maybe support batching requests?
				// Can be done by checking len(reqCh) because this is the only goroutine that consumes from reqCh

				if err := stream.Send(req); err != nil {
					log.WithError(err).Error("could not send request")
					cancel()
					return
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				res, err := stream.Recv()
				if err != nil {
					cancel()
					if err == io.EOF {
						return
					}
					return
				}
				resCh <- res
			}
		}
	}()
	return
}

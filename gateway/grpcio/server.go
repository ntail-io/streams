package grpcio

import (
	"context"
	"fmt"
	"net"

	v1 "github.com/ntail-io/streams/proto/v1"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/alts"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/reflection"
)

func NewServer(ctx context.Context, addr string, service *GatewayService, useAlts bool) error {
	opts := make([]grpc.ServerOption, 0)

	// ALTS
	if useAlts {
		altsTC := alts.NewServerCreds(alts.DefaultServerOptions())
		opts = append(opts, grpc.Creds(altsTC))
	}

	//opts = append(opts, grpc.ForceServerCodec(grpccodec.Codec{}))

	// listen on a tcp socket
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	fmt.Printf("Listening for grpc on %s\n", addr)

	// we're going to run the different protocol servers in parallel, so
	// make an errgroup
	var group errgroup.Group

	// grpc handling
	group.Go(func() error {
		s := grpc.NewServer(opts...)
		reflection.Register(s)

		// register the proto-specific methods on the server

		v1.RegisterGatewayServiceServer(s, service)

		// run the server
		return s.Serve(lis)
	})

	go func() {
		err := group.Wait()
		if err != nil {
			log.WithContext(ctx).WithError(err).Fatalln("grpc server failed")
		}
	}()

	return nil
}

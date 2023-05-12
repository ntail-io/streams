package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	bufboot "github.com/ntail-io/streams/buffer/boot"
	gwboot "github.com/ntail-io/streams/gateway/boot"

	log "github.com/sirupsen/logrus"
)

func main() {
	args := os.Args[1:]

	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
	})
	log.SetReportCaller(true)

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	if len(args) == 0 {
		panic("missing argument: [buffer | gateway | both]")
	}

	if args[0] == "buffer" {
		bufboot.Start(ctx)
	} else if args[0] == "gateway" {
		gwboot.Start(ctx)
	} else if args[0] == "both" {
		go bufboot.Start(ctx)
		gwboot.Start(ctx)
	} else {
		panic("invalid argument: [buffer | gateway | both]")
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
}

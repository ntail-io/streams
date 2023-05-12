package cmdhandler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ntail-io/streams/buffer/bq"
	"github.com/ntail-io/streams/buffer/commands"
	"github.com/ntail-io/streams/buffer/domain"

	log "github.com/sirupsen/logrus"
)

var (
	ErrSegmentClosed = errors.New("segment closed")
	ErrCancelled     = errors.New("command cancelled")
)

type Handler struct {
	Ctx              context.Context
	CmdCh            chan commands.Command
	Service          *SegmentService
	Conn             *bq.BigQueryConn
	Next             *SegmentService
	mu               sync.Mutex
	AppendCh         chan struct{}
	AppendFinishedCh chan error
}

func NewHandler(ctx context.Context, service *SegmentService) *Handler {
	return &Handler{
		Ctx:              ctx,
		Service:          service,
		CmdCh:            service.CmdCh, // TODO Let's not let service know about the cmd channel. Also let handler talk to the router.
		AppendCh:         make(chan struct{}, 1),
		AppendFinishedCh: make(chan error, 1),
	}
}

func (h *Handler) Handle(cmd commands.Command) {
	defer measureHandle(time.Now(), cmd)

	switch cmd := cmd.(type) {
	case *commands.Append:
		h.handleAppend(cmd)
	case *commands.Get:
		cmd.MsgId, cmd.Data, cmd.Ok, cmd.Err = h.Service.Get(cmd.Key)
		cmd.ResCh <- cmd
	case *commands.Poll:
		cmd.Result, cmd.Err = h.Service.Poll(cmd.Addr, cmd.FromTime, cmd.FromMsgId)
		// TODO Check if buffer would be full if append msg is included
	case *commands.Finish:
		h.Service.Finish()
		cmd.ResCh <- cmd
	default:
		panic(fmt.Sprintf("unknown command type %T", cmd))
	}
}

func (h *Handler) Run() {
	ctx, cnl := context.WithCancel(h.Ctx)
	defer cnl()
	defer log.Debugf("handler for segment [%v] closed", h.Service)

	for {
		select {
		case <-ctx.Done():
			return
		case cmd, more := <-h.CmdCh:
			if !more {
				h.Close()
				reportClosed(cmd)
				return
			}
			h.Handle(cmd)
		}
	}
}

func (h *Handler) handleAppend(cmd *commands.Append) {
	if h.Service.Segment.Closed {
		cmd.Err = ErrSegmentClosed
		cmd.ResCh <- cmd
		return
	}

	if h.Service.Segment.Finished {
		log.Debug("trying to append message to finished segment, will try to forward messages to next segment")
		if h.Next != nil {
			h.Next.CmdCh <- cmd
		} else {
			cmd.Err = ErrSegmentFinished
			cmd.ResCh <- cmd
		}
		return
	}

	// Call append of SegmentService, if the segment is full, create a new one and call the append command on that one.
	{
		err := h.Service.Append(cmd)
		if err == domain.ErrSegmentFull { // Segment is full, create a new one. We make a reference to the new one in case we need to forward append cmds to it.
			log.Infof("segment [%v] is full, creating new segment and appending pending messages to the new segment", h.Service)
			h.Next = NewSegmentService(h.Service.Segment.Topic, h.Service.Segment.HashRange, h.Service.SegmentServiceProps)
			if err = h.Next.Append(cmd); err != nil {
				log.WithError(err).Error("failed to append messages to new (next) segment")
			}
			// Use the original ctx for the RunHandler, not the one with a cancel func. Otherwise the handler will close prematurely.
			handler := NewHandler(h.Ctx, h.Next)
			go handler.Run()
		}
		if err != nil {
			cmd.Err = err
			cmd.ResCh <- cmd
		}
	}
}

func (h *Handler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.Infof("handler closing segment because api channel is closed: %v", h.Service)
	h.Service.close()

	// Drain the cmd channel
	if len(h.CmdCh) > 0 {
		log.Debugf("draining %d pending commands from segment %v", len(h.CmdCh), h.Service)
		for len(h.CmdCh) > 0 {
			reportClosed(<-h.CmdCh)
		}

		_, ok := <-h.CmdCh
		if ok {
			panic("closing segment when api channels are not closed")
		}
	}
	log.Debugf("handler finished closing segment because api channel is closed: %v", h.Service)
}

func reportClosed(cmd commands.Command) {
	if cmd == nil {
		return
	}
	switch cmd := cmd.(type) {
	case *commands.Append:
		cmd.Err = ErrSegmentClosed
		cmd.ResCh <- cmd
	case *commands.Poll:
		cmd.Err = ErrSegmentClosed
		cmd.ResCh <- cmd
	case *commands.Finish:
		cmd.ResCh <- cmd
	case *commands.Get:
		cmd.Err = ErrSegmentClosed
		cmd.ResCh <- cmd
	default:
		panic(fmt.Sprintf("unknown command type %T", cmd))
	}
}

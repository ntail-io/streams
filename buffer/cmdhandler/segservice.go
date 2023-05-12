package cmdhandler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ntail-io/streams/buffer"
	"github.com/ntail-io/streams/buffer/bq"
	"github.com/ntail-io/streams/buffer/buftypes"
	"github.com/ntail-io/streams/buffer/commands"
	"github.com/ntail-io/streams/buffer/domain"
	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/buffer/etcd/repo"
	"github.com/ntail-io/streams/core/types"

	"github.com/google/uuid"

	log "github.com/sirupsen/logrus"
)

var (
	ErrInvalidSegmentAddress = errors.New("invalid segment address")
	ErrBufferSizeExceeded    = errors.New("buffer size exceeded")
	ErrSegmentFinished       = errors.New("segment finished")
)

const (
	MaxBufSize          = 1<<20*10 - 1024 // Just under 10MB
	MaxBufferedCommands = 40_000
)

type SegmentServiceProps struct {
	Ctx          context.Context
	Router       *buffer.Router
	Repo         *repo.SegmentRepo
	Conn         *bq.BigQueryConn
	FlushEnabled bool
}
type SegmentService struct {
	SegmentServiceProps
	Segment            domain.Segment
	CmdCh              chan commands.Command
	Initialized        bool
	UncommittedAppends chan *commands.Append
	Writer             *bq.BatchWriter
}

func NewSegmentService(topic types.Topic, hashRange types.HashRange, props SegmentServiceProps) *SegmentService {
	from := uuidutil.Now()

	s := &SegmentService{
		SegmentServiceProps: props,
		Segment:             domain.NewSegment(topic, hashRange, from, props.Router.Allocator),
		CmdCh:               make(chan commands.Command, MaxBufferedCommands),
		UncommittedAppends:  make(chan *commands.Append, MaxBufferedCommands),
	}

	addr := buftypes.SegmentAddress{
		Topic:     topic,
		HashRange: hashRange,
		From:      from,
	}

	writer := bq.NewWriter(s.Ctx, s.Conn, s.Segment.Topic, s.Segment.HashRange, s.Segment.From)
	serializer := bq.NewRowSerializer(5)
	s.Writer = bq.NewBatchWriter(writer, serializer, props.FlushEnabled)

	s.Router.PutSegment(addr, s.CmdCh) // TODO Probably move GW handling to handler

	log.Infof("new segment service: %v", s)

	return s
}

func (s *SegmentService) String() string {
	return fmt.Sprintf("topic: %s, hash range: %s, from: %d",
		s.Segment.Topic, s.Segment.HashRange.String(), s.Segment.From)
}

func (s *SegmentService) init() error {
	if err := s.Writer.Writer.Init(); err != nil {
		log.WithError(err).Error("failed to initialize segment writer")
		return err
	}

	if err := s.Repo.Save(s.Router.Ctx, &s.Segment); err != nil {
		log.WithError(err).Error("failed to save segment")
		return err
	}

	// go s.writer.Run()

	s.Initialized = true
	return nil
}

func (s *SegmentService) Append(cmd *commands.Append) error {
	if s.Segment.Finished {
		panic(ErrSegmentFinished)
	}

	if !s.Initialized {
		log.Infof("initializing segment %v", s)
		s.Initialized = true
		go func() {
			log.Infof("starting bq batch writer for segment: %v", s)
			for {
				select {
				case <-s.Router.Ctx.Done():
					return
				default:
					rows := s.Writer.Flush()
					if rows == 0 {
						time.Sleep(time.Millisecond)
					}
					s.Segment.Commit(rows)

					for i := 0; i < rows; i++ {
						cmd := <-s.UncommittedAppends
						cmd.ResCh <- cmd
					}
				}
			}
		}() // TODO Should be in init function
		// if err := s.init(); err != nil {
		// 	return err
		// }
	}

	if !s.Segment.HashRange.Contains(cmd.Hash) {
		panic("hash not in range")
	}

	id, err := s.Segment.Append(cmd.Key, cmd.Data)
	if err != nil {
		log.WithError(err).Error("failed to append to segment, marking segment as finished")
		s.Finish()
		return err
	}

	cmd.MsgId = id

	s.UncommittedAppends <- cmd
	s.Writer.Write(id, cmd)

	return nil
}

func (s *SegmentService) Finish() {
	if s.Segment.Finished {
		return
	}

	s.Segment.WaitCommitted()

	s.Segment.Finished = true
	s.Router.SegmentFinished(s.Segment.Topic, s.Segment.HashRange, s.Segment.From)
	log.Infof("segment finished topic: %v", s)
}

// TODO We might want the poll result to be a channel and get notified if append is called.
// TODO If we are at the end of the segment, segment is not finished, and we have no messages, we should wait for a message to be appended.
// TODO We need to be notified if: client ctx is Done, segment is finished, segment is closed, and message is appended.
func (s *SegmentService) Poll(addr buftypes.SegmentAddress, from time.Time, fromMsgId uuid.UUID) (domain.PollResult, error) {
	if !s.validAddress(addr) {
		return domain.PollResult{}, ErrInvalidSegmentAddress
	}

	return s.Segment.Poll(from, fromMsgId)
}

func (s *SegmentService) Get(key types.Key) (msgId uuid.UUID, data []byte, ok bool, err error) {
	msgId, data, ok = s.Segment.Get(key)
	return
}

func (s *SegmentService) validAddress(addr buftypes.SegmentAddress) bool {
	return s.Segment.Topic == addr.Topic && s.Segment.HashRange.Equal(addr.HashRange) && addr.From == s.Segment.From
}

func (s *SegmentService) close() {
	s.Segment.Close() // This frees the memory used by the segment.a
}

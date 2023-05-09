package commands

import (
	"time"

	"github.com/ntail-io/streams/internal/buffer/buftypes"
	"github.com/ntail-io/streams/internal/buffer/domain"
	"github.com/ntail-io/streams/internal/core/types"

	"github.com/google/uuid"
)

type CommandType int

const (
	AppendCommand CommandType = iota
	PollCommand
	GetCommand
	FinishCommand

	CommandCount
)

var commandTypeStr = [CommandCount]string{
	"append",
	"poll",
	"get",
	"finish",
}

func (t CommandType) String() string {
	return commandTypeStr[t]
}

type Command interface {
	CommandType() CommandType
}

type Append struct {
	Id    int64
	Topic types.Topic
	Key   types.Key
	Hash  types.Hash
	Data  []byte
	Err   error
	MsgId uuid.UUID
	ResCh chan *Append
}

func (a *Append) CommandType() CommandType {
	return AppendCommand
}

type Poll struct {
	Addr      buftypes.SegmentAddress
	FromTime  time.Time
	FromMsgId uuid.UUID
	Result    domain.PollResult
	Err       error
	ResCh     chan *Poll
}

func (p *Poll) CommandType() CommandType {
	return PollCommand
}

type Get struct {
	Topic types.Topic
	Key   types.Key
	Hash  types.Hash
	From  uuid.Time
	Ok    bool // True if msg was found
	Data  []byte
	MsgId uuid.UUID
	Err   error
	ResCh chan *Get
}

func (g *Get) CommandType() CommandType {
	return GetCommand
}

// Finish a segment without creating a next one
type Finish struct {
	ResCh chan *Finish
}

func (f *Finish) CommandType() CommandType {
	return FinishCommand
}

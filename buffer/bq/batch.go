package bq

import (
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"
	log "github.com/sirupsen/logrus"

	"github.com/ntail-io/streams/buffer/commands"
	"github.com/ntail-io/streams/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/util/backoff"

	"github.com/google/uuid"
)

type BatchWriter struct {
	Writer       *Writer
	Serializer   *RowSerializer
	FlushEnabled bool
}

func NewBatchWriter(w *Writer, s *RowSerializer, flushEnabled bool) *BatchWriter {
	return &BatchWriter{
		Writer:       w,
		Serializer:   s,
		FlushEnabled: flushEnabled,
	}
}

func (w *BatchWriter) Write(id uuid.UUID, cmd *commands.Append) {
	microTime := uuidutil.ToTime(id.Time()).UnixMicro()
	msgIdTime := int64(id.Time()) // 100s of nanos, not epoch
	clockSeq := int32(id.ClockSequence())
	hash := uint32(cmd.Hash)

	row := v1.Row{
		MsgIdTime:     &msgIdTime,
		MsgIdClockSeq: &clockSeq,
		MsgIdNodeId:   id.NodeID(),
		MsgTime:       &microTime,
		Hash:          &hash,
		Key:           (*string)(&cmd.Key),
		Data:          cmd.Data,
	}

	w.Serializer.Serialize(&row)
}

// func (w *BatchWriter) Write(id uuid.UUID, key types.Key, data []byte) {
// 	microTime := uuidutil.ToTime(id.Time()).UnixMicro()
// 	msgIdTime := int64(id.Time()) // 100s of nanos, not epoch
// 	clockSeq := int32(id.ClockSequence())
// 	hash := uint32(key.Hash())

// 	row := v1.Row{
// 		MsgIdTime:     &msgIdTime,
// 		MsgIdClockSeq: &clockSeq,
// 		MsgIdNodeId:   id.NodeID(),
// 		MsgTime:       &microTime,
// 		Hash:          &hash,
// 		Key:           (*string)(&key),
// 		Data:          data,
// 	}

// 	w.Serializer.Serialize(&row)
// }

func (w *BatchWriter) Flush() (rows int) {
	rows = w.Serializer.Rows(func(rows [][]byte) {
		if !w.FlushEnabled {
			return
		}
		backoff := backoff.Backoff{
			Until:      time.Now().Add(1 * time.Minute),
			Interval:   10 * time.Millisecond,
			MaxRetries: 100,
		}

		for backoff.Try() {
			err := w.Writer.Write(rows)
			if err == nil {
				return
			}
			log.WithError(err).Error("could not write to bigquery, retrying...")
		}
		panic("could not write to bigquery after many retries")
	})

	return
}

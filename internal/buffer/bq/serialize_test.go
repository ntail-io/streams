package bq

import (
	"runtime"
	"testing"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/ntail-io/streams/internal/buffer/domain/uuidutil"
	"github.com/ntail-io/streams/internal/core/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRowSerializer_Serialize(t *testing.T) {
	id, _ := uuid.NewUUID()
	data := make([]byte, 1024)
	key := types.Key("a-key")
	microTime := uuidutil.ToTime(id.Time()).UnixMicro()
	msgIdTime := int64(id.Time()) // 100s of nanos, not epoch
	clockSeq := int32(id.ClockSequence())
	hash := uint32(key.Hash())

	row := v1.Row{
		MsgIdTime:     &msgIdTime,
		MsgIdClockSeq: &clockSeq,
		MsgIdNodeId:   id.NodeID(),
		MsgTime:       &microTime,
		Hash:          &hash,
		Key:           (*string)(&key),
		Data:          data,
	}

	ser := NewRowSerializer(1)

	ser.Serialize(&row)

	row2 := v1.Row{}
	ser.Rows(func(rows [][]byte) {
		err := row2.UnmarshalVT(rows[0])
		assert.Nil(t, err)
	})
	assert.Equal(t, row.String(), row2.String())
}

func TestRowSerializer_Serialize_Many(t *testing.T) {
	id, _ := uuid.NewUUID()
	data := make([]byte, 1024)
	key := types.Key("a-key")
	microTime := uuidutil.ToTime(id.Time()).UnixMicro()
	msgIdTime := int64(id.Time()) // 100s of nanos, not epoch
	clockSeq := int32(id.ClockSequence())
	hash := uint32(key.Hash())

	row := v1.Row{
		MsgIdTime:     &msgIdTime,
		MsgIdClockSeq: &clockSeq,
		MsgIdNodeId:   id.NodeID(),
		MsgTime:       &microTime,
		Hash:          &hash,
		Key:           (*string)(&key),
		Data:          data,
	}

	ser := NewRowSerializer(1)

	for i := 0; i < 4; i++ {
		ser.Serialize(&row)
	}
	ser.Rows(func(rows [][]byte) {
		assert.Equal(t, 4, len(rows))
	})
}

// Benchmarks the row serializer, should have 0 allocs/op.
func BenchmarkRowSerializer(b *testing.B) {
	ser := NewRowSerializer(4)

	id, _ := uuid.NewUUID()
	data := make([]byte, 1024)
	key := types.Key("a-key")

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		microTime := uuidutil.ToTime(id.Time()).UnixMicro()
		msgIdTime := int64(id.Time()) // 100s of nanos, not epoch
		clockSeq := int32(id.ClockSequence())
		hash := uint32(key.Hash())

		row := v1.Row{
			MsgIdTime:     &msgIdTime,
			MsgIdClockSeq: &clockSeq,
			MsgIdNodeId:   id.NodeID(),
			MsgTime:       &microTime,
			Hash:          &hash,
			Key:           (*string)(&key),
			Data:          data,
		}

		ser.Serialize(&row)

		if len(ser.fullBuffers) > 0 {
			buf := <-ser.fullBuffers
			buf.Reset()
			ser.freeBuffers <- buf
		}
	}
}

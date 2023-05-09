package bigquery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type offset = int64

type ReaderSession struct {
	client  *storage.BigQueryReadClient
	session *storagepb.ReadSession
	offset  offset
	traceId string
}

func rowRestriction(fromMsgId uuid.UUID) string {
	return fmt.Sprintf("msg_id_time > %[1]d OR (msg_id_time = %[1]d AND msg_id_clock_seq > %[2]d)", fromMsgId.Time(), fromMsgId.ClockSequence())
}

func NewReader(ctx context.Context, client *storage.BigQueryReadClient, projectId string, fromMsgId uuid.UUID, table string, traceId string) (*ReaderSession, error) {
	sessReq := &storagepb.CreateReadSessionRequest{
		Parent:         fmt.Sprintf("projects/%s", projectId),
		MaxStreamCount: 1,
		ReadSession: &storagepb.ReadSession{
			Table:      table,
			DataFormat: storagepb.DataFormat_AVRO,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				SelectedFields: []string{"data", "msg_id_time", "msg_id_clock_seq", "msg_id_node_id"},
				RowRestriction: rowRestriction(fromMsgId),
			},
		},
	}
	session, err := client.CreateReadSession(ctx, sessReq)
	if err != nil {
		return nil, err
	}
	if session.GetStreams() == nil {
		return nil, status.Error(codes.NotFound, "no streams found")
	}
	r := &ReaderSession{
		client:  client,
		session: session,
		offset:  0,
		traceId: traceId,
	}

	if _, err = client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: r.stream()}); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *ReaderSession) stream() string {
	return r.session.GetStreams()[0].Name
}

func (r *ReaderSession) Poll(ctx context.Context, buf []byte) (out []byte, lastMsgId uuid.UUID, err error) {
	req := &storagepb.ReadRowsRequest{
		ReadStream: r.stream(),
		Offset:     r.offset,
	}
	rowsClient, err := r.client.ReadRows(ctx, req)
	if err != nil {
		return
	}

	out = buf

	rowsResp, err := rowsClient.Recv()
	if err != nil {
		if err = rowsClient.CloseSend(); err != nil {
			log.WithError(err).Fatal("failed to close rows client")
		}
		return
	}
	out, lastMsgId, err = decodeAvro(rowsResp.GetAvroRows().GetSerializedBinaryRows(), r.session.GetAvroSchema().GetSchema(), out)
	if err != nil {
		if err = rowsClient.CloseSend(); err != nil {
			log.WithError(err).Fatal("failed to close rows client")
		}
		return
	}
	r.offset += rowsResp.GetRowCount()
	return
}

func decodeAvro(bytes []byte, schema string, buf []byte) (out []byte, lastMsgId uuid.UUID, err error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return
	}

	if len(buf) < 1<<20*10 {
		out = make([]byte, 1<<20*10)
	} else {
		out = buf[:cap(buf)]
	}

	outLen := 0

	for len(bytes) > 0 {
		datum, remaining, err := codec.NativeFromBinary(bytes)
		if err != nil {
			return out, lastMsgId, err
		}
		bytes = remaining
		m, ok := datum.(map[string]interface{})
		if !ok {
			err = errors.New("could not cast datum as map")
			return out, lastMsgId, err
		}

		msgIdTime := m["msg_id_time"].(int64)
		msgIdClockSeq := m["msg_id_clock_seq"].(int64)
		msgIdNodeId := m["msg_id_node_id"].([]byte)
		lastMsgId = ParseUUID(uuid.Time(msgIdTime), uint16(msgIdClockSeq), msgIdNodeId)
		data := m["data"].([]byte)

		copy(out[outLen:], lastMsgId[:])
		outLen += len(lastMsgId)
		binary.LittleEndian.PutUint32(out[outLen:], uint32(len(data)))
		outLen += 4
		copy(out[outLen:], data)
		outLen += len(data)

		log.Infof("poll msg_id: %s", lastMsgId.String())
	}

	out = out[:outLen]
	return
}

func ParseUUID(time uuid.Time, seq uint16, nodeId []byte) uuid.UUID {
	if len(nodeId) != 6 {
		panic("nodeId must be 6 bytes")
	}
	var id uuid.UUID

	timeLow := uint32(time & 0xffffffff)
	timeMid := uint16((time >> 32) & 0xffff)
	timeHi := uint16((time >> 48) & 0x0fff)
	timeHi |= 0x1000 // Version 1

	binary.BigEndian.PutUint32(id[0:], timeLow)
	binary.BigEndian.PutUint16(id[4:], timeMid)
	binary.BigEndian.PutUint16(id[6:], timeHi)
	binary.BigEndian.PutUint16(id[8:], seq)

	copy(id[10:], nodeId[:])

	return id
}

package bq

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/ntail-io/streams/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ntail-io/streams/core/types"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/reflect/protodesc"
)

type Offset = int64

type Writer struct {
	topic   types.Topic
	tableId string
	conn    *BigQueryConn
	stream  *managedwriter.ManagedStream
	offset  Offset
	ctx     context.Context
}

type BigQueryConn struct {
	ProjectId    string
	WriterClient *managedwriter.Client
	QueryClient  *bigquery.Client
}

func NewWriter(ctx context.Context, conn *BigQueryConn, topic types.Topic, hr types.HashRange, from uuid.Time) *Writer {
	s := &Writer{
		topic:   topic,
		tableId: tableId(hr, from),
		conn:    conn,
		offset:  0,
		ctx:     ctx,
	}

	//go s.sender()
	return s
}

func (s *Writer) Init() error {
	m := &v1.Row{}
	descriptorProto := protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())

	if err := createDatasetIfNotExists(s.ctx, s.conn.QueryClient, s.conn.ProjectId, string(s.topic)); err != nil {
		return err
	}
	if err := createTable(s.ctx, s.conn.QueryClient, s.conn.ProjectId, string(s.topic), s.tableId); err != nil {
		return err
	}
	table := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", s.conn.ProjectId, string(s.topic), s.tableId)
	stream, err := s.conn.WriterClient.NewManagedStream(
		s.ctx,
		//managedwriter.WithStreamName(streamName),
		//managedwriter.WithTraceID(traceId),
		//managedwriter.WithDataOrigin(dataOrigin),
		managedwriter.WithDestinationTable(table),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return err
	}
	s.stream = stream
	return nil
}

func (s *Writer) Write(rows [][]byte) (err error) {
	defer measureWrite(s.tableId, len(rows), time.Now())

	res, err := s.stream.AppendRows(
		s.ctx,
		rows,
	)
	if err != nil {
		return
	}

	_, err = res.GetResult(s.ctx)

	log.Debugf("wrote %d rows to bigquery", len(rows))
	return
}

var (
	histWrite    = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "bq_write"}, []string{"table_id"})
	counterWrite = promauto.NewCounterVec(prometheus.CounterOpts{Name: "bq_write_rows"}, []string{"table_id"})
)

func measureWrite(tableId string, rows int, since time.Time) {
	histWrite.WithLabelValues(tableId).Observe(float64(time.Since(since).Nanoseconds()))
	counterWrite.WithLabelValues(tableId).Add(float64(rows))
}

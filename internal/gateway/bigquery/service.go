package bigquery

import (
	"context"
	"fmt"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/google/uuid"
	"github.com/ntail-io/streams/internal/gateway/domain"
)

type ReaderService struct {
	client  *storage.BigQueryReadClient
	project string
}

func NewReaderService(client *storage.BigQueryReadClient, project string) *ReaderService {
	return &ReaderService{
		client:  client,
		project: project,
	}
}

func (s *ReaderService) NewSession(ctx context.Context, addr *domain.SegmentAddress, fromMsgId uuid.UUID) (*ReaderSession, error) {
	topic := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", s.project, addr.SegmentId.Topic, addr.SegmentId.BigQueryTable())
	return NewReader(ctx, s.client, s.project, fromMsgId, topic, "")
}

package buftypes

import (
	"github.com/ntail-io/streams/internal/core/types"

	"github.com/google/uuid"
)

type SegmentAddress struct {
	Topic     types.Topic
	HashRange types.HashRange
	From      uuid.Time
}

type LeaseAddress struct {
	Topic     types.Topic
	HashRange types.HashRange
}

package buftypes

import (
	"github.com/ntail-io/streams/core/types"

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

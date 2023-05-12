package etcd

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/domain"
	"github.com/stretchr/testify/assert"
)

func TestParseSegmentKey(t *testing.T) {
	actual, err := parseSegmentKey("/segments/topic1/16801584610000000_0-32")
	expected := domain.SegmentId{
		Topic:     "topic1",
		HashRange: types.HashRange{From: 0, To: 32},
		TimeFrom:  16801584610000000,
	}
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestParseBufferSegmentKey(t *testing.T) {
	actual, err := parseBufferSegmentKey("/buffer_segments/topic1/16801584610000000_0-32")
	expected := domain.SegmentId{
		Topic:     "topic1",
		HashRange: types.HashRange{From: 0, To: 32},
		TimeFrom:  16801584610000000,
	}
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestParseSubscriptionKey(t *testing.T) {
	topic, sub, err := parseSubscriptionKey("/subscriptions/topic1/subscription1")
	assert.NoError(t, err)
	assert.Equal(t, types.Topic("topic1"), topic)
	assert.Equal(t, types.SubscriptionId("subscription1"), sub)
}

func TestParseBookmarkKey(t *testing.T) {
	subId, segId, err := parseBookmarkKey("/bookmarks/topic1/subscription1/138994597963355337_8192-16383")
	assert.NoError(t, err)
	assert.Equal(t, types.SubscriptionId("subscription1"), subId)
	assert.Equal(t, domain.SegmentId{
		Topic:     "topic1",
		HashRange: types.HashRange{From: 8192, To: 16383},
		TimeFrom:  uuid.Time(138994597963355337),
	}, segId)
}

func TestParseBookmarkLeaseKey(t *testing.T) {
	subId, segId, err := parseBookmarkLeaseKey("/bookmark_leases/topic1/subscription1/138994597963355337_8192-16383")
	assert.NoError(t, err)
	assert.Equal(t, types.SubscriptionId("subscription1"), subId)
	assert.Equal(t, domain.SegmentId{
		Topic:     "topic1",
		HashRange: types.HashRange{From: 8192, To: 16383},
		TimeFrom:  uuid.Time(138994597963355337),
	}, segId)
}

func TestParseBufferAddressKey(t *testing.T) {
	topic, hashRange, err := parseBufferAddressKey("/hash_range_leases/topic1/0-32")
	assert.NoError(t, err)
	assert.Equal(t, types.Topic("topic1"), topic)
	assert.Equal(t, types.HashRange{From: 0, To: 32}, hashRange)
}

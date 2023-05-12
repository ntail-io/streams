package bufclient

import (
	"time"

	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway"
	"github.com/ntail-io/streams/gateway/domain"
	v1 "github.com/ntail-io/streams/proto/v1"
)

func Append(pool *AppendStreamPool, ts *gateway.TopicService, req *v1.AppendRequest) (err error) {
	topic, err := ts.Get(types.Topic(req.GetTopic()))
	if err != nil {
		return
	}
	key, err := types.NewKey(req.GetKey())
	if err != nil {
		return
	}

	var addr domain.BufferAddress
	for i := 0; i < 3; i++ {
		addr, err = topic.AddressOfHash(key.Hash())
		// retry getting address
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return
	}

	pool.Append(addr, req)
	return nil
}

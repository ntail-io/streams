package buffer

import (
	"context"

	"github.com/ntail-io/streams/buffer/buftypes"
	"github.com/ntail-io/streams/buffer/domain"

	log "github.com/sirupsen/logrus"
)

// SegCloser listens on the Full channel of the allocator and closes the oldest segments.
// SegmentsCh is a buffered channel that is used to hold all non closed segments. When a new segment is created, it is important to close the oldest segment.

func RunSegCloser(ctx context.Context, router *Router, allocator *domain.ChunkAllocator, finishedSegmentsCh chan *buftypes.SegmentAddress) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-allocator.FullCh:
			addr := <-finishedSegmentsCh
			log.Infof("seg closer closing segment: %v", addr)
			if err := router.CloseSegment(*addr); err != nil {
				log.WithError(err).Fatal("seg closer failed to close segment")
				return
			}
			log.Infof("seg closer closed segment %v", addr)
		}
	}
}

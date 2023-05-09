package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashRangeSize_HashRanges(t *testing.T) {
	var hr HashRangeSize = 15
	ranges := hr.HashRanges()
	assert.Len(t, ranges, 2)
	assert.Equal(t, Hash(0), ranges[0].From)
	assert.Equal(t, Hash(32767), ranges[0].To)
	assert.Equal(t, Hash(32768), ranges[1].From)
	assert.Equal(t, Hash(65535), ranges[1].To)
}

func TestKey_Hash_Deterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		hash := Key("key-1").Hash()
		assert.Equal(t, Hash(11795), hash)
	}
}

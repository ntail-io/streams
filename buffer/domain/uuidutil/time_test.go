package uuidutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFromTime(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	uuidNow := FromTime(now)
	assert.Equal(t, now, ToTime(uuidNow).Truncate(time.Microsecond))
}

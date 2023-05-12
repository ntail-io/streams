package uuidutil

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestCmp(t *testing.T) {
	a, _ := uuid.NewUUID()
	b, _ := uuid.NewUUID()
	assert.Equal(t, 0, Cmp(a, a))
	assert.Equal(t, 0, Cmp(b, b))
	assert.Equal(t, -1, Cmp(a, b))
	assert.Equal(t, 1, Cmp(b, a))
}

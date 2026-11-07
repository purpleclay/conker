package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStream_WithContext_CancelsReplacedContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := New().WithContext(parent)
	replaced := s.ctx
	s.WithContext(parent)

	assert.Error(t, replaced.Err(), "the replaced context must be cancelled so it detaches from its parent")
}

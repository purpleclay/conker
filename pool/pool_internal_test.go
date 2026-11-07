package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultPool_StoresEachErrorOnce(t *testing.T) {
	p := NewWithResults[int]()
	for range 3 {
		p.Go(func(_ context.Context) (int, error) { return 0, errors.New("task failed") })
	}

	_, err := p.Wait()
	require.Error(t, err)
	assert.Len(t, p.Errors(), 3)
	assert.Empty(t, p.pool.errs, "errors must live only in the results slice, not also in the inner pool")
}

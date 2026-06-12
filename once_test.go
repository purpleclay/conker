package conker_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/purpleclay/conker"
)

func TestOnceErr_CallsFnAtMostOnce(t *testing.T) {
	var calls atomic.Int32
	once := conker.OnceErr(func() (int, error) {
		calls.Add(1)
		return 42, nil
	})

	for range 3 {
		val, err := once()
		require.NoError(t, err)
		assert.Equal(t, 42, val)
	}

	assert.Equal(t, int32(1), calls.Load())
}

func TestOnceErr_CachesError(t *testing.T) {
	wantErr := errors.New("boom")

	var calls atomic.Int32
	once := conker.OnceErr(func() (int, error) {
		calls.Add(1)
		return 0, wantErr
	})

	for range 3 {
		val, err := once()
		assert.Equal(t, 0, val)
		assert.ErrorIs(t, err, wantErr)
	}

	assert.Equal(t, int32(1), calls.Load())
}

func TestOnceErr_ConcurrentCallers_CallFnOnceAndShareResult(t *testing.T) {
	wantErr := errors.New("boom")

	var calls atomic.Int32
	once := conker.OnceErr(func() (int, error) {
		calls.Add(1)
		return 7, wantErr
	})

	const n = 50
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			val, err := once()
			assert.Equal(t, 7, val)
			assert.ErrorIs(t, err, wantErr)
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load())
}

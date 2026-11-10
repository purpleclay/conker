package conker_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/purpleclay/conker"
	"github.com/purpleclay/conker/panics"
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

func TestOnceErr_PanicReturnedAsErrorToEveryCaller(t *testing.T) {
	var calls atomic.Int32
	once := conker.OnceErr(func() (*int, error) {
		calls.Add(1)
		panic("boom")
	})

	var first error
	for i := range 3 {
		var (
			val *int
			err error
		)
		require.NotPanics(t, func() { val, err = once() })
		assert.Nil(t, val)
		require.ErrorIs(t, err, panics.ErrPanic)

		var rec *panics.Recovered
		require.ErrorAs(t, err, &rec)
		assert.Equal(t, "boom", rec.Value)

		if i == 0 {
			first = err
		}
		assert.Same(t, first, err)
	}

	assert.Equal(t, int32(1), calls.Load())
}

func TestOnceErr_ConcurrentCallers_ShareRecoveredPanic(t *testing.T) {
	var calls atomic.Int32
	once := conker.OnceErr(func() (int, error) {
		calls.Add(1)
		panic("boom")
	})

	const n = 50
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			assert.NotPanics(t, func() { _, errs[i] = once() })
		})
	}
	wg.Wait()

	var rec *panics.Recovered
	require.ErrorAs(t, errs[0], &rec)
	for _, err := range errs {
		assert.Same(t, rec, err)
	}
	assert.Equal(t, int32(1), calls.Load())
}

package conker_test

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/purpleclay/conker"
	"github.com/purpleclay/conker/panics"
)

func TestWaitGroup_Wait_RepanicsWithRecovered(t *testing.T) {
	var wg conker.WaitGroup
	wg.Go(func() { panic("boom") })

	v := func() (val any) {
		defer func() { val = recover() }()
		wg.Wait()
		return nil
	}()

	r, ok := v.(*panics.Recovered)
	require.True(t, ok, "repanic value type = %T, want *panics.Recovered", v)
	assert.Equal(t, "boom", r.Value)
}

func TestWaitGroup_Wait_StackPreservedAtPanicSite(t *testing.T) {
	var wg conker.WaitGroup
	wg.Go(func() {
		panic("boom")
	})

	v := func() (val any) {
		defer func() { val = recover() }()
		wg.Wait()
		return nil
	}()

	r, ok := v.(*panics.Recovered)
	require.True(t, ok)
	// Stack must reference the panic frame, not just the file (which also
	// contains the Wait call site and would produce a false positive).
	stack := string(r.Stack)
	assert.True(t, strings.Contains(stack, "waitgroup_test.go"),
		"stack trace does not include panic file:\n%s", r.Stack)
	assert.True(t, strings.Contains(stack, "TestWaitGroup_Wait_StackPreservedAtPanicSite.func1"),
		"stack trace does not include panic frame:\n%s", r.Stack)
}

func TestWaitGroup_WaitAndRecover_NilWhenNoPanic(t *testing.T) {
	var wg conker.WaitGroup
	wg.Go(func() {})

	assert.Nil(t, wg.WaitAndRecover())
}

func TestWaitGroup_WaitAndRecover_ReturnsRecovered(t *testing.T) {
	var wg conker.WaitGroup
	wg.Go(func() { panic("captured") })

	r := wg.WaitAndRecover()
	require.NotNil(t, r)
	assert.Equal(t, "captured", r.Value)
	assert.ErrorIs(t, r, panics.ErrPanic)
}

func TestWaitGroup_NoGoroutines_WaitIsNoOp(t *testing.T) {
	var wg conker.WaitGroup
	assert.NotPanics(t, wg.Wait)
}

func TestWaitGroup_NoGoroutines_WaitAndRecoverIsNoOp(t *testing.T) {
	var wg conker.WaitGroup
	assert.Nil(t, wg.WaitAndRecover())
}

func TestWaitGroup_FirstPanicWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var wg conker.WaitGroup

		first := errors.New("first")
		second := errors.New("second")

		wg.Go(func() { panic(first) })
		wg.Go(func() {
			time.Sleep(time.Millisecond)
			panic(second)
		})

		r := wg.WaitAndRecover()
		require.NotNil(t, r)
		assert.Equal(t, first, r.Value)
	})
}

func TestWaitGroup_ReuseAfterPanic_AlwaysReturnsStalePanic(t *testing.T) {
	var wg conker.WaitGroup

	first := errors.New("first panic")
	wg.Go(func() { panic(first) })
	r1 := wg.WaitAndRecover()
	require.NotNil(t, r1)

	// The Catcher still holds the first panic. A new panic in cycle 2 loses
	// the CAS and is silently dropped; WaitAndRecover returns the stale value.
	second := errors.New("second panic")
	wg.Go(func() { panic(second) })
	r2 := wg.WaitAndRecover()

	require.NotNil(t, r2)
	assert.Equal(t, first, r2.Value, "stale first panic returned — create a new WaitGroup after a panic")
}

func TestWaitGroup_Reusable(t *testing.T) {
	var wg conker.WaitGroup

	// First cycle.
	result := 0
	wg.Go(func() { result = 1 })
	wg.Wait()
	assert.Equal(t, 1, result)

	// Second cycle — sync.WaitGroup must be freely reusable after Wait returns.
	wg.Go(func() { result = 2 })
	wg.Wait()
	assert.Equal(t, 2, result)
}

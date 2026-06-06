package panics_test

import (
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/purpleclay/conker/panics"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type stubError struct{ msg string }

func (e *stubError) Error() string { return e.msg }

func TestRecovered_NilReceiver(t *testing.T) {
	var r *panics.Recovered

	assert.NotPanics(t, func() { _ = r.Error() })
	assert.Equal(t, "panic: <nil>", r.Error())
	assert.False(t, r.Is(panics.ErrPanic), "nil Recovered should not match ErrPanic")
	assert.NotPanics(t, func() { _ = r.Unwrap() })
	assert.Nil(t, r.Unwrap())
}

func TestRecovered_IsErrPanic(t *testing.T) {
	var c panics.Catcher
	c.Try(func() { panic("boom") })

	r := c.Recovered()
	require.NotNil(t, r)
	assert.ErrorIs(t, r, panics.ErrPanic)
}

func TestRecovered_UnwrapsError(t *testing.T) {
	inner := &stubError{"inner"}
	var c panics.Catcher
	c.Try(func() { panic(inner) })

	r := c.Recovered()
	require.NotNil(t, r)

	var got *stubError
	require.ErrorAs(t, r, &got)
	assert.Same(t, inner, got)
}

func TestRecovered_UnwrapNilForNonError(t *testing.T) {
	var c panics.Catcher
	c.Try(func() { panic("not an error") })

	r := c.Recovered()
	require.NotNil(t, r)
	assert.Nil(t, r.Unwrap())
}

func TestRecovered_ErrorString(t *testing.T) {
	var c panics.Catcher
	c.Try(func() { panic("boom") })

	r := c.Recovered()
	require.NotNil(t, r)
	assert.Contains(t, r.Error(), "boom")
}

func TestCatcher_NoPanic(t *testing.T) {
	var c panics.Catcher
	c.Try(func() {})

	assert.Nil(t, c.Recovered())
}

func TestCatcher_RepanicIsNoOpWhenNoPanic(t *testing.T) {
	var c panics.Catcher
	c.Try(func() {})

	assert.NotPanics(t, c.Repanic)
}

func TestCatcher_RepanicWithRecovered(t *testing.T) {
	var c panics.Catcher
	c.Try(func() { panic("boom") })

	v := func() (val any) {
		defer func() { val = recover() }()
		c.Repanic()
		return nil
	}()

	r, ok := v.(*panics.Recovered)
	require.True(t, ok, "repanic value type = %T, want *panics.Recovered", v)
	assert.Equal(t, "boom", r.Value)
}

func TestCatcher_StackCapturedAtPanicSite(t *testing.T) {
	var c panics.Catcher
	c.Try(func() {
		panic("boom")
	})

	r := c.Recovered()
	require.NotNil(t, r)
	require.NotEmpty(t, r.Stack)
	// Stack must reference this file (the panic site), not just panics package internals.
	assert.True(t, strings.Contains(string(r.Stack), "panics_test.go"),
		"stack trace does not include panic site:\n%s", r.Stack)
}

// TestCatcher_FirstPanicWins verifies that under concurrent Try calls,
// only the first panic is stored and subsequent panics are discarded.
func TestCatcher_FirstPanicWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c panics.Catcher
		var wg sync.WaitGroup

		first := &stubError{"first"}
		second := &stubError{"second"}

		// Goroutine 1 panics immediately; goroutine 2 panics after a fake-time delay.
		// Both must finish before the root goroutine exits (wg.Wait is durable-blocking
		// inside a synctest bubble, so fake time advances to unblock goroutine 2).
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Try(func() { panic(first) })
		}()
		go func() {
			defer wg.Done()
			c.Try(func() {
				time.Sleep(time.Millisecond)
				panic(second)
			})
		}()

		wg.Wait()

		r := c.Recovered()
		require.NotNil(t, r)
		assert.Equal(t, first, r.Value)
	})
}

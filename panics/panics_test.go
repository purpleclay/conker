package panics_test

import (
	"errors"
	"fmt"
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

func TestRecovered_FormatNilReceiver(t *testing.T) {
	var r *panics.Recovered

	assert.Equal(t, "panic: <nil>", fmt.Sprintf("%+v", r))
}

func TestRecovered_JoinedErrorsStayReadable(t *testing.T) {
	var c1, c2 panics.Catcher
	c1.Try(func() { panic("first") })
	c2.Try(func() { panic("second") })

	err := errors.Join(c1.Recovered(), c2.Recovered())
	assert.Equal(t, "panic: first\npanic: second", err.Error())
}

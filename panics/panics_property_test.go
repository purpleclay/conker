package panics_test

import (
	"errors"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/purpleclay/conker/panics"
)

var errUnrelated = errors.New("unrelated")

// drawPanicValue returns a value to panic with, of one of the kinds callers
// hit in practice: a string, an int, an error, or a struct.
func drawPanicValue(t *rapid.T) any {
	switch rapid.IntRange(0, 3).Draw(t, "kind") {
	case 0:
		return rapid.String().Draw(t, "string")
	case 1:
		return rapid.Int().Draw(t, "int")
	case 2:
		return &stubError{rapid.String().Draw(t, "msg")}
	default:
		return struct{ N int }{rapid.Int().Draw(t, "n")}
	}
}

// drawDirective returns a fmt directive other than %+v, which is the only one
// that prints more than Error.
func drawDirective(t *rapid.T) string {
	verb := rapid.SampledFrom([]rune("vsqxXd")).Draw(t, "verb")
	flags := ""
	for _, f := range "-# 0+" {
		if (f != '+' || verb != 'v') && rapid.Bool().Draw(t, "flag "+string(f)) {
			flags += string(f)
		}
	}
	directive := "%" + flags
	if rapid.Bool().Draw(t, "hasWidth") {
		directive += fmt.Sprint(rapid.IntRange(0, 20).Draw(t, "width"))
	}
	if rapid.Bool().Draw(t, "hasPrecision") {
		directive += "." + fmt.Sprint(rapid.IntRange(0, 20).Draw(t, "precision"))
	}
	return directive + string(verb)
}

// TestRecovered_Property checks that a recovered panic of any value reports
// "panic: <value>" as its error, formats exactly like that string for every
// directive except %+v, appends the stack for %+v, always matches ErrPanic,
// and unwraps to the panic value only when that value is an error.
func TestRecovered_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := drawPanicValue(t)
		var c panics.Catcher
		c.Try(func() { panic(v) })
		r := c.Recovered()
		if r == nil || r.Value != v {
			t.Fatalf("recovered %v, want a Recovered holding %v", r, v)
		}

		if want := "panic: " + fmt.Sprint(v); r.Error() != want {
			t.Fatalf("Error() = %q, want %q", r.Error(), want)
		}
		directive := drawDirective(t)
		if got, want := fmt.Sprintf(directive, r), fmt.Sprintf(directive, r.Error()); got != want {
			t.Fatalf("Sprintf(%q) = %q, want %q", directive, got, want)
		}
		if got, want := fmt.Sprintf("%+v", r), r.Error()+"\n\n"+string(r.Stack); got != want {
			t.Fatalf("Sprintf(%%+v) = %q, want the error and stack", got)
		}

		if !errors.Is(r, panics.ErrPanic) {
			t.Fatalf("errors.Is(%v, ErrPanic) = false", r)
		}
		if errors.Is(r, errUnrelated) {
			t.Fatalf("errors.Is(%v, errUnrelated) = true", r)
		}
		inner, isErr := v.(error)
		var se *stubError
		if got := errors.As(r, &se); got != isErr {
			t.Fatalf("errors.As to the panic value's type = %v, want %v", got, isErr)
		}
		if isErr && (r.Unwrap() != inner || !errors.Is(r, inner)) {
			t.Fatalf("Unwrap() = %v, want the panic value %v", r.Unwrap(), inner)
		}
		if !isErr && r.Unwrap() != nil {
			t.Fatalf("Unwrap() = %v for a non-error panic value", r.Unwrap())
		}
	})
}

// TestCatcher_Property runs random sequences of Try (with and without a
// panic), Reset, and Repanic against a model in which the first panic since
// the last Reset is kept.
func TestCatcher_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var c panics.Catcher
		var kept any // the model; nil when nothing is captured
		ops := rapid.SliceOfN(rapid.IntRange(0, 3), 0, 20).Draw(t, "ops")
		for i, op := range ops {
			switch op {
			case 0:
				c.Try(func() {})
			case 1:
				c.Try(func() { panic(i) })
				if kept == nil {
					kept = i
				}
			case 2:
				c.Reset()
				kept = nil
			case 3:
				repanicked := func() (v any) {
					defer func() { v = recover() }()
					c.Repanic()
					return nil
				}()
				if kept == nil && repanicked != nil {
					t.Fatalf("op %d: Repanic panicked with %v with nothing captured", i, repanicked)
				}
				if r, _ := repanicked.(*panics.Recovered); kept != nil && (r == nil || r.Value != kept) {
					t.Fatalf("op %d: Repanic panicked with %v, want the panic from op %v", i, repanicked, kept)
				}
			}

			r := c.Recovered()
			if kept == nil && r != nil {
				t.Fatalf("op %d: Recovered() = %v, want nil", i, r)
			}
			if kept != nil && (r == nil || r.Value != kept) {
				t.Fatalf("op %d: Recovered() = %v, want the panic from op %v", i, r, kept)
			}
		}
	})
}

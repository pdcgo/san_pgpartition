package san_pgpartition

import (
	"errors"
	"math"
	"testing"
	"time"
)

type stringerValue struct{ text string }

func (s stringerValue) String() string { return s.text }

func TestLiteralText(t *testing.T) {
	jakarta := time.FixedZone("WIB", 7*60*60)

	cases := []struct {
		name  string
		value any
		want  string
	}{
		{name: "string", value: "2026-01-01", want: "2026-01-01"},
		{name: "string with a quote", value: "o'brien", want: "o'brien"},
		{name: "int", value: 42, want: "42"},
		{name: "negative int64", value: int64(-7), want: "-7"},
		{name: "uint8", value: uint8(200), want: "200"},
		{name: "float", value: 1.5, want: "1.5"},
		{name: "bool", value: true, want: "true"},
		{name: "time keeps its offset", value: time.Date(2026, 1, 1, 0, 0, 0, 0, jakarta), want: "2026-01-01T00:00:00+07:00"},
		{name: "fmt.Stringer", value: stringerValue{text: "00000000-0000-0000-0000-000000000001"}, want: "00000000-0000-0000-0000-000000000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := literalText(tc.value)
			if err != nil {
				t.Fatalf("literalText(%v): %v", tc.value, err)
			}
			if got == nil || *got != tc.want {
				t.Fatalf("literalText(%v) = %v, want %q", tc.value, got, tc.want)
			}
		})
	}

	t.Run("nil is NULL", func(t *testing.T) {
		got, err := literalText(nil)
		if err != nil || got != nil {
			t.Fatalf("literalText(nil) = %v, %v; want nil, nil", got, err)
		}
	})

	rejected := []struct {
		name  string
		value any
	}{
		{name: "NaN", value: math.NaN()},
		{name: "infinity", value: math.Inf(1)},
		{name: "NUL byte", value: "a\x00b"},
		{name: "unsupported type", value: struct{}{}},
		{name: "slice", value: []string{"a"}},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := literalText(tc.value)
			if !errors.Is(err, ErrInvalidBound) {
				t.Fatalf("literalText(%v) error = %v, want ErrInvalidBound", tc.value, err)
			}
		})
	}
}

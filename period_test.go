package san_pgpartition

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestDateChildren(t *testing.T) {
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	newYork := mustLoadLocation(t, "America/New_York")

	cases := []struct {
		name      string
		rangeType RangeType
		keyType   string
		loc       *time.Location
		min, max  time.Time
		want      []childRange
	}{
		{
			name:      "monthly timestamptz: a month starts at 00:00 in the Location",
			rangeType: RangeTypeMonthly,
			keyType:   keyTimestamptz,
			loc:       jakarta,
			// Both instants are on 2026-10-31 in UTC.
			min: time.Date(2026, 10, 31, 23, 30, 0, 0, jakarta).UTC(),
			max: time.Date(2026, 11, 1, 0, 10, 0, 0, jakarta).UTC(),
			want: []childRange{
				{suffix: "_202610", from: "2026-10-01T00:00:00+07:00", to: "2026-11-01T00:00:00+07:00"},
				{suffix: "_202611", from: "2026-11-01T00:00:00+07:00", to: "2026-12-01T00:00:00+07:00"},
			},
		},
		{
			name:      "daily timestamptz",
			rangeType: RangeTypeDaily,
			keyType:   keyTimestamptz,
			loc:       jakarta,
			min:       time.Date(2026, 11, 16, 23, 59, 0, 0, jakarta),
			max:       time.Date(2026, 11, 17, 0, 0, 0, 0, jakarta),
			want: []childRange{
				{suffix: "_20261116", from: "2026-11-16T00:00:00+07:00", to: "2026-11-17T00:00:00+07:00"},
				{suffix: "_20261117", from: "2026-11-17T00:00:00+07:00", to: "2026-11-18T00:00:00+07:00"},
			},
		},
		{
			name:      "yearly timestamptz",
			rangeType: RangeTypeYearly,
			keyType:   keyTimestamptz,
			loc:       jakarta,
			min:       time.Date(2025, 12, 31, 23, 0, 0, 0, jakarta),
			max:       time.Date(2026, 1, 1, 0, 0, 0, 0, jakarta),
			want: []childRange{
				{suffix: "_2025", from: "2025-01-01T00:00:00+07:00", to: "2026-01-01T00:00:00+07:00"},
				{suffix: "_2026", from: "2026-01-01T00:00:00+07:00", to: "2027-01-01T00:00:00+07:00"},
			},
		},
		{
			name:      "daily across a daylight-saving change keeps 00:00 local",
			rangeType: RangeTypeDaily,
			keyType:   keyTimestamptz,
			loc:       newYork,
			min:       time.Date(2026, 3, 8, 12, 0, 0, 0, newYork),
			max:       time.Date(2026, 3, 8, 12, 0, 0, 0, newYork),
			want: []childRange{
				{suffix: "_20260308", from: "2026-03-08T00:00:00-05:00", to: "2026-03-09T00:00:00-04:00"},
			},
		},
		{
			name:      "timestamp without time zone uses the wall clock",
			rangeType: RangeTypeDaily,
			keyType:   keyTimestamp,
			loc:       jakarta,
			// As the driver returns a timestamp: the wall clock in UTC.
			min: time.Date(2026, 10, 31, 23, 30, 0, 0, time.UTC),
			max: time.Date(2026, 11, 1, 0, 10, 0, 0, time.UTC),
			want: []childRange{
				{suffix: "_20261031", from: "2026-10-31 00:00:00", to: "2026-11-01 00:00:00"},
				{suffix: "_20261101", from: "2026-11-01 00:00:00", to: "2026-11-02 00:00:00"},
			},
		},
		{
			name:      "date",
			rangeType: RangeTypeMonthly,
			keyType:   keyDate,
			loc:       jakarta,
			min:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
			max:       time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			want: []childRange{
				{suffix: "_202601", from: "2026-01-01", to: "2026-02-01"},
				{suffix: "_202602", from: "2026-02-01", to: "2026-03-01"},
				{suffix: "_202603", from: "2026-03-01", to: "2026-04-01"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dateChildren(tc.rangeType, tc.keyType, tc.loc, tc.min, tc.max)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("dateChildren =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

func TestFixedChildren(t *testing.T) {
	cases := []struct {
		name      string
		blockSize int
		min, max  int64
		want      []childRange
	}{
		{
			name:      "BlockSize 4 gives 0-3 and 4-7",
			blockSize: 4,
			min:       0,
			max:       7,
			want: []childRange{
				{suffix: "_0_3", from: "0", to: "4"},
				{suffix: "_4_7", from: "4", to: "8"},
			},
		},
		{
			name:      "values inside one block",
			blockSize: 1000000,
			min:       17,
			max:       999999,
			want: []childRange{
				{suffix: "_0_999999", from: "0", to: "1000000"},
			},
		},
		{
			name:      "negative values: floor division, m for minus",
			blockSize: 4,
			min:       -5,
			max:       0,
			want: []childRange{
				{suffix: "_m8_m5", from: "-8", to: "-4"},
				{suffix: "_m4_m1", from: "-4", to: "0"},
				{suffix: "_0_3", from: "0", to: "4"},
			},
		},
		{
			name:      "BlockSize 1",
			blockSize: 1,
			min:       5,
			max:       6,
			want: []childRange{
				{suffix: "_5_5", from: "5", to: "6"},
				{suffix: "_6_6", from: "6", to: "7"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fixedChildren(tc.blockSize, tc.min, tc.max)
			if err != nil {
				t.Fatalf("fixedChildren: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("fixedChildren =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}

	t.Run("a block past the int64 range is refused", func(t *testing.T) {
		_, err := fixedChildren(4, math.MaxInt64-1, math.MaxInt64)
		if err == nil {
			t.Fatal("fixedChildren accepted a block ending past math.MaxInt64")
		}
	})
}

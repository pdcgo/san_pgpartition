package san_pgpartition

import (
	"math"
	"testing"
	"time"
)

// suffixes lists the name suffixes of children, for short comparisons.
func suffixes(children []childRange) []string {
	var names []string
	for _, c := range children {
		names = append(names, c.suffix)
	}
	return names
}

func timePtr(at time.Time) *time.Time {
	return &at
}

func int64Ptr(n int64) *int64 {
	return &n
}

func TestProvisionDates(t *testing.T) {
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	nov17 := time.Date(2026, 11, 17, 9, 30, 0, 0, jakarta)

	cases := []struct {
		name       string
		rangeType  RangeType
		keyType    string
		now        time.Time
		n          int
		newestEnd  *time.Time
		want       []string
		wantFirst  childRange
		checkFirst bool
	}{
		{
			name:       "month: the current month and the next 6",
			rangeType:  RangeTypeMonthly,
			keyType:    keyTimestamptz,
			now:        nov17,
			n:          6,
			want:       []string{"_202611", "_202612", "_202701", "_202702", "_202703", "_202704", "_202705"},
			wantFirst:  childRange{suffix: "_202611", from: "2026-11-01T00:00:00+07:00", to: "2026-12-01T00:00:00+07:00"},
			checkFirst: true,
		},
		{
			name:      "daily: today and the next 6 days",
			rangeType: RangeTypeDaily,
			keyType:   keyTimestamptz,
			now:       nov17,
			n:         6,
			want:      []string{"_20261117", "_20261118", "_20261119", "_20261120", "_20261121", "_20261122", "_20261123"},
		},
		{
			name:      "year: this year and the next 6",
			rangeType: RangeTypeYearly,
			keyType:   keyTimestamptz,
			now:       nov17,
			n:         6,
			want:      []string{"_2026", "_2027", "_2028", "_2029", "_2030", "_2031", "_2032"},
		},
		{
			name:      "the current period is the Location's: 18:00 UTC on Oct 31 is November in Jakarta",
			rangeType: RangeTypeMonthly,
			keyType:   keyTimestamptz,
			now:       time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC),
			n:         1,
			want:      []string{"_202611", "_202612"},
		},
		{
			name:      "a missed cron: every period from the newest child's end is filled",
			rangeType: RangeTypeMonthly,
			keyType:   keyTimestamptz,
			now:       time.Date(2027, 3, 5, 8, 0, 0, 0, jakarta),
			n:         1,
			newestEnd: timePtr(time.Date(2026, 12, 1, 0, 0, 0, 0, jakarta)),
			want:      []string{"_202612", "_202701", "_202702", "_202703", "_202704"},
		},
		{
			name:      "children already reaching past the horizon change nothing",
			rangeType: RangeTypeMonthly,
			keyType:   keyTimestamptz,
			now:       nov17,
			n:         1,
			newestEnd: timePtr(time.Date(2027, 8, 1, 0, 0, 0, 0, jakarta)),
			want:      []string{"_202611", "_202612"},
		},
		{
			name:       "timestamp key: today by the Location's wall clock, bounds without a zone",
			rangeType:  RangeTypeDaily,
			keyType:    keyTimestamp,
			now:        time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC),
			n:          1,
			want:       []string{"_20261101", "_20261102"},
			wantFirst:  childRange{suffix: "_20261101", from: "2026-11-01 00:00:00", to: "2026-11-02 00:00:00"},
			checkFirst: true,
		},
		{
			name:       "date key",
			rangeType:  RangeTypeMonthly,
			keyType:    keyDate,
			now:        nov17,
			n:          1,
			want:       []string{"_202611", "_202612"},
			wantFirst:  childRange{suffix: "_202611", from: "2026-11-01", to: "2026-12-01"},
			checkFirst: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := provisionDates(tc.rangeType, tc.keyType, jakarta, tc.now, tc.n, tc.newestEnd)
			mustEqualStrings(t, "children", suffixes(got), tc.want)
			if tc.checkFirst && got[0] != tc.wantFirst {
				t.Fatalf("first child = %+v, want %+v", got[0], tc.wantFirst)
			}
		})
	}
}

func TestProvisionBlocks(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		highest   *int64
		newestEnd *int64
		want      []string
	}{
		{
			name:      "the highest value's block and the next 6",
			n:         6,
			highest:   int64Ptr(9),
			newestEnd: int64Ptr(12),
			want:      []string{"_8_11", "_12_15", "_16_19", "_20_23", "_24_27", "_28_31", "_32_35"},
		},
		{
			name: "no children and no rows: block 0 and the next 6",
			n:    6,
			want: []string{"_0_3", "_4_7", "_8_11", "_12_15", "_16_19", "_20_23", "_24_27"},
		},
		{
			name:      "children but no rows: from block 0",
			n:         1,
			newestEnd: int64Ptr(8),
			want:      []string{"_0_3", "_4_7"},
		},
		{
			name:      "a newest child ending below the highest value's block: the blocks between are filled",
			n:         1,
			highest:   int64Ptr(9),
			newestEnd: int64Ptr(4),
			want:      []string{"_4_7", "_8_11", "_12_15"},
		},
		{
			name:      "negative values",
			n:         1,
			highest:   int64Ptr(-3),
			newestEnd: int64Ptr(0),
			want:      []string{"_m4_m1", "_0_3"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := provisionBlocks(4, tc.n, tc.highest, tc.newestEnd)
			if err != nil {
				t.Fatalf("provisionBlocks: %v", err)
			}
			mustEqualStrings(t, "children", suffixes(got), tc.want)
		})
	}

	t.Run("blocks past the bigint range are refused", func(t *testing.T) {
		_, err := provisionBlocks(4, 6, int64Ptr(math.MaxInt64-5), nil)
		if err == nil {
			t.Fatal("provisionBlocks accepted blocks ending past math.MaxInt64")
		}
	})
}

func TestDateMoveWindow(t *testing.T) {
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	now := time.Date(2027, 1, 10, 9, 0, 0, 0, jakarta)
	month := func(year int, m time.Month) time.Time {
		return time.Date(year, m, 1, 0, 0, 0, 0, jakarta)
	}

	t.Run("no children: only the current period", func(t *testing.T) {
		lower, upper := dateMoveWindow(RangeTypeMonthly, keyTimestamptz, jakarta, now, nil, nil)
		if !lower.Equal(month(2027, 1)) || !upper.Equal(month(2027, 2)) {
			t.Fatalf("window = [%s, %s), want [2027-01-01, 2027-02-01) in Jakarta", lower, upper)
		}
	})

	t.Run("from the oldest child to the end of the current period", func(t *testing.T) {
		lower, upper := dateMoveWindow(RangeTypeMonthly, keyTimestamptz, jakarta, now, timePtr(month(2026, 10)), timePtr(month(2026, 12)))
		if !lower.Equal(month(2026, 10)) || !upper.Equal(month(2027, 2)) {
			t.Fatalf("window = [%s, %s), want [2026-10-01, 2027-02-01) in Jakarta", lower, upper)
		}
	})

	t.Run("to the newest child's end when it lies past the current period", func(t *testing.T) {
		lower, upper := dateMoveWindow(RangeTypeMonthly, keyTimestamptz, jakarta, now, timePtr(month(2026, 10)), timePtr(month(2027, 6)))
		if !lower.Equal(month(2026, 10)) || !upper.Equal(month(2027, 6)) {
			t.Fatalf("window = [%s, %s), want [2026-10-01, 2027-06-01) in Jakarta", lower, upper)
		}
	})
}

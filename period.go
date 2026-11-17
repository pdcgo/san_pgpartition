package san_pgpartition

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Key column types a conversion accepts, as format_type prints them.
const (
	keyTimestamptz = "timestamp with time zone"
	keyTimestamp   = "timestamp without time zone"
	keyDate        = "date"
	keySmallint    = "smallint"
	keyInteger     = "integer"
	keyBigint      = "bigint"
)

// childRange is one child partition still to be created: its name suffix and
// its bound values as text, FROM inclusive and TO exclusive.
type childRange struct {
	suffix string
	from   string
	to     string
}

// dateChildren returns one child per day, month or year from the period
// holding lowest to the period holding highest, in order.
//
// A timestamptz key is cut at 00:00 in loc, and each bound carries its offset
// so PostgreSQL reads the exact instant. A timestamp key has no zone: it is
// cut at 00:00 of its own wall clock (the driver returns it as UTC), and a
// date key at the date.
func dateChildren(rangeType RangeType, keyType string, loc *time.Location, lowest, highest time.Time) []childRange {
	zone := loc
	if keyType != keyTimestamptz {
		zone = time.UTC
	}

	hi := highest.In(zone)
	start := periodStart(rangeType, lowest.In(zone), zone)

	var children []childRange
	for !start.After(hi) {
		next := nextPeriodStart(rangeType, start, zone)
		children = append(children, childRange{
			suffix: periodSuffix(rangeType, start),
			from:   dateLiteral(keyType, start),
			to:     dateLiteral(keyType, next),
		})
		start = next
	}
	return children
}

// periodStart is 00:00 of the day, the first of the month or the first of
// the year holding at, in zone.
func periodStart(rangeType RangeType, at time.Time, zone *time.Location) time.Time {
	switch rangeType {
	case RangeTypeDaily:
		return time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, zone)
	case RangeTypeMonthly:
		return time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, zone)
	}
	return time.Date(at.Year(), time.January, 1, 0, 0, 0, 0, zone)
}

// nextPeriodStart is the start of the period after the one starting at start.
// It works from the calendar date, so a day of 23 or 25 hours still ends at
// the next 00:00.
func nextPeriodStart(rangeType RangeType, start time.Time, zone *time.Location) time.Time {
	switch rangeType {
	case RangeTypeDaily:
		return time.Date(start.Year(), start.Month(), start.Day()+1, 0, 0, 0, 0, zone)
	case RangeTypeMonthly:
		return time.Date(start.Year(), start.Month()+1, 1, 0, 0, 0, 0, zone)
	}
	return time.Date(start.Year()+1, time.January, 1, 0, 0, 0, 0, zone)
}

// periodSuffix names a date child: _20261117, _202611 or _2026.
func periodSuffix(rangeType RangeType, start time.Time) string {
	switch rangeType {
	case RangeTypeDaily:
		return fmt.Sprintf("_%04d%02d%02d", start.Year(), int(start.Month()), start.Day())
	case RangeTypeMonthly:
		return fmt.Sprintf("_%04d%02d", start.Year(), int(start.Month()))
	}
	return fmt.Sprintf("_%04d", start.Year())
}

// dateLiteral writes a period boundary in the key column's input form.
func dateLiteral(keyType string, at time.Time) string {
	switch keyType {
	case keyTimestamptz:
		return at.Format(time.RFC3339)
	case keyTimestamp:
		return at.Format("2006-01-02 15:04:05")
	}
	return at.Format("2006-01-02")
}

// fixedChildren returns one child per block [k·blockSize, (k+1)·blockSize)
// from the block holding lowest to the block holding highest. k is the floor of
// value / blockSize, so -1 falls in [-blockSize, 0). A child is named after
// its first and last number, a minus sign written as "m": _0_3, _m4_m1.
func fixedChildren(blockSize int, lowest, highest int64) ([]childRange, error) {
	size := int64(blockSize)
	if size < 1 {
		return nil, fmt.Errorf("%w: a %q range needs a BlockSize of at least 1, got %d", ErrInvalidConfig, FixedRange, blockSize)
	}

	return blockChildren(size, floorDiv(lowest, size), floorDiv(highest, size))
}

// blockChildren returns one child per block from block first to block last,
// block k being [k·size, (k+1)·size). A block outside the bigint range is
// refused.
func blockChildren(size, first, last int64) ([]childRange, error) {
	var children []childRange
	for k := first; k <= last; k++ {
		from := k * size
		if from/size != k {
			return nil, fmt.Errorf("%w: block %d of size %d starts outside the bigint range", ErrInvalidConfig, k, size)
		}
		if from > 0 && from > maxInt64-size {
			return nil, fmt.Errorf("%w: the block starting at %d ends past the bigint range", ErrInvalidConfig, from)
		}
		to := from + size

		children = append(children, childRange{
			suffix: "_" + blockNumber(from) + "_" + blockNumber(to-1),
			from:   strconv.FormatInt(from, 10),
			to:     strconv.FormatInt(to, 10),
		})
	}
	return children, nil
}

const maxInt64 = int64(^uint64(0) >> 1)

// floorDiv divides rounding toward negative infinity (Go's / rounds toward
// zero).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// blockNumber writes n for a child name, "m" standing for the minus sign.
func blockNumber(n int64) string {
	text := strconv.FormatInt(n, 10)
	if strings.HasPrefix(text, "-") {
		return "m" + text[1:]
	}
	return text
}

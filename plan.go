package san_pgpartition

import (
	"fmt"
	"time"
)

// keyClock returns now as the key column reads it, and the zone its periods
// are cut in. A timestamptz key is cut at 00:00 in loc. A timestamp or date
// key has no zone: "now" is the wall clock in loc, written as UTC — the form
// the driver returns such a column in, and the one dateChildren cuts.
func keyClock(now time.Time, keyType string, loc *time.Location) (time.Time, *time.Location) {
	local := now.In(loc)
	if keyType == keyTimestamptz {
		return local, loc
	}
	wall := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), local.Second(), local.Nanosecond(), time.UTC)
	return wall, keyZone(keyType, loc)
}

// keyZone is the zone a key's periods are cut in: loc for timestamptz, UTC
// (standing for the wall clock) for timestamp and date.
func keyZone(keyType string, loc *time.Location) *time.Location {
	if keyType == keyTimestamptz {
		return loc
	}
	return time.UTC
}

// provisionDates returns the children a date table must have: the current
// period (in loc) and the n after it, plus every period from the newest
// child's end up to the current one when the newest child ends earlier (a
// cron that missed some runs). Older gaps are not its business. newestEnd is
// nil for a table without children. Children that already exist are
// skipped by the caller.
func provisionDates(rangeType RangeType, keyType string, loc *time.Location, now time.Time, n int, newestEnd *time.Time) []childRange {
	clock, zone := keyClock(now, keyType, loc)
	current := periodStart(rangeType, clock, zone)

	start := current
	if newestEnd != nil {
		gap := periodStart(rangeType, newestEnd.In(zone), zone)
		if gap.Before(start) {
			start = gap
		}
	}

	last := current
	for i := 0; i < n; i++ {
		last = nextPeriodStart(rangeType, last, zone)
	}

	return dateChildren(rangeType, keyType, loc, start, last)
}

// provisionBlocks returns the blocks a FixedRange table must have: the block
// holding highest — the highest key in its children, nil when they hold no
// rows, which counts as block 0 — and the n after it, plus every block from
// the newest child's end up to it when the newest child ends below it.
func provisionBlocks(blockSize, n int, highest, newestEnd *int64) ([]childRange, error) {
	size := int64(blockSize)
	if size < 1 {
		return nil, fmt.Errorf("%w: a %q range needs a BlockSize of at least 1, got %d", ErrInvalidConfig, FixedRange, blockSize)
	}

	top := int64(0)
	if highest != nil {
		top = floorDiv(*highest, size)
	}

	first := top
	if newestEnd != nil {
		gap := floorDiv(*newestEnd, size)
		if gap < first {
			first = gap
		}
	}

	if top > maxInt64-int64(n) {
		return nil, fmt.Errorf("%w: %d blocks after block %d pass the bigint range", ErrInvalidConfig, n, top)
	}
	return blockChildren(size, first, top+int64(n))
}

// dateMoveWindow returns the periods whose DEFAULT rows MaintainPartition
// moves, as [lower, upper): from the oldest child's start to the newest
// child's end or the end of the current period, whichever is later. A table
// without children (oldestFrom or newestEnd nil) gets only the current
// period.
func dateMoveWindow(rangeType RangeType, keyType string, loc *time.Location, now time.Time, oldestFrom, newestEnd *time.Time) (time.Time, time.Time) {
	clock, zone := keyClock(now, keyType, loc)
	current := periodStart(rangeType, clock, zone)
	currentEnd := nextPeriodStart(rangeType, current, zone)

	if oldestFrom == nil || newestEnd == nil {
		return current, currentEnd
	}

	upper := currentEnd
	if newestEnd.After(upper) {
		upper = *newestEnd
	}
	return *oldestFrom, upper
}

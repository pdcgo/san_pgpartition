package san_pgpartition

import (
	"fmt"
	"time"
)

// RangeType is how a converted table's partition key is cut into children.
// Its value is what the manager stores in partitioned_tables.range_type.
type RangeType string

// The supported range types.
const (
	// RangeTypeDaily: one child per day in the table's Location, named
	// <table>_20261117.
	RangeTypeDaily RangeType = "daily"
	// RangeTypeMonthly: one child per month in the table's Location, named
	// <table>_202611.
	RangeTypeMonthly RangeType = "month"
	// RangeTypeYearly: one child per year in the table's Location, named
	// <table>_2026.
	RangeTypeYearly RangeType = "year"
	// FixedRange: one child per block of BlockSize numbers, [k·BlockSize,
	// (k+1)·BlockSize), named <table>_<first>_<last> (BlockSize 4:
	// <table>_0_3, <table>_4_7).
	FixedRange RangeType = "fixed"
)

// PartitionRangeConfig is how a table is partitioned: given once, to
// [Manager.PartitionExistTable], and kept in the manager's
// partitioned_tables row for every later call.
type PartitionRangeConfig struct {
	// Location is the time zone a day, month or year starts in (00:00).
	// Required by the date types and stored by its IANA name, so it must be
	// a named zone time.LoadLocation can load again ("Asia/Jakarta", "UTC");
	// time.Local and unnamed fixed zones are refused. Ignored by FixedRange.
	Location *time.Location
	// RangeType is one of RangeTypeDaily, RangeTypeMonthly, RangeTypeYearly
	// or FixedRange.
	RangeType RangeType
	// BlockSize is how many numbers one FixedRange child holds; at least 1.
	// Ignored by the date types.
	BlockSize int
	// Key is the partition key: one column of the table, matched exactly.
	// The date types take a timestamptz, timestamp or date column;
	// FixedRange takes a smallint, integer or bigint column.
	Key string
}

// isDate reports whether the range type cuts dates (daily, month, year).
func (r RangeType) isDate() bool {
	return r == RangeTypeDaily || r == RangeTypeMonthly || r == RangeTypeYearly
}

// validate checks everything that does not need the database.
func (c *PartitionRangeConfig) validate() error {
	var err error

	if c == nil {
		return fmt.Errorf("%w: the partition config is nil", ErrInvalidConfig)
	}

	switch {
	case c.RangeType.isDate():
		err = validateLocation(c.Location)
		if err != nil {
			return err
		}
	case c.RangeType == FixedRange:
		if c.BlockSize < 1 {
			return fmt.Errorf("%w: a %q range needs a BlockSize of at least 1, got %d", ErrInvalidConfig, FixedRange, c.BlockSize)
		}
	default:
		return fmt.Errorf("%w: range type %q is not one of %q, %q, %q, %q", ErrInvalidConfig, c.RangeType, RangeTypeDaily, RangeTypeMonthly, RangeTypeYearly, FixedRange)
	}

	if c.Key == "" {
		return fmt.Errorf("%w: the partition key is empty", ErrInvalidConfig)
	}
	err = validateIdentifier("partition key", c.Key)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	return nil
}

// locationProbes are instants a Location must agree on with the zone of the
// same name, so a fixed zone merely named "UTC" is not taken for UTC.
var locationProbes = []time.Time{
	time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
	time.Date(2000, time.July, 1, 0, 0, 0, 0, time.UTC),
	time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
	time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
}

// validateLocation requires a zone that survives being stored by name: the
// manager keeps loc.String() and later calls time.LoadLocation on it.
func validateLocation(loc *time.Location) error {
	if loc == nil {
		return fmt.Errorf("%w: a date range needs a Location", ErrInvalidConfig)
	}

	name := loc.String()
	if name == "" {
		return fmt.Errorf("%w: the Location has no name; use a named zone such as time.LoadLocation(\"Asia/Jakarta\")", ErrInvalidConfig)
	}
	if name == "Local" {
		return fmt.Errorf("%w: time.Local is not stored by a name another machine can load; use time.LoadLocation(\"Asia/Jakarta\") or similar", ErrInvalidConfig)
	}

	loaded, err := time.LoadLocation(name)
	if err != nil {
		return fmt.Errorf("%w: Location %q cannot be loaded again by name: %v", ErrInvalidConfig, name, err)
	}
	for _, at := range locationProbes {
		_, want := at.In(loc).Zone()
		_, got := at.In(loaded).Zone()
		if want != got {
			return fmt.Errorf("%w: Location %q is not the zone of that name (offset %ds, the named zone has %ds on %s)", ErrInvalidConfig, name, want, got, at.Format("2006-01-02"))
		}
	}

	return nil
}

// Name suffixes of the tables and indexes a conversion creates or renames.
const (
	suffixNew     = "_new"
	suffixOld     = "_old"
	suffixDefault = "_default"
)

// childSuffixSample is the longest child suffix of each date range type.
var childSuffixSample = map[RangeType]string{
	RangeTypeDaily:   "_20261117",
	RangeTypeMonthly: "_202611",
	RangeTypeYearly:  "_2026",
}

// validateDerivedNames refuses a table whose derived names would pass
// PostgreSQL's 63-byte limit. FixedRange children are checked as they are
// named: their length depends on the values.
func validateDerivedNames(table TableName, rangeType RangeType) error {
	var err error

	suffixes := []string{suffixNew, suffixOld, suffixDefault}
	sample, ok := childSuffixSample[rangeType]
	if ok {
		suffixes = append(suffixes, sample)
	}

	for _, suffix := range suffixes {
		err = validateIdentifier("derived table name", table.Name+suffix)
		if err != nil {
			return err
		}
	}

	return nil
}

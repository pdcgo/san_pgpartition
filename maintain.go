package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// MaintainReport is what [Manager.MaintainPartition] did to one table.
type MaintainReport struct {
	// Table is the registered table.
	Table TableName
	// Moved counts the rows moved out of the table's DEFAULT partition into
	// their children by this call.
	Moved int64
	// LeftInDefault counts the rows still in DEFAULT afterwards: keys before
	// the oldest child or past the horizon (a back-dated import, a typo
	// year, a block above the newest child), NULL and infinite keys. They
	// stay until someone deals with them, and are counted on every call.
	LeftInDefault int64
}

// MaintainPartition moves the rows found in each registered table's DEFAULT
// partition into their children, creating each child it needs, and reports
// per table what it moved and what it left. The caller's cron runs it after
// [Manager.MaintainPartitionTableProvision].
//
// A DEFAULT row is moved when its period (dates, in the table's Location) or
// block lies between the table's oldest child and the horizon: for dates the
// later of the newest child's end and the end of the current period, for
// FixedRange the newest child's end. A table with no children moves only the
// current period's rows (dates) or nothing (FixedRange). Every other row
// stays in DEFAULT and is counted in LeftInDefault on every call — a
// back-dated import before the oldest child, a typo year past the horizon,
// NULL and infinite keys — until someone deals with it. Children are created
// and rows moved exactly as provisioning does (ACCESS EXCLUSIVE on the
// parent while it runs, rows through a temporary table, children named and
// marked like the conversion's); a second call moves nothing.
//
// The reports come one per registered table, zeros included, sorted by
// schema and then name. Each table is one transaction under the manager's
// advisory lock on it; a failed table is rolled back, left out of the
// reports, and its error — naming it — is joined into the returned error
// while the other tables are still done. Without a registry it returns nil,
// nil and creates nothing.
func (m *Manager) MaintainPartition(ctx context.Context) ([]MaintainReport, error) {
	var err error

	err = m.check(ctx)
	if err != nil {
		return nil, err
	}

	entries, err := m.readRegistry(ctx)
	if err != nil {
		return nil, err
	}

	var reports []MaintainReport
	var errs []error
	for _, entry := range entries {
		var report MaintainReport
		err = m.db.
			WithContext(ctx).
			Transaction(func(tx *gorm.DB) error {
				var txErr error

				report, txErr = m.maintainTable(tx, entry)
				return txErr
			})
		if err != nil {
			errs = append(errs, fmt.Errorf("san_pgpartition: maintain %s: %w", entry.table(), err))
			continue
		}
		reports = append(reports, report)
	}
	return reports, errors.Join(errs...)
}

// maintainTable moves one table's DEFAULT rows that lie between its oldest
// child and the horizon into their children, creating each child.
func (m *Manager) maintainTable(tx *gorm.DB, entry registryEntry) (MaintainReport, error) {
	var err error

	report := MaintainReport{Table: entry.table()}

	err = m.lockTable(tx, entry.table())
	if err != nil {
		return report, err
	}

	mt, err := loadTable(tx, entry)
	if err != nil {
		return report, err
	}
	mt.afterLock = m.hooks.afterTableLock
	if mt.deflt == nil {
		return report, nil
	}

	plan, err := mt.maintainPlan(tx, m.now())
	if err != nil {
		return report, err
	}

	for _, r := range plan {
		moved, err := mt.createChild(tx, r)
		if err != nil {
			return report, lockLimitHint(err, len(plan))
		}
		report.Moved += moved
	}

	err = tx.
		Raw("SELECT count(*) FROM " + mt.deflt.quoted()).
		Row().
		Scan(&report.LeftInDefault)
	if err != nil {
		return report, fmt.Errorf("san_pgpartition: count the rows left in %s: %w", mt.deflt, err)
	}
	return report, nil
}

// dateUnits are date_trunc's names for the date range types.
var dateUnits = map[RangeType]string{
	RangeTypeDaily:   "day",
	RangeTypeMonthly: "month",
	RangeTypeYearly:  "year",
}

// maintainPlan lists the children whose periods (or blocks) hold DEFAULT
// rows and lie between the oldest child and the horizon.
func (mt *managedTable) maintainPlan(tx *gorm.DB, now time.Time) ([]childRange, error) {
	if mt.pconf.RangeType == FixedRange {
		return mt.maintainBlocks(tx)
	}
	return mt.maintainDates(tx, now)
}

func (mt *managedTable) maintainDates(tx *gorm.DB, now time.Time) ([]childRange, error) {
	var err error

	key := quoteIdentifier(mt.pconf.Key)
	unit := dateUnits[mt.pconf.RangeType]

	// Each DEFAULT row's period start, cut as the children are: in the
	// Location for timestamptz, at the wall clock for timestamp and date.
	var starts []time.Time
	query := tx.Raw("SELECT DISTINCT pg_catalog.date_trunc(?::text, "+key+"::timestamp) FROM "+mt.deflt.quoted()+" WHERE "+key+" IS NOT NULL AND pg_catalog.isfinite("+key+")", unit)
	if mt.keyType == keyTimestamptz {
		query = tx.Raw("SELECT DISTINCT pg_catalog.date_trunc(?::text, "+key+", ?::text) FROM "+mt.deflt.quoted()+" WHERE "+key+" IS NOT NULL AND pg_catalog.isfinite("+key+")", unit, mt.pconf.Location.String())
	}
	err = query.
		Scan(&starts).
		Error
	if err != nil {
		return nil, fmt.Errorf("san_pgpartition: read the periods in %s: %w", mt.deflt, err)
	}

	lower, upper := dateMoveWindow(mt.pconf.RangeType, mt.keyType, mt.pconf.Location, now, mt.oldestFrom, mt.newestEnd)
	zone := keyZone(mt.keyType, mt.pconf.Location)

	var plan []childRange
	for _, start := range starts {
		start = periodStart(mt.pconf.RangeType, start.In(zone), zone)
		if start.Before(lower) || !start.Before(upper) {
			continue
		}
		r := dateChildren(mt.pconf.RangeType, mt.keyType, mt.pconf.Location, start, start)[0]
		if mt.has(r) {
			continue
		}
		plan = append(plan, r)
	}
	return plan, nil
}

func (mt *managedTable) maintainBlocks(tx *gorm.DB) ([]childRange, error) {
	var err error

	// No children: no range to move into.
	if mt.oldestFromBlock == nil || mt.newestEndBlock == nil {
		return nil, nil
	}

	size := int64(mt.pconf.BlockSize)
	key := quoteIdentifier(mt.pconf.Key)

	var blocks []int64
	err = tx.
		Raw("SELECT DISTINCT pg_catalog.floor("+key+"::numeric / CAST(? AS bigint))::bigint FROM "+mt.deflt.quoted()+" WHERE "+key+" IS NOT NULL", size).
		Scan(&blocks).
		Error
	if err != nil {
		return nil, fmt.Errorf("san_pgpartition: read the blocks in %s: %w", mt.deflt, err)
	}

	var plan []childRange
	for _, k := range blocks {
		children, err := blockChildren(size, k, k)
		if err != nil {
			return nil, err
		}
		from := k * size
		if from < *mt.oldestFromBlock || from >= *mt.newestEndBlock {
			continue
		}
		if mt.has(children[0]) {
			continue
		}
		plan = append(plan, children[0])
	}
	return plan, nil
}

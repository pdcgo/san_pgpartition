package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// MaintainPartitionTableProvision creates, for every table in
// partitioned_tables, the children it needs n of its own periods ahead, so
// the app's inserts find a child instead of landing in DEFAULT. The caller's
// cron runs it (before [Manager.MaintainPartition]); n is the same for every
// table and counts each table's own periods: with 6 on 2026-11-17 in
// Jakarta, a month table gets _202611 … _202705, a daily one _20261117 …
// _20261123, a year one _2026 … _2032, and a FixedRange table the block
// holding the highest key in its children (block 0 when they hold no rows —
// DEFAULT never counts) plus the 6 blocks above it. n below 1 is
// ErrInvalidConfig.
//
// Every period from the table's newest child up to the current one is
// created too (a cron that missed runs); gaps further back in history are
// left alone. Children that exist are left as they are — a second call
// creates nothing — and new ones are named and marked like the conversion's
// (<table>_202612, comment "created by san_pgpartition").
//
// When DEFAULT holds rows of a period being created, they are moved into the
// new child in the same transaction (PostgreSQL refuses the child
// otherwise): the parent is locked ACCESS EXCLUSIVE, the rows are deleted
// from DEFAULT into a temporary table, the child is created, and the rows go
// back in through the parent. The app's reads and writes of the table wait
// from that lock to the commit, then run on the new child. Nothing is locked
// when nothing is missing.
//
// Each table is one transaction, serialised with the conversion and
// MaintainPartition through the manager's advisory lock on the table: a
// second cron waits and then finds the children there. A failure rolls back
// only that table (its moved rows are back in DEFAULT) and the others are
// still done; the returned error joins one error per failed table, each
// naming it (a registered table dropped since is ErrParentNotPartitioned).
// Without a registry it creates nothing — not even the registry — and
// returns nil.
//
// Every child created holds its locks until the table's commit: a daily
// table catching up years at once can run out of PostgreSQL's lock table
// (SQLSTATE 53200, explained in the error).
func (m *Manager) MaintainPartitionTableProvision(ctx context.Context, n int) error {
	var err error

	err = m.check(ctx)
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("%w: provision at least 1 period ahead, got %d", ErrInvalidConfig, n)
	}

	entries, err := m.readRegistry(ctx)
	if err != nil {
		return err
	}

	var errs []error
	for _, entry := range entries {
		err = m.db.
			WithContext(ctx).
			Transaction(func(tx *gorm.DB) error {
				return m.provisionTable(tx, entry, n)
			})
		if err != nil {
			errs = append(errs, fmt.Errorf("san_pgpartition: provision %s: %w", entry.table(), err))
		}
	}
	return errors.Join(errs...)
}

// provisionTable creates the children one table is missing.
func (m *Manager) provisionTable(tx *gorm.DB, entry registryEntry, n int) error {
	var err error

	err = m.lockTable(tx, entry.table())
	if err != nil {
		return err
	}

	mt, err := loadTable(tx, entry)
	if err != nil {
		return err
	}
	mt.afterLock = m.hooks.afterTableLock

	plan, err := mt.provisionPlan(tx, m.now(), n)
	if err != nil {
		return err
	}

	var missing []childRange
	for _, r := range plan {
		if !mt.has(r) {
			missing = append(missing, r)
		}
	}

	for _, r := range missing {
		_, err = mt.createChild(tx, r)
		if err != nil {
			return lockLimitHint(err, len(missing))
		}
	}
	return nil
}

// provisionPlan lists the children the table must have after provisioning.
func (mt *managedTable) provisionPlan(tx *gorm.DB, now time.Time, n int) ([]childRange, error) {
	var err error

	if mt.pconf.RangeType.isDate() {
		return provisionDates(mt.pconf.RangeType, mt.keyType, mt.pconf.Location, now, n, mt.newestEnd), nil
	}

	var highest *int64
	err = tx.
		Raw("SELECT max("+quoteIdentifier(mt.pconf.Key)+")::bigint FROM "+mt.table.quoted()+" AS r WHERE r.tableoid <> CAST(? AS bigint)::oid", mt.defaultOid).
		Row().
		Scan(&highest)
	if err != nil {
		return nil, fmt.Errorf("san_pgpartition: read the highest %s of %s: %w", mt.pconf.Key, mt.table, err)
	}
	return provisionBlocks(mt.pconf.BlockSize, n, highest, mt.newestEndBlock)
}
